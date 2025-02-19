use std::{
    cmp::{min, Reverse},
    collections::{BinaryHeap, HashMap},
    fs::Permissions,
    io::ErrorKind,
    ops::AsyncFnOnce,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering},
        Arc, LazyLock,
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Local, Timelike, Utc};
use dashmap::DashMap;
use either::Either;
use futures::{stream, StreamExt, TryFutureExt};
use hashring::HashRing;
use nix::sys::stat::stat;
use opendal::Buffer;
use parking_lot::{Mutex, RwLock};
use snafu::{whatever, OptionExt, ResultExt};
use tokio::{
    fs::{self},
    io::{self, AsyncReadExt, AsyncWriteExt},
    sync::{mpsc::{channel, error::TrySendError, Receiver, Sender}, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tracing::{debug, error, info, instrument::WithSubscriber, warn};
use uuid::Uuid;

use crate::{
    buffer::{checksum, ChecksumLevel, FileBuffer},
    cache::{CacheKey, TotalAndUsed},
    cached_store::Config,
    error::{Result, StorageErrorEnum},
    uploader::{NormalUploader, Uploader},
    utils::{disk_usage, is_in_root_volumn},
};
use crate::{cache::CacheManager, cached_store::BLOCK_FILE_REGEX, error::StorageError};

// cache const
const CACHE_DIR: &str = "raw";
const CACHE_STAGE_DIR: &str = "rawstaging";

// disk state const
static MAX_NUM_IO_ERR_TO_UNSTABLE: u32 = 3;
static MIN_NUM_IO_SUCC_TO_NORMAL: LazyLock<u32> = LazyLock::new(|| {
    std::env::var("MIN_NUM_IO_SUCC_TO_NORMAL").unwrap_or("60".to_string())
        .parse()
        .unwrap_or(60)
});
// MAX_IO_ERR_PERCENTAGE
static MAX_IO_ERR_PERCENTAGE_TO_NORMAL: f64 = 0.0;
// MAX_CONCURRENCY_FOR_UNSTABLE
static MAX_CONCURRENCY_FOR_UNSTABLE: u32 = 10;
// MAX_IO_DURATION
static MAX_DURATION_TO_DOWN: Duration = Duration::minutes(30);

static STATE_CHECK_DURATION: Duration = Duration::minutes(1);
static PROBE_DURATION: Duration = Duration::milliseconds(500);
const PROBE_DIR: &str = "probe";
const PROBE_DATA: [u8; 3] = [1, 2, 3];
const PROBE_BUFF: [u8; 3] = [0; 3];

/// A cache manager that manage lots of [ `DiskCache` ] keyed by a object key
pub struct DiskCacheManager {
    cache_stores: Arc<RwLock<ConsistentHashDiskCache>>,
    stopped: Arc<AtomicBool>,
}

impl DiskCacheManager {
    pub async fn new(config: &Config, uploader: NormalUploader) -> Result<Self> {
        if let Some(upload_hours) = config.upload_hours {
            if upload_hours.0 != upload_hours.1 {
                info!("background upload at {}:00 ~ {}:00", upload_hours.0, upload_hours.1)
            }
        }
        // TODO: use env to replace global default config
        let cache_dirs = if config.auto_create {
            for dir in config.cache_dirs.iter() {
                fs::create_dir_all(dir).await?;
            }
            config.cache_dirs.clone()
        } else {
            config
                .cache_dirs
                .iter()
                .filter(|dir| dir.is_dir())
                .cloned()
                .collect()
        };
        if cache_dirs.is_empty() {
            whatever!("no cache dir existed, use memory cache instead");
        }
        let dir_cache_size = config.cache_size / cache_dirs.len() as u64;
        let mut cache_stores = ConsistentHashDiskCache {
            hash_ring: HashRing::new(),
            caches: HashMap::new(),
        };
        for dir in cache_dirs {
            let store =
                CacheStore::new(&config, dir.clone(), dir_cache_size, uploader.clone()).await?;
            cache_stores.hash_ring.add(dir.clone());
            cache_stores.caches.insert(dir.clone(), store);
        }
        let cache_stores = Arc::new(RwLock::new(cache_stores));
        let cache_stores_cloned = cache_stores.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_cloned = stopped.clone();
        tokio::spawn(async move {
            while !stopped_cloned.load(Ordering::SeqCst) {
                sleep(std::time::Duration::from_secs(1)).await;
                let mut cache_store = cache_stores_cloned.write();
                cache_store.caches.retain(|_, store| store.available());
            }
        });
        let manager = DiskCacheManager {
            cache_stores,
            stopped,
        };
        Ok(manager)
    }

    fn get_store(&self, key: &CacheKey) -> Option<Arc<CacheStore>> {
        let cache_store = self.cache_stores.read();
        let path = cache_store.hash_ring.get(key);
        if let Some(path) = path
            && let Some(store) = cache_store.caches.get(path)
            && store.available()
        {
            Some(store.clone())
        } else {
            None
        }
    }
}

impl Drop for DiskCacheManager {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl Uploader for DiskCacheManager {
    async fn upload(&self, key: &str, buffer: Buffer) -> Result<Either<Buffer, FileBuffer>> {
        let cache_key = CacheKey::from_str(key)?;
        if let Some(cache) = self.get_store(&cache_key) {
            cache.upload(key, buffer).await
        } else {
            whatever!("cache is err or not available")
        }
    }
}

impl CacheManager for DiskCacheManager {
    async fn put(&self, key: &CacheKey, p: Either<Buffer, FileBuffer>, force: bool) -> Result<()> {
        if let Some(cache) = self.get_store(key) {
            cache.put(key, p, force).await?;
        }
        Ok(())
    }

    async fn remove(&self, key: &CacheKey) {
        if let Some(cache) = self.get_store(key) {
            cache.remove(key).await;
        }
    }

    async fn get(&self, key: &CacheKey) -> Result<Option<Either<Buffer, FileBuffer>>> {
        if let Some(cache) = self.get_store(key) {
            cache.get(key).await
        } else {
            Ok(None)
        }
    }

    fn stats(&self) -> TotalAndUsed {
        let (mut cnt, mut used) = (0_i64, 0_i64);
        let cache_store = self.cache_stores.read();

        for (_, cache) in cache_store.caches.iter() {
            let (c, u) = cache.stats();
            cnt += c;
            used += u;
        }
        (cnt, used)
    }

    fn used_memory(&self) -> i64 {
        let mut mem = 0;
        let cache_store = self.cache_stores.read();

        for (_, cache) in cache_store.caches.iter() {
            let m = cache.used_memory();
            mem += m;
        }
        mem
    }

    fn is_invalid(&self) -> bool {
        let cache_store = self.cache_stores.read();
        cache_store.caches.is_empty()
    }
}

struct ConsistentHashDiskCache {
    hash_ring: HashRing<PathBuf>,
    caches: HashMap<PathBuf, Arc<CacheStore>>,
}

impl FromStr for CacheKey {
    type Err = StorageError;

    fn from_str(s: &str) -> Result<Self> {
        let blocks = BLOCK_FILE_REGEX
            .captures(s)
            .with_whatever_context::<_, _, StorageError>(|| format!("Invalid cache key: {s}"))?;
        Ok(CacheKey {
            id: blocks[1]
                .parse()
                .with_whatever_context::<_, _, StorageError>(|e| {
                    format!("Invalid cache key: {s}, e: {e}")
                })?,
            indx: blocks[2]
                .parse()
                .with_whatever_context::<_, _, StorageError>(|e| {
                    format!("Invalid cache key: {s}, e: {e}")
                })?,
            size: blocks[3]
                .parse()
                .with_whatever_context::<_, _, StorageError>(|e| {
                    format!("Invalid cache key: {s}, e: {e}")
                })?,
        })
    }
}

struct CacheItem {
    size: i32,
    atime: i64,
}

#[derive(Debug)]
enum DiskState {
    Normal {
        io_err_cnt: AtomicU32,
    },
    Unstable {
        start_time: DateTime<Utc>,
        io_err_cnt: AtomicU32,
        io_cnt: AtomicU32,
        io_limit: Arc<Semaphore>,
    },
    Down,
    Unchanged,
}

pub enum DiskEvent {
    IOSuccess,
    IOError,
}

pub struct PendingStage {
    /// stage upload key
    key: String,
    /// file buffer to be staged
    file_buffer: FileBuffer,
    /// timestamp when this item is added
    ts: DateTime<Utc>,
}

impl PartialEq for PendingStage {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts
    }
}

impl PartialOrd for PendingStage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.ts.partial_cmp(&other.ts)
    }
}

impl Eq for PendingStage {
    fn assert_receiver_is_total_eq(&self) {
        self.ts.assert_receiver_is_total_eq();
    }
}

impl Ord for PendingStage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts.cmp(&other.ts)
    }
}

struct WriteBacker {
    /// staged pages wait to upload to object storage
    pending_stages: Sender<(String, FileBuffer)>,
    // TODO: 为什么go的代码这里要设置成map,需要判断去重吗，为什么会有重复的stage呢
    deplayed_pending_stages: Mutex<BinaryHeap<Reverse<PendingStage>>>,
    /// the time range for staged pages can upload to object storage
    upload_hours: Option<(u32, u32)>,
    delay_upload: Option<Duration>,
}

impl WriteBacker {
    fn can_upload(&self) -> bool {
        let (start_hour, end_hour) = match self.upload_hours {
            Some((start_hour, end_hour)) => (start_hour, end_hour),
            None => return true,
        };
        if start_hour == end_hour {
            return true;
        }
        let h = Local::now().hour();
        start_hour < end_hour && h >= start_hour && h < end_hour
            || start_hour > end_hour && (h >= start_hour || h < end_hour)
    }
}

/// A cache store that stores pages on disk
pub struct CacheStore {
    /// cache unique id
    id: String,
    /// cached pages wait to flush to disk
    pending_cache: Sender<(CacheKey, Buffer)>,
    /// writer back
    write_backer: Option<WriteBacker>,

    /// ----------- Mem manage -----------
    /// used page mem space in bytes
    used_mem_page: AtomicI64,
    /// memory cached biffer
    mem_caches: DashMap<CacheKey, Buffer>,

    /// -------- Disk manage -----------
    /// disk state manage
    state: RwLock<DiskState>,
    /// used page disk space in bytes
    used_disk_page: AtomicI64,
    /// disk cached buffer, it will be scanned to correct it.
    /// TODO: put `disk_caches`, `scanning`, `stage_full`, `raw_full` into a struct
    disk_caches: Mutex<HashMap<CacheKey, CacheItem>>,
    /// max space for current cache
    max_disk_space: u64,
    /// file buffer checksum level
    checksum_level: ChecksumLevel,
    /// for every file buffer reader task, it should report error to this channel
    disk_event_sender: Sender<DiskEvent>,
    /// disk dir free ratio(space and files)
    min_disk_free_ratio: f32,
    /// disk dir(cache and stage(if has))
    dir: PathBuf,
    /// cache expire time, only expire disk cache, because mem cache is temporary,
    /// mem cache will persisted to disk cache soon
    cache_expire: Option<Duration>,
    /// cache file permission
    mode: Permissions,
    /// scan cache dir to correct cache size
    scanning: AtomicBool,
    /// scan interval
    scan_interval: Duration,
    /// has hash prefix for disk cache
    hash_prefix: bool,
    /// whether stage free space is not enough
    stage_space_full: AtomicBool,
    /// whether whole disk free space is not engough
    raw_space_full: AtomicBool,
    eviction: bool,
    uploader: NormalUploader,
}

impl CacheStore {
    pub async fn new(
        config: &Config,
        dir: PathBuf,
        max_disk_size: u64,
        uploader: NormalUploader,
    ) -> Result<Arc<Self>> {
        let min_disk_free_ratio = if is_in_root_volumn(dir.as_path()) && config.free_space < 0.2 {
            info!(
                "cache directory {} is in root volume, keep 20% space free",
                dir.as_path().display()
            );
            0.2
        } else {
            config.free_space
        };
        // TODO: because here cann't determine buffer size, give a fixed channel for pending cache
        let (pending_cache_sender, pending_cache_reveiver) = channel::<(CacheKey, Buffer)>(1024);
        let state = if config.writeback {
            DiskState::Unchanged
        } else {
            DiskState::Normal {
                io_err_cnt: AtomicU32::default(),
            }
        };
        let (disk_event_sender, disk_event_receiver) = channel::<DiskEvent>(1024);
        let (writer_backer, pending_stage_receiver) = if config.writeback {
            let (pending_stage_sender, pending_stage_receiver) = channel::<(String, FileBuffer)>(1024);
            (Some(WriteBacker {
                pending_stages: pending_stage_sender,
                deplayed_pending_stages: Mutex::new(BinaryHeap::new()),
                upload_hours: config.upload_hours,
                delay_upload: config.upload_delay,
            }), Some(pending_stage_receiver))
        } else {
            (None, None)
        };

        let mut cache_store = CacheStore {
            id: "".to_string(),
            pending_cache: pending_cache_sender,
            write_backer: writer_backer,
            used_mem_page: AtomicI64::default(),
            mem_caches: DashMap::default(),
            state: RwLock::new(state),
            used_disk_page: AtomicI64::default(),
            disk_caches: Mutex::new(HashMap::new()),
            max_disk_space: max_disk_size,
            checksum_level: config.cache_checksum.clone(),
            disk_event_sender: disk_event_sender,
            min_disk_free_ratio,
            dir,
            cache_expire: config.cache_expire,
            mode: config.cache_file_mode.clone(),
            scanning: AtomicBool::new(false),
            scan_interval: config.cache_scan_interval,
            hash_prefix: config.is_hash_prefix,
            stage_space_full: AtomicBool::new(false),
            raw_space_full: AtomicBool::new(false),
            eviction: config.cache_eviction,
            uploader: uploader,
        };
        let lock_file = cache_store.lock_path();
        let raw_id = cache_store
            .check_fs_op(async move || {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .mode(config.cache_file_mode.mode())
                    .open(lock_file)
                    .await?;
                let mut raw_id = String::new();
                file.read_to_string(&mut raw_id).await?;
                if raw_id.is_empty() {
                    let new_raw_id = Uuid::new_v4().to_string();
                    file.write_all(new_raw_id.as_bytes()).await?;
                    Ok(new_raw_id)
                } else {
                    Ok(raw_id)
                }
            })
            .await?;
        cache_store.id = raw_id;
        let cache_store = Arc::new(cache_store);
        cache_store
            .clone()
            .spawn_flush_cache(pending_cache_reveiver);
        cache_store.clone().spawn_find_unfinished_stage();
        cache_store
            .clone()
            .spawn_upload_stage(pending_stage_receiver);
        cache_store.clone().spawn_check_lock_file();
        cache_store.clone().spawn_check_free_space_ratio();
        cache_store.clone().spawn_check_expire();
        cache_store.clone().spawn_repair_cache_item();
        cache_store.clone().spawn_io_err_cnt_check();
        cache_store.clone().spawn_io_err_receive(disk_event_receiver);
        cache_store.clone().spawn_probe_cache_capability();

        Ok(cache_store)
    }

    /// Flush pending file into disk intevally
    fn spawn_flush_cache(
        self: Arc<Self>,
        mut pending_cache: Receiver<(CacheKey, Buffer)>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Some((key, buffer)) = pending_cache.recv().await {
                    let path = self.dir.join(key.to_path(self.hash_prefix));
                    if self.max_disk_space > 0
                        && let Ok(file) = self
                            .flush(Path::new(&path), buffer.clone())
                            .inspect_err(|e| error!("Failed to flush {path:?} into cache disk, {e}"))
                            .await
                    {
                        self.cache(&key, file.len() as i32, Some(Utc::now().timestamp()))
                            .await;
                    }
                    let v = self.mem_caches.remove(&key);
                    self.used_mem_page
                        .fetch_sub(buffer.len() as i64, Ordering::SeqCst);
                    // there exist mem cache removed, but channel to be persisted cache don't remove,
                    // so we need to double check it
                    if v.is_none() {
                        self.remove(&key).await;
                    }
                };
            }
        })
    }

    /// Find unfinished stage to continue upload them to object storage
    fn spawn_find_unfinished_stage(self: Arc<Self>) {}

    /// Flush pending stage file into remote storage and cache intevally
    fn spawn_upload_stage(
        self: Arc<Self>,
        pending_stages: Option<Receiver<(String, FileBuffer)>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if pending_stages.is_none() {
                return;
            }
            let store = self.clone();
            let scaner = tokio::spawn(async move {
                let mut pending_stages = pending_stages.unwrap();
                let write_backer = store.write_backer.as_ref().unwrap();
                while store.available() {
                    if let Some((key, stage_fb)) = pending_stages.recv().await {
                        let pending_stage = PendingStage {
                            key: key.clone(),
                            file_buffer: stage_fb.clone(),
                            ts: Utc::now() + write_backer.delay_upload.unwrap_or(Duration::zero()),
                        };
                        let mut deplayed_pending_stages = write_backer.deplayed_pending_stages.lock();
                        deplayed_pending_stages.push(Reverse(pending_stage));
                    }
                }
            });

            let uploader = tokio::spawn(async move {
                while self.available() {
                    sleep(std::time::Duration::from_secs(1)).await;
                    let write_backer = self.write_backer.as_ref().unwrap();
                    if !write_backer.can_upload() {
                        continue;
                    }

                    let pending_stages = {
                        let mut deplayed_pending_stages = write_backer.deplayed_pending_stages.lock();
                        let now = Utc::now();
                        let mut to_be_uploaded_pending_stages = vec![];
                        while let Some(Reverse(stage)) = deplayed_pending_stages.peek() {
                            if now < stage.ts {
                                break;
                            } else {
                                to_be_uploaded_pending_stages
                                    .push(deplayed_pending_stages.pop().unwrap());
                            }
                        }
                        to_be_uploaded_pending_stages
                    };

                    let mut err_stage = vec![];
                    for Reverse(mut stage) in pending_stages {
                        let buffer =
                            match stage.file_buffer.read_at(0, stage.file_buffer.len()).await {
                                Ok(buffer) => buffer,
                                Err(e) => {
                                    error!(
                                        "Failed to read staging file {:?}, {e}",
                                        stage.file_buffer.as_path()
                                    );
                                    err_stage.push(stage);
                                    continue;
                                }
                            };
                        match self.uploader.upload(&stage.key, buffer).await {
                            Ok(_) => {
                                debug!(
                                    "Upload staging file {:?} to object storage",
                                    stage.file_buffer.as_path()
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Failed to upload staging file {:?}, {e}",
                                    stage.file_buffer.as_path()
                                );
                                err_stage.push(stage);
                            }
                        }
                    }

                    {
                        let mut deplayed_pending_stages = write_backer.deplayed_pending_stages.lock();
                        deplayed_pending_stages.extend(err_stage.into_iter().map(Reverse));
                    }
                }
            });

            tokio::select! {
                _ = scaner => info!("scan task is finished"),
                _ = uploader => info!("uploader task is finished"),
            }
        })
    }

    /// Check lock file exists.
    fn spawn_check_lock_file(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let lock_file = self.lock_path();
            while self.available() {
                sleep(std::time::Duration::from_secs(10)).await;
                if let Err(e) = fs::metadata(&lock_file).await {
                    if e.kind() == io::ErrorKind::NotFound {
                        warn!("lockfile is lost, cache device maybe broken");
                    }
                }
            }
        })
    }

    /// Check free space ratio and evict cache pages if necessary
    fn spawn_check_free_space_ratio(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            while self.available() {
                let (br, fr) = self.stats_free_ratio();
                self.stage_space_full.store(
                    br < self.min_disk_free_ratio / 2.0 || fr < self.min_disk_free_ratio / 2.0,
                    Ordering::SeqCst,
                );
                self.raw_space_full.store(
                    br < self.min_disk_free_ratio || fr < self.min_disk_free_ratio,
                    Ordering::SeqCst,
                );
                if self.raw_space_full.load(Ordering::SeqCst) && self.eviction {
                    debug!(
                        "Cleanup cache when check free space ({:?}): free ratio ({:?}), space usage ({:?}), inodes usage ({:?})",
                        self.dir,
                        self.min_disk_free_ratio,
                        br,
                        fr
                    );
                    {
                        let mut cache_stats = self.disk_caches.lock();
                        self.cleanup_full(&mut cache_stats);
                    }
                    let (br, fr) = self.stats_free_ratio();
                    self.raw_space_full.store(
                        br < self.min_disk_free_ratio || fr < self.min_disk_free_ratio,
                        Ordering::SeqCst,
                    );
                }
                // TODO: immediately upload stages when raw space full
                sleep(std::time::Duration::from_secs(1)).await;
            }
        })
    }

    /// Check cache pages expire and evict cache pages if necessary
    fn spawn_check_expire(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let cache_expire = match self.cache_expire {
                Some(expire) => expire,
                None => return,
            };
            let interval = min(cache_expire, Duration::minutes(1));
            loop {
                let cutoff = (Utc::now() - cache_expire).timestamp();
                let (to_dels, wait_interval) = {
                    let cache_stats = self.disk_caches.lock();
                    let mut to_dels = vec![];
                    let (mut cnt, mut deleted) = (0, 0);
                    let mut freed = 0;
                    for (key, item) in cache_stats.iter() {
                        cnt += 1;
                        if cnt > 1000 {
                            break;
                        }
                        // staging
                        if item.size < 0 {
                            continue;
                        }
                        if item.atime < cutoff {
                            deleted += 1;
                            to_dels.push(key.to_path(self.hash_prefix));
                            freed += item.size as i64 + 4096;
                        }
                    }
                    self.used_disk_page.fetch_sub(freed, Ordering::SeqCst);
                    debug!(
                        "Cleanup expired cache ({:?}): {:?} blocks ({:?} MB), expired {:?} blocks ({:?} MB)",
                        self.dir,
                        cache_stats.len(),
                        self.used_disk_page.load(Ordering::SeqCst) >> 20,
                        to_dels.len(),
                        freed >> 20
                    );
                    (
                        to_dels,
                        (interval * (cnt + 1 - deleted) / (cnt + 1))
                            .to_std()
                            .unwrap(),
                    )
                };

                let cache_cloned = self.clone();
                let _ = tokio::spawn(async move {
                    let mut stream = stream::iter(to_dels);
                    while cache_cloned.available()
                        && let Some(to_del) = stream.next().await
                    {
                        fs::remove_file(to_del.clone())
                            .await
                            .inspect_err(|e| warn!("Failed to clean up cache file {to_del:?}, {e}"))
                            .ok();
                    }
                });
                sleep(wait_interval).await;
            }
        })
    }

    /// Check cache dir to auto correct cache file real size
    fn spawn_repair_cache_item(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let scan_interval = match self.scan_interval.to_std() {
                Ok(time) => time,
                Err(e) => {
                    error!("Failed to convert scan interval to std time: {e}");
                    return
                },
            };
            loop {
                {
                    let mut disk_caches = self.disk_caches.lock();
                    self.scanning.store(true, Ordering::SeqCst);
                    *disk_caches = HashMap::default();
                }

                let start = Local::now();
                let one_min_ago = start.to_utc() - Duration::minutes(1);
                let cache_prefix = Path::new(&self.dir).join(CACHE_DIR);
                debug!("Scan {cache_prefix:?} to find cached blocks");
                for entry in walkdir::WalkDir::new(cache_prefix.as_path()) {
                    let (entry, stat) = match entry {
                        Ok(entry) => match stat(entry.path()) {
                            Ok(stat) => (entry, stat),
                            Err(e) => {
                                warn!("Failed to get path {:?} metadata: {e}", entry.path());
                                continue;
                            }
                        },
                        Err(e) => {
                            warn!("Failed to scan cache dir: {e}");
                            continue;
                        }
                    };
                    let path = entry.path();
                    let relative_path = path.strip_prefix(cache_prefix.as_path()).unwrap();
                    if entry.file_type().is_dir() || path.to_string_lossy().ends_with(".tmp") {
                        let mtime =
                            DateTime::from_timestamp(stat.st_mtime, stat.st_atime_nsec as u32)
                                .unwrap();
                        if mtime < one_min_ago {
                            let del_res = if entry.file_type().is_dir() {
                                fs::remove_dir(path).await
                            } else {
                                fs::remove_file(path).await
                            };
                            if let Ok(_) = del_res {
                                info!("Remove expire empty directory or .tmp file: {path:?}");
                            }
                        }
                    } else {
                        let cache_key_path = match path.strip_prefix(cache_prefix.as_path()) {
                            Ok(p) => p.to_string_lossy().to_string(),
                            Err(e) => {
                                warn!("Failed to strip prefix from {path:?}: {e}");
                                continue;
                            }
                        };
                        if !BLOCK_FILE_REGEX.is_match(&relative_path.to_string_lossy()) {
                            warn!("Ignoring invalid cache file: {path:?}");
                            continue;
                        }
                        let atime =
                            DateTime::from_timestamp(stat.st_atime, stat.st_atime_nsec as u32)
                                .unwrap();
                        let cache_key = match cache_key_path.parse::<CacheKey>() {
                            Ok(key) => key,
                            Err(e) => {
                                warn!("Failed to parse cache key from {cache_key_path}: {e}");
                                continue;
                            }
                        };
                        if stat.st_nlink > 1 {
                            self.cache(&cache_key, -(stat.st_size as i32), Some(atime.timestamp()))
                                .await;
                        } else {
                            self.cache(&cache_key, stat.st_size as i32, Some(atime.timestamp()))
                                .await;
                        }
                    }
                }

                {
                    let disk_caches = self.disk_caches.lock();
                    self.scanning.store(false, Ordering::SeqCst);
                    debug!(
                        "Found {} cached blocks ({}) in {} with {start}",
                        disk_caches.len(),
                        self.used_disk_page.load(Ordering::SeqCst),
                        self.dir.display(),
                    );
                }
                sleep(scan_interval).await;
            }
        })
    }

    fn spawn_io_err_receive(
        self: Arc<Self>,
        mut disk_event_receiver: Receiver<DiskEvent>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Some(event) = disk_event_receiver.recv().await {
                    self.on_disk_event(event);
                }
            }
        })
    }

    /// Check io error occupies the number of io system calls
    fn spawn_io_err_cnt_check(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                sleep(STATE_CHECK_DURATION.to_std().unwrap()).await;
                self.correct_state();
            }
        })
    }

    /// Request a cache disk probe to test disk is ok or not
    fn spawn_probe_cache_capability(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut cnt = 0_u64;
            while self.available() {
                let key = CacheKey::new(cnt, 0, PROBE_DATA.len());
                let _ = self
                    .put(&key, Either::Left(Buffer::from(PROBE_DATA.to_vec())), true)
                    .await;
                sleep(PROBE_DURATION.to_std().unwrap()).await;
                self.remove(&key).await;
                sleep(PROBE_DURATION.to_std().unwrap()).await;
                cnt += 1;
            }
        })
    }

    fn correct_state(&self) {
        let mut state = self.state.write();
        let next_state = match *state {
            DiskState::Normal { ref io_err_cnt } => {
                if io_err_cnt.load(Ordering::SeqCst) > MAX_NUM_IO_ERR_TO_UNSTABLE {
                    Some(DiskState::Unstable {
                        start_time: Utc::now(),
                        io_err_cnt: AtomicU32::new(0),
                        io_cnt: AtomicU32::new(0),
                        io_limit: Arc::new(Semaphore::new(MAX_CONCURRENCY_FOR_UNSTABLE as usize)),
                    })
                } else {
                    None
                }
            }
            DiskState::Unstable {
                ref start_time,
                ref io_err_cnt,
                ref io_cnt,
                ..
            } => {
                if io_cnt.load(Ordering::SeqCst) > *MIN_NUM_IO_SUCC_TO_NORMAL
                    && io_err_cnt.load(Ordering::SeqCst) as f64
                        / io_cnt.load(Ordering::SeqCst) as f64
                        <= MAX_IO_ERR_PERCENTAGE_TO_NORMAL
                {
                    Some(DiskState::Normal {
                        io_err_cnt: AtomicU32::new(0),
                    })
                } else if Utc::now() - *start_time >= MAX_DURATION_TO_DOWN {
                    Some(DiskState::Down)
                } else {
                    io_cnt.store(0, Ordering::SeqCst);
                    io_err_cnt.store(0, Ordering::SeqCst);
                    None
                }
            }
            _ => None,
        };
        if let Some(next_state) = next_state {
            info!("Change disk state from {:?} to {:?}", *state, next_state);
            *state = next_state;
        }
    }

    // about path
    fn cache_path(&self, key: &CacheKey) -> PathBuf {
        let mut path_buf = PathBuf::new();
        path_buf.push(&self.dir);
        path_buf.push(CACHE_DIR);
        path_buf.push(key.to_path(self.hash_prefix));
        path_buf
    }

    fn stage_path(&self, key: &str) -> PathBuf {
        let mut path_buf = PathBuf::new();
        path_buf.push(&self.dir);
        path_buf.push(CACHE_STAGE_DIR);
        path_buf.push(key);
        path_buf
    }

    fn lock_path(&self) -> PathBuf {
        let mut path_buf = PathBuf::new();
        path_buf.push(&self.dir);
        path_buf.push(".lock");
        path_buf
    }

    // Check fs operation and send disk error to channel
    async fn check_fs_op<T>(&self, f: impl AsyncFnOnce() -> io::Result<T>) -> io::Result<T> {
        let res = match timeout(std::time::Duration::from_secs(30), f()).await {
            Ok(t) => match t {
                Ok(t) => Ok(t),
                Err(e) => Err(e),
            },
            Err(_) => Err(io::Error::new(ErrorKind::TimedOut, "io timeout 30s")),
        };
        self.disk_event_sender
            .send(
                res.as_ref()
                    .map_or_else(|_| DiskEvent::IOError, |_| DiskEvent::IOSuccess),
            )
            .await
            .err()
            .iter()
            .for_each(|send_err| {
                error!("failed to send disk error: {:?}", send_err);
            });
        res
    }

    // Put cache into cache items
    async fn cache(&self, key: &CacheKey, size: i32, atime: Option<i64>) {
        let mut cache_stats = self.disk_caches.lock();
        // hold lock until cleanup is finish
        cache_stats.get(&key).iter().for_each(|item| {
            self.used_disk_page
                .fetch_sub((item.size + 4096) as i64, Ordering::SeqCst);
        });

        let item = cache_stats.entry(key.clone()).or_insert(CacheItem {
            size,
            atime: Utc::now().timestamp(),
        });
        item.size = size;
        atime.iter().for_each(|atime| {
            item.atime = *atime;
        });
        self.used_disk_page
            .fetch_add((size + 4096) as i64, Ordering::SeqCst);

        if self.used_disk_page.load(Ordering::SeqCst) > self.max_disk_space as i64 && self.eviction
        {
            debug!(
                "Cleanup cache when add new data ({:?}): {:?} blocks ({:?} MB)",
                self.dir,
                cache_stats.len(),
                self.used_disk_page.load(Ordering::SeqCst) >> 20
            );
            self.cleanup_full(&mut cache_stats);
        }
    }

    // Flush memory buffer into disk
    async fn flush(&self, path: &Path, mut page: Buffer) -> Result<FileBuffer> {
        if !self.available() {
            whatever!("err cache down");
        }

        self.check_fs_op(async || {
            self.ensure_cache_dir_created(path).await
        }).await?;
        let tmp_path = path.with_extension("tmp");
        match self
            .check_fs_op(async || {
                let mut f = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .mode(self.mode.mode())
                    .open(tmp_path.as_path())
                    .await?;
                f.write_buf(&mut page).await?;
                if self.checksum_level != ChecksumLevel::None {
                    let mut checksum = checksum(&page);
                    f.write_buf(&mut checksum).await?;
                }
                f.sync_all().await?;
                fs::rename(tmp_path.as_path(), path).await
            })
            .await
        {
            Ok(_) => {
                let fb = FileBuffer::new(
                    path,
                    page.len(),
                    self.checksum_level.clone(),
                    self.disk_event_sender.clone(),
                )?;
                Ok(fb)
            }
            Err(e) => {
                fs::remove_file(tmp_path.as_path()).await.ok();
                return Err(e.into());
            }
        }
    }

    fn available(&self) -> bool {
        let state = self.state.read();
        match *state {
            DiskState::Down => false,
            _ => true,
        }
    }

    // notify disk event
    fn on_disk_event(&self, event: DiskEvent) {
        let state = self.state.read();
        match event {
            DiskEvent::IOError => match *state {
                DiskState::Normal { ref io_err_cnt } => {
                    io_err_cnt.fetch_add(1, Ordering::SeqCst);
                }
                DiskState::Unstable {
                    ref io_err_cnt,
                    ref io_cnt,
                    ..
                } => {
                    io_err_cnt.fetch_add(1, Ordering::SeqCst);
                    io_cnt.fetch_add(1, Ordering::SeqCst);
                }
                _ => (),
            },
            DiskEvent::IOSuccess => match *state {
                DiskState::Unstable { ref io_cnt, .. } => {
                    io_cnt.fetch_add(1, Ordering::SeqCst);
                }
                _ => (),
            },
        }
    }

    // Locked
    fn cleanup_full(&self, cache_stats: &mut HashMap<CacheKey, CacheItem>) {
        if !self.available() {
            return;
        }

        let mut remain_space = (self.max_disk_space * 95 / 100) as i64;
        let mut remain_inodes = cache_stats.len() * 99 / 100;
        // make sure we have enough free space after cleanup
        let (br, fr) = self.stats_free_ratio();
        if br < self.min_disk_free_ratio {
            let (total, _, _, _) = disk_usage(Path::new(&self.dir));
            let to_free = (total as f32 * (self.min_disk_free_ratio - br)) as i64;
            if to_free > self.used_disk_page.load(Ordering::SeqCst) {
                remain_space = 0;
            } else if self.used_disk_page.load(Ordering::SeqCst) - to_free < remain_space {
                remain_space = self.used_disk_page.load(Ordering::SeqCst) - to_free;
            }
        }
        if fr < self.min_disk_free_ratio {
            let (_, _, files, _) = disk_usage(Path::new(&self.dir));
            let to_free = (files as f32 * (self.min_disk_free_ratio - fr)) as usize;
            if to_free > cache_stats.len() {
                remain_inodes = 0;
            } else {
                remain_inodes = cache_stats.len() - to_free;
            }
        }

        // for each two random keys, then compare the access time, evict the older one
        let mut keys = cache_stats
            .keys()
            .into_iter()
            .filter(|key| cache_stats.get(*key).unwrap().size > 0)
            .collect::<Vec<_>>();
        keys.sort_by(|a, b| {
            let a_atime = cache_stats.get(*a).unwrap().atime;
            let b_atime = cache_stats.get(*b).unwrap().atime;
            if a_atime < b_atime {
                std::cmp::Ordering::Greater
            } else if a_atime > b_atime {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });
        let mut to_dels = Vec::new();
        let mut to_del_spaces = 0;
        let used_space = self.used_disk_page.load(Ordering::SeqCst);
        for key in keys {
            to_dels.push(self.cache_path(&key));
            to_del_spaces += cache_stats.get(key).unwrap().size as i64 + 4096;
            if to_dels.len() > cache_stats.len() - remain_inodes
                && to_del_spaces > used_space - remain_space
            {
                break;
            }
        }
        cache_stats.retain(|key, _| !to_dels.contains(&self.cache_path(&key)));
        debug!(
            "cleanup cache ({:?}): {:?} blocks ({:?} MB), freed {:?} blocks ({:?} MB)",
            self.dir,
            cache_stats.len(),
            self.used_disk_page.load(Ordering::SeqCst) >> 20,
            to_dels.len(),
            to_del_spaces >> 20
        );
        // todo: ignote the result
        let _ = tokio::spawn(async move {
            let mut stream = stream::iter(to_dels);
            loop {
                tokio::select! {
                    to_del = stream.next() => {
                        if let Some(to_del) = to_del {
                            fs::remove_file(to_del.clone())
                                .await
                                .inspect_err(|e| warn!("Failed to clean up cache file {to_del:?}, {e}")).ok();
                        }
                    },
                    // todo: wait for dc down message, to break this loop
                }
            }
        });
    }

    fn stats_free_ratio(&self) -> (f32, f32) {
        let (total, free, files, ffree) = disk_usage(Path::new(&self.dir));
        (free as f32 / total as f32, ffree as f32 / files as f32)
    }

    async fn remove_stage(&self, key: &CacheKey) -> Result<()> {
        match self
            .check_fs_op(async || {
                fs::remove_file(self.stage_path(&key.to_path(self.hash_prefix)).as_path()).await
            })
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    async fn ensure_cache_dir_created(&self, dir: &Path) -> io::Result<()> {
        // who can read the cache, should be able to access the directories and add new file.
        // If a certain identity has read permission,
        // the corresponding write and execute permissions will be automatically granted
        let read_mode = self.mode.mode() & 0o444;
        let mode = Permissions::from_mode(self.mode.mode() | (read_mode >> 2) | (read_mode >> 1));

        let mut missing_paths = vec![];
        let mut path = dir.to_path_buf();
        while path.pop() {
            if !path.exists() {
                missing_paths.push(path.clone());
            } else {
                break;
            }
        }
        for path in missing_paths.iter().rev() {
            fs::create_dir(path).await?;
            // umask may remove some permisssions
            fs::set_permissions(path, mode.clone()).await?;
        }
        Ok(())
    }

    /// state check, for unstable state, limit request concurrency
    fn state_check(&self) -> Result<Option<OwnedSemaphorePermit>> {
        let state = self.state.read();
        if let DiskState::Unstable {
            ref io_limit,
            ..
        } = *state {
            let io_limit = io_limit.clone().try_acquire_owned().map_err(|_| StorageErrorEnum::DiskUnstableError)?;
            Ok(Some(io_limit))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl Uploader for CacheStore {
    async fn upload(&self, key: &str, buffer: Buffer) -> Result<Either<Buffer, FileBuffer>> {
        match self.write_backer {
            Some(ref write_backer) => {
                let staging_path = self.stage_path(key);
                if self.stage_space_full.load(Ordering::SeqCst) {
                    whatever!("space not enough on device");
                }
                let _ = self.state_check()?;
                let res_buffer = self.flush(staging_path.as_path(), buffer.clone()).await?;
                // Save time loading from disk into memory
                if write_backer.delay_upload.is_none() && write_backer.can_upload() {
                    if let Ok(res_buffer) = self.uploader.upload(key, buffer).await {
                        return Ok(res_buffer);
                    }
                }

                write_backer.pending_stages
                    .send((key.to_string(), res_buffer.clone()))
                    .await
                    .map_err(|_| StorageErrorEnum::SenderError)?;
                Ok(Either::Right(res_buffer))
            }
            None => {
                self.uploader.upload(key, buffer).await
            }
        }

        
    }
}

impl CacheManager for CacheStore {
    async fn put(
        &self,
        key: &CacheKey,
        buffer: Either<Buffer, FileBuffer>,
        force: bool,
    ) -> Result<()> {
        let _ = self.state_check()?;
        match buffer {
            Either::Left(buffer) => {
                if self.max_disk_space == 0 {
                    return Ok(());
                }

                if self.raw_space_full.load(Ordering::SeqCst) && !self.eviction {
                    debug!(
                        "Caching directory is full ({:?}), drop new incoming {:?} ({:?} bytes)",
                        self.dir,
                        key,
                        buffer.len()
                    );
                    return Ok(());
                }

                if self.mem_caches.contains_key(key) {
                    return Ok(());
                }
                self.mem_caches.insert(key.clone(), buffer.clone());
                self.used_mem_page.fetch_add(buffer.len() as i64, Ordering::SeqCst);
                match self.pending_cache.try_send((key.clone(), buffer)) {
                    Ok(()) => Ok(()),
                    Err(TrySendError::Full((key, buffer))) => {
                        if force {
                            self.pending_cache
                                .send((key.clone(), buffer))
                                .await
                                .map_err(|_| StorageErrorEnum::SenderError)?;
                            Ok(())
                        } else {
                            debug!(
                                "Caching queue is full ({:?}), drop {:?} ({:?} bytes)",
                                self.dir,
                                key,
                                buffer.len()
                            );
                            Ok(())
                        }
                    }
                    Err(TrySendError::Closed(_)) => {
                        whatever!("Caching queue is closed")
                    }
                }
            }
            Either::Right(buffer) => {
                let cache_path = self.cache_path(key);
                match self
                    .check_fs_op(async || {
                        fs::hard_link(buffer.as_path(), cache_path.as_path()).await
                    })
                    .await
                {
                    Ok(()) => {
                        self.cache(key, -(buffer.len() as i32), Some(Utc::now().timestamp()))
                            .await
                    }
                    Err(e) => {
                        warn!(
                            "link {:?} to {:?} failed: {:?}",
                            buffer.as_path(),
                            cache_path.as_path(),
                            e
                        );
                    }
                };
                Ok(())
            }
        }
    }

    async fn remove(&self, key: &CacheKey) {
        {
            self.mem_caches.remove(key);
            let mut cache_stats = self.disk_caches.lock();
            if let Some(item) = cache_stats.remove(&key) {
                self.used_mem_page
                    .fetch_sub(item.size as i64, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let cache_path = self.cache_path(key);
        if cache_path.exists() {
            if let Err(e) = self
                .check_fs_op(async || fs::remove_file(cache_path.as_path()).await)
                .await
            {
                warn!("Remove cache file {:?} failed: {:?}", cache_path, e);
            }
            if let Err(e) = self.remove_stage(key).await {
                warn!(
                    "Remove cache stage file {:?} failed: {:?}",
                    self.stage_path(&key.to_path(self.hash_prefix)),
                    e
                );
            }
        }
    }

    async fn get(&self, key: &CacheKey) -> Result<Option<Either<Buffer, FileBuffer>>> {
        let _ = self.state_check()?;
        {
            if let Some(page) = self.mem_caches.get(key) {
                return Ok(Some(Either::Left(page.clone())));
            }
            let cache_stats = self.disk_caches.lock();
            // not in scaning represents all cache in cache_stats
            if !self.scanning.load(Ordering::SeqCst) && cache_stats.get(&key).is_none() {
                return Ok(None);
            }
        }

        let buffer = self
            .check_fs_op(async || {
                FileBuffer::new(
                    self.cache_path(key).as_path(),
                    key.size(),
                    self.checksum_level.clone(),
                    self.disk_event_sender.clone(),
                )
                .map(Either::Right)
            })
            .await
            .inspect(|_| {
                let mut cache_stats = self.disk_caches.lock();
                cache_stats
                    .entry(key.clone())
                    .and_modify(|item| item.atime = Utc::now().timestamp());
            })
            .inspect_err(|_| {
                let mut cache_stats = self.disk_caches.lock();
                if cache_stats.get(&key).is_some() {
                    self.used_disk_page
                        .fetch_sub(key.size() as i64, Ordering::SeqCst);
                }
                cache_stats.remove(&key);
            })?;
        Ok(Some(buffer))
    }

    fn stats(&self) -> TotalAndUsed {
        let cache_stats = self.disk_caches.lock();
        (
            self.mem_caches.len() as i64 + cache_stats.len() as i64,
            self.used_disk_page.load(Ordering::SeqCst) + self.used_memory(),
        )
    }

    fn used_memory(&self) -> i64 {
        self.used_mem_page.load(Ordering::SeqCst)
    }

    fn is_invalid(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs::Permissions, os::unix::fs::PermissionsExt, sync::{atomic::{AtomicU32, Ordering}, Arc}};

    use assert_matches::assert_matches;
    use chrono::{Duration, Utc};
    use either::Either;
    use opendal::{Buffer, Operator};
    use tempfile::tempdir;
    use tokio::{sync::Semaphore, time::sleep};
    use tracing_test::traced_test;

    use crate::{
        buffer::ChecksumLevel,
        cache::{
            disk::cache::{CacheStore, DiskState, MAX_CONCURRENCY_FOR_UNSTABLE, MAX_DURATION_TO_DOWN, MAX_NUM_IO_ERR_TO_UNSTABLE, MIN_NUM_IO_SUCC_TO_NORMAL},
            CacheKey, CacheManager, DiskEvent,
        },
        cached_store::{Config, BLOCK_FILE_REGEX},
        uploader::NormalUploader,
    };

    use super::DiskCacheManager;

    fn default_config() -> Config {
        Config {
            cache_dirs: vec![env::temp_dir().join("diskCache")],
            auto_create: true,
            cache_file_mode: Permissions::from_mode(0o666),
            cache_size: 10 << 20,
            cache_scan_interval: Duration::seconds(300),
            cache_expire: None,
            cache_checksum: ChecksumLevel::None,
            max_block_size: 1 << 20,
            ..Default::default()
        }
    }

    async fn test_cache_store_state_impl(dir_num: u32) {
        let dir = tempdir().unwrap();
        let mut config = default_config();
        config.cache_dirs = (0..dir_num)
            .into_iter()
            .map(|num| dir.path().join(num.to_string()))
            .collect::<Vec<_>>();
        let operator = Operator::new(()).unwrap().finish();
        let uploader = NormalUploader::new(Arc::new(operator), None);
        let cache_manager = DiskCacheManager::new(&config, uploader).await.unwrap();
        {
            let cache_stores = cache_manager.cache_stores.read();
            assert_eq!(cache_stores.caches.len(), dir_num as usize);
        }

        // case: cache
        let data = vec![1_u8, 2, 3];
        let page = Buffer::from(data);
        let key = CacheKey {
            id: 0,
            indx: 0,
            size: page.len(),
        };
        cache_manager
            .put(&key, Either::Left(page.clone()), false)
            .await
            .unwrap();

        // case: normal -> unstable
        let cache_store = cache_manager.get_store(&key);
        assert!(cache_store.is_some());
        let cache_store = cache_store.unwrap();
        for _ in 0..=MAX_NUM_IO_ERR_TO_UNSTABLE {
            cache_store.on_disk_event(DiskEvent::IOError);
        }
        cache_store.correct_state();
        sleep(Duration::seconds(1).to_std().unwrap()).await;  // probe every 500 milliseconds
        assert_matches!(*cache_store.state.read(), DiskState::Unstable { 
            ref io_cnt,
            .. 
        } if io_cnt.load(Ordering::SeqCst) >= 1);

        // case: unstable concurrency limit
        {
            let state = cache_store.state.read();
            if let DiskState::Unstable { ref io_limit, .. } = *state {
                let _premit = io_limit.try_acquire_many(MAX_CONCURRENCY_FOR_UNSTABLE).unwrap();
                let value = cache_store.get(&key).await;
                assert!(value.is_err());
                assert!(value.err().unwrap().is_disk_unstable_error());
            }        
        }

        // case: unstable -> normal
        for _ in 0..=*MIN_NUM_IO_SUCC_TO_NORMAL {
            cache_store.on_disk_event(DiskEvent::IOSuccess);
        }
        cache_store.correct_state();
        assert_matches!(*cache_store.state.read(), DiskState::Normal{..});

        // case: unstable -> down
        {
            let mut state = cache_store.state.write();
            let mock_start_time = Utc::now() - MAX_DURATION_TO_DOWN + Duration::seconds(1);
            *state = DiskState::Unstable { 
                start_time: mock_start_time,
                io_err_cnt: AtomicU32::default(), 
                io_cnt: AtomicU32::default(),
                io_limit: Arc::new(Semaphore::new(MAX_CONCURRENCY_FOR_UNSTABLE as usize)) 
            }
        }
        sleep(Duration::seconds(1).to_std().unwrap()).await;
        cache_store.correct_state();
        assert_matches!(*cache_store.state.read(), DiskState::Down);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cache_store_state() {
        test_cache_store_state_impl(1).await;
        test_cache_store_state_impl(10).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn test_new_cache_store() {
        let dir = tempdir().unwrap();
        let operator = Operator::new(()).unwrap().finish();
        let uploader = NormalUploader::new(Arc::new(operator), None);
        CacheStore::new(&default_config(), dir.into_path(), 1 << 30, uploader)
            .await
            .expect("create new cache store failed");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_check_path() {
        let cases = vec![
            ("chunks/0/0/1_0_1048576", Some(CacheKey { id: 1, indx: 0, size: 1048576 })),
            ("chunks/111/222/3333_3333_3333", Some(CacheKey { id: 3333, indx: 3333, size: 3333 })),
            ("chunks/111/222/3333_3333_0", Some(CacheKey { id: 3333, indx: 3333, size: 0 })),
            ("chunks/0/0/0_0_0", Some(CacheKey { id: 0, indx: 0, size: 0 })),
            ("chunks/01/10/0_01_0", Some(CacheKey { id: 0, indx: 1, size: 0 })),
            ("achunks/111/222/3333_3333_3333", None),
            ("chunksa/111/222/3333_3333_3333", None),
            ("chunksa", None),
            ("chunks/111", None),
            ("chunks/111/2222", None),
            ("chunks/111/2222/3", None),
            ("chunks/111/2222/3333_3333", None),
            ("chunks/111/2222/3333_3333_3333_4444", None),
            ("chunks/111/2222/3333_3333_3333/4444", None),
            ("chunks/111_/2222/3333_3333_3333", None),
            ("chunks/111/22_22/3333_3333_3333", None),
            ("chunks/111/22_22/3333_3333_3333", None),
        ];
        for (path, expected) in cases {
            if let Some(expected) = expected {
                assert_eq!(path.parse::<CacheKey>().ok(), Some(expected));
            } else {
                assert!(path.parse::<CacheKey>().is_err());
            }
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cache_manager() {
        let mut config = default_config();
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let dir3 = tempdir().unwrap();
        
        config.cache_dirs = vec![dir1.path().to_path_buf(), dir2.path().to_path_buf(), dir3.path().to_path_buf()];
        let operator = Operator::new(()).unwrap().finish();
        let uploader = NormalUploader::new(Arc::new(operator), None);
        let manager = DiskCacheManager::new(&config, uploader).await.expect("create disk cache manager failed");
        assert!(!manager.is_invalid());
        {
            let cache_stores = manager.cache_stores.read();
            assert_eq!(cache_stores.caches.len(), 3);
        }

        fn shutdown_store(store: Arc<CacheStore>) {
            let mut lock = store.state.write();
            *lock = DiskState::Down;
        }


        // case: key rehash after store removal
        let k1 = CacheKey {
            id: 0,
            indx: 0,
            size: 3,
        };
        let p1 = Buffer::from(vec![1, 2, 3]);
        manager.put(&k1, Either::Left(p1), true).await.expect("put cache failed");
        let p1_1 = manager.get(&k1).await.expect("get cache failed");
        assert!(p1_1.is_some());

        shutdown_store(manager.get_store(&k1).unwrap());
        let p1_2 = manager.get(&k1).await.expect("get cache failed");
        assert!(p1_2.is_none());
        let s1_1 = manager.get_store(&k1);
        assert!(s1_1.is_none());


        // case: remove all store
        {
            let cache_stores = manager.cache_stores.write();
            for (_, store) in cache_stores.caches.iter() {
                shutdown_store(store.clone());
            }
        }
        sleep(Duration::seconds(1).to_std().unwrap()).await;
        assert!(!manager.is_invalid());
    }
}
