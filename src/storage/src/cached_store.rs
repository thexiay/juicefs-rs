use std::cmp::min;
use std::env;
use std::io::ErrorKind;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fs::Permissions, future::Future, sync::Arc};

use bytes::{Bytes, BytesMut};
use chrono::{Duration, Utc};
use either::Either;
use futures::future::FutureExt;
use opendal::{Buffer, Operator};
use regex::Regex;
use snafu::whatever;
use tokio::task::JoinSet;
use tracing::{error, warn};

use crate::api::{ChunkStore, SliceWriter};
use crate::buffer::{ChecksumLevel, FileBuffer};
use crate::cache::{CacheKey, CacheManagerImpl};
use crate::compress::CompressArgo;
use crate::error::Result;
use crate::uploader::{NormalUploader, Uploader};
use crate::{
    api::SliceReader, cache::CacheManager, compress::Compressor, pre_fetcher::PreFetcher,
    single_flight::SingleFlight,
};

const CHUNK_SIZE: usize = 64 << 20; // 64MB
const PAGE_SIZE: usize = 64 << 10; // 64KB

// object stroage file name format, "chunks/${d}/${d}/${slice_id}_${block_idx}_${data_size}"
pub static BLOCK_FILE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^chunks/\d+/\d+/(\d+)_(\d+)_(\d+)$").unwrap());

// Config contains options for cachedStore
#[derive(Clone)]
pub struct Config {
    pub cache_dirs: Vec<PathBuf>,
    pub cache_file_mode: Permissions,
    pub cache_size: u64,
    pub cache_checksum: ChecksumLevel,
    pub cache_eviction: bool,
    pub cache_scan_interval: Duration,
    pub cache_expire: Option<Duration>,
    pub free_space: f32,
    /// whether auto create cache dir
    pub auto_create: bool,
    pub compress: Option<CompressArgo>,
    pub max_upload: isize,
    /// max concurrent stage write
    pub max_stage_write: Option<usize>,
    pub max_retries: isize,
    /// max upload Bytes to object storage per second
    pub upload_limit: Option<u64>,
    /// max download Bytes from object storage per second
    pub download_limit: Option<u64>,
    pub writeback: bool,
    /// the time to delay upload data to object storage
    pub upload_delay: Option<Duration>,
    /// the time range to allow upload data to object storage
    pub upload_hours: Option<(u32, u32)>,
    pub is_hash_prefix: bool,
    pub max_block_size: usize,
    /// timeout of download from object storage
    pub get_timeout: Duration,
    /// timeout of upload to object storage
    pub put_timeout: Duration,
    pub cache_full_block: bool,
    pub buffer_size: u64,
    pub read_ahead: isize,
    pub prefetch_parallelism: Option<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            cache_dirs: vec![env::temp_dir().join("cache")],
            cache_file_mode: Permissions::from_mode(0o600),
            cache_size: 10 << 20, // 10MB
            cache_checksum: ChecksumLevel::None,
            cache_eviction: false,
            cache_scan_interval: Duration::seconds(300),
            cache_expire: None,
            free_space: 0.1,
            auto_create: false,
            compress: None,
            max_upload: 1,
            max_stage_write: None,
            max_retries: 10,
            upload_limit: None,
            download_limit: None,
            writeback: false,
            upload_delay: None,
            upload_hours: None,
            is_hash_prefix: false,
            max_block_size: 1 << 20, // 1MB
            get_timeout: Duration::seconds(60),
            put_timeout: Duration::seconds(60),
            cache_full_block: false,
            buffer_size: 32 << 20, // 32MB
            read_ahead: 0,
            prefetch_parallelism: None,
        }
    }
}

impl Config {
    pub fn should_cache(&self, size: usize) -> bool {
        // cache_full_block意味着永远cache
        // size <= max_block_size意味着小内存块cache
        // upload_delay不为0意味着，stage块不会立即上传远端存储
        self.cache_full_block || size <= self.max_block_size || !self.upload_delay.is_none()
    }
}

#[derive(Clone)]
pub struct SliceHelper {
    id: u64,
    length: usize,
    conf: Arc<Config>,
}

impl SliceHelper {
    /// The index of the block that first contains the offset
    fn block_index(&self, off: usize) -> usize {
        off / self.conf.max_block_size
    }

    /// The size of the this block at index
    fn block_size(&self, idx: usize) -> usize {
        let bsize = self.length - idx * self.conf.max_block_size;
        if bsize > self.conf.max_block_size {
            self.conf.max_block_size
        } else {
            bsize
        }
    }

    fn page_size(&self, b_idx: usize) -> usize {
        // First block have multiple pages,
        // Rest have only one page
        // TODO: why??Please explain it
        // Small object optimization: If it is a very small file, store it according to page. If it is a large object, store it according to the granularity of the chunk.
        if b_idx > 0 || self.conf.max_block_size < PAGE_SIZE {
            self.conf.max_block_size
        } else {
            PAGE_SIZE
        }
    }

    fn key(&self, indx: usize) -> CacheKey {
        CacheKey::new(self.id, indx, self.block_size(indx))
    }
}

pub struct RSlice {
    slice_helper: SliceHelper,
    // timeout layer, retry layer
    storage: Arc<Operator>,
    cache_manager: Arc<CacheManagerImpl>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,
}

impl Deref for RSlice {
    type Target = SliceHelper;

    fn deref(&self) -> &Self::Target {
        &self.slice_helper
    }
}

impl RSlice {
    fn keys(&self) -> Vec<CacheKey> {
        if self.length <= 0 {
            return vec![];
        }
        let last_indx = (self.length - 1) / self.conf.max_block_size;
        let mut keys = Vec::with_capacity(last_indx + 1);
        for i in 0..=last_indx {
            keys.push(self.key(i));
        }
        keys
    }

    fn should_random_partial_read(&self, off: usize, len: usize) -> bool {
        self.compressor.is_none()
            && off % self.conf.max_block_size > 0
            && len <= self.conf.max_block_size / 4
    }

    /// read data from storage and cache it
    async fn read_block(&self, key: &CacheKey, cache: bool, force_cache: bool) -> Result<Buffer> {
        let page_len = key.size();
        let start = Utc::now();
        let key_path = key.to_path(self.conf.is_hash_prefix);
        let reader = self.storage.reader(&key_path).await?;
        let buffer = reader.read(0..page_len as u64).await?;
        if buffer.len() < page_len {
            whatever!(
                "read {} not fully: ({} < {}) after {}",
                key_path,
                buffer.len(),
                page_len,
                Utc::now().signed_duration_since(start)
            )
        }
        if cache {
            self.cache_manager
                .put(key, Either::Left(buffer.clone()), force_cache)
                .await?;
        }
        Ok(buffer)
    }

    async fn delete(&self) -> Result<()> {
        if self.length == 0 {
            return Ok(());
        }

        let last_block_idx = self.block_index(self.length - 1);
        for i in 0..=last_block_idx {
            // there could be multiple clients try to remove the same chunk in the same time,
            // any of them should succeed if any blocks is removed
            let key = self.key(i);
            self.cache_manager.remove(&key).await;
        }

        for i in 0..=last_block_idx {
            let key = self.key(i);
            self.storage
                .delete(&key.to_path(self.conf.is_hash_prefix))
                .await?;
        }
        Ok(())
    }
}

impl SliceReader for RSlice {
    fn id(&self) -> u64 {
        self.id
    }

    fn read_at(&self, off: usize, len: usize) -> impl Future<Output = Result<Buffer>> + Send {
        async move {
            if len == 0 {
                return Ok(Bytes::new().into());
            }
            if off >= self.length || off + len > self.length {
                return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
            }

            let idx = self.block_index(off);
            let b_off = off % self.conf.max_block_size;
            let block_size = self.block_size(idx);
            assert!(block_size > b_off);
            let block_avail_size = block_size - b_off;
            // read beyond current block page
            if len > block_avail_size {
                let mut got = 0;
                let mut off = off;
                let mut bufs = vec![];
                while got < len {
                    // align to current block page
                    let block_avail_size =
                        self.block_size(self.block_index(off)) - off % self.conf.max_block_size;
                    let left_size = len - got;
                    let buffer = self
                        .read_at(off, min(left_size, block_avail_size))
                        .boxed()
                        .await?;
                    if buffer.is_empty() {
                        return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
                    }
                    got += buffer.len();
                    off += buffer.len();
                    bufs.push(buffer);
                }
                return Ok(bufs.into_iter().flatten().collect());
            }

            // read current block page
            // read data from cache if cache is enable
            let key = self.key(idx);
            let key_path = key.to_path(self.conf.is_hash_prefix);
            match self.cache_manager.get(&key).await {
                Ok(Some(Either::Left(p))) => {
                    assert!(p.len() > b_off);
                    return Ok(p.slice(b_off..min(len, p.len() - b_off)).into());
                }
                Ok(Some(Either::Right(mut p))) => {
                    assert!(p.len() > b_off);
                    return Ok(p.read_at(b_off, min(len, p.len() - b_off)).await?);
                }
                Err(e) => {
                    self.cache_manager.remove(&key).await;
                    warn!("read partial cached block {} fail: {}", &key_path, e);
                }
                _ => (),
            }

            // read from object storage
            if self.should_random_partial_read(off, len) {
                let reader = self.storage.reader(&key_path).await?;
                let range = b_off as u64..(b_off + len) as u64;
                match reader.read(range).await {
                    Ok(buffer) => return Ok(buffer),
                    // fallback to full read
                    Err(e) => error!("read partial block {} fail: {}", &key_path, e),
                }
            }
            let cache = self
                .group
                .execute(&key, async || {
                    self.read_block(&key, self.conf.should_cache(block_size), false)
                        .await
                })
                .await?;
            Ok(cache.slice(b_off..cache.len()).into())
        }
    }
}

pub struct WSlice {
    slice_helper: SliceHelper,
    storage: Arc<Operator>,
    cache_manager: Arc<CacheManagerImpl>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
    uploader: Arc<dyn Uploader>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,

    /// For every block, has a vector of pages
    /// /------------------/------------------/-----/
    /// /      block       /      block       / ... /
    /// / page / page / .. / page / page / .. / ... /
    /// block pages always be equal or larger than block size
    block_pages: Vec<Vec<BytesMut>>,
    /// The length of submitted uploading data length, but maybe not finished
    uploaded_len: usize,
    pengind_upload_tasks: JoinSet<Result<Either<Buffer, FileBuffer>>>,
}

impl Deref for WSlice {
    type Target = SliceHelper;

    fn deref(&self) -> &Self::Target {
        &self.slice_helper
    }
}

impl DerefMut for WSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slice_helper
    }
}

impl SliceWriter for WSlice {
    fn id(&self) -> u64 {
        self.id
    }

    fn write_all_at(&mut self, buffer: Buffer, off: usize) -> Result<()> {
        if off + buffer.len() > CHUNK_SIZE {
            whatever!(
                "write out of chunk boudary: {} > {}",
                off + buffer.len(),
                CHUNK_SIZE
            );
        }
        if off < self.uploaded_len {
            whatever!(
                "cannot overwrite uploadded block: {} < {}",
                off,
                self.uploaded_len
            );
        }

        // Fill pages with zeros until the offset
        if self.length < off {
            let zeros = Buffer::from(vec![0; off - self.length]);
            self.write_all_at(zeros, self.length)?;
        }

        let mut n = 0;
        while n < buffer.len() {
            let b_idx = self.block_index(off + n);
            let b_off = (off + n) % self.conf.max_block_size;
            let p_size = self.page_size(b_idx);
            let p_idx = b_off / p_size;
            let p_off = b_off % p_size;
            let page_cnt = self.block_pages[b_idx].len();
            let page = if p_idx < page_cnt {
                &mut self.block_pages[b_idx][p_idx]
            } else {
                let mut page = BytesMut::new();
                page.resize(p_size, 0); // only resize will fill zeros
                self.block_pages[b_idx].push(page);
                &mut self.block_pages[b_idx][page_cnt]
            };
            let left = buffer.len() - n;
            // every time copy until page finish
            let copied = min(page.len() - p_off, left);
            page[p_off..p_off + copied].copy_from_slice(&buffer.slice(n..n + copied).to_bytes());
            n += copied;
        }
        // if write beyond current length, update length
        if off + n > self.length {
            self.length = off + n;
        }

        Ok(())
    }

    fn spawn_flush_until(&mut self, offset: usize) -> Result<()> {
        if offset < self.uploaded_len {
            whatever!("Invalid offset: {} < {}", offset, self.uploaded_len);
        }

        for i in 0..self.block_pages.len() {
            let start = i * self.conf.max_block_size;
            let end = start + self.conf.max_block_size;
            if start >= self.uploaded_len && end <= offset {
                if !self.block_pages[i].is_empty() {
                    let block_len = self.block_size(i);
                    let cache_key = self.key(i);
                    let cache_key_path = cache_key.to_path(self.conf.is_hash_prefix);
                    let block = std::mem::take(&mut self.block_pages[i])
                        .into_iter()
                        .map(|a| a.freeze())
                        .flatten()
                        .collect::<Buffer>()
                        .slice(0..block_len);

                    let uploader = self.uploader.clone();
                    let cache_manager = self.cache_manager.clone();
                    let should_cache = block.len() < self.conf.max_block_size;
                    self.pengind_upload_tasks.spawn(async move {
                        if should_cache {
                            let _ = cache_manager
                                .put(&cache_key, Either::Left(block.clone()), false)
                                .await;
                        }

                        uploader.upload(&cache_key_path, block).await
                    });
                }
                self.uploaded_len = end;
            }
        }
        Ok(())
    }

    async fn abort(&mut self) {
        self.length = self.uploaded_len;
        let r_slice = RSlice {
            slice_helper: self.slice_helper.clone(),
            storage: self.storage.clone(),
            cache_manager: self.cache_manager.clone(),
            compressor: self.compressor.clone(),
            fetcher: self.fetcher.clone(),
            group: self.group.clone(),
        };
        match r_slice.delete().await {
            Ok(_) => (),
            Err(e) => {
                warn!("abort delete error: {}", e);
            }
        }
    }

    async fn finish(&mut self) -> Result<usize> {
        let n = (self.length - 1) / self.conf.max_block_size + 1;
        self.spawn_flush_until(n * self.conf.max_block_size)?;
        while let Some(join_res) = self.pengind_upload_tasks.join_next().await {
            match join_res {
                Ok(task_res) => {
                    if let Err(e) = task_res {
                        return Err(e);
                    }
                }
                Err(e) => {
                    whatever!("upload task cancel or panic error: {}", e);
                }
            }
        }
        Ok(self.length)
    }
}

pub struct CachedStore {
    /// concurrent limit and rate limit and retry limit
    /// all of those could put it in accesssor layer
    /// TODO: modify upload limit and download limit at runtime, add compressor at runtime
    /// retry 2 times, timeouit layer
    storage: Arc<Operator>,
    cache_manager: Arc<CacheManagerImpl>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
    uploader: Arc<dyn Uploader>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,
    conf: Arc<Config>,
}

impl CachedStore {
    pub fn new(storage: Arc<Operator>, config: Config, uploader: NormalUploader) -> Result<Self> {
        let cache_manager = Arc::new(CacheManagerImpl::new(&config, uploader)?);
        Ok(CachedStore {
            storage,
            cache_manager: cache_manager.clone(),
            compressor: None,
            uploader: cache_manager,
            fetcher: Arc::new(PreFetcher::new()),
            group: Arc::new(SingleFlight::new()),
            conf: Arc::new(config),
        })
    }
}

impl ChunkStore for CachedStore {
    type Writer = WSlice;
    type Reader = RSlice;

    /// Create a new reader for a slice
    fn new_reader(&self, id: u64, length: usize) -> Self::Reader {
        RSlice {
            slice_helper: SliceHelper {
                id,
                length,
                conf: self.conf.clone(),
            },
            storage: self.storage.clone(),
            cache_manager: self.cache_manager.clone(),
            compressor: self.compressor.clone(),
            fetcher: self.fetcher.clone(),
            group: self.group.clone(),
        }
    }

    /// Create a new writer for a slice
    fn new_writer(&self, id: u64) -> Self::Writer {
        WSlice {
            slice_helper: SliceHelper {
                id,
                length: 0,
                conf: self.conf.clone(),
            },
            storage: self.storage.clone(),
            cache_manager: self.cache_manager.clone(),
            compressor: self.compressor.clone(),
            uploader: self.uploader.clone(),
            fetcher: self.fetcher.clone(),
            group: self.group.clone(),
            block_pages: vec![Vec::default(); CHUNK_SIZE / self.conf.max_block_size],
            uploaded_len: 0,
            pengind_upload_tasks: Default::default(),
        }
    }

    /// Remove a slice from the store
    async fn remove(&self, id: u64, length: usize) -> Result<()> {
        let reader = self.new_reader(id, length);
        reader.delete().await
    }

    /// Fill cache
    async fn fill_cache(&self, id: u64, length: u32) -> Result<()> {
        let reader = self.new_reader(id, length as usize);
        let keys = reader.keys();
        for key in keys {
            if let Ok(Some(_)) = self.cache_manager.get(&key).await {
                continue;
            }
            reader.read_block(&key, true, true).await.inspect_err(|e| {
                warn!(
                    "fill cache {} fail: {}",
                    key.to_path(self.conf.is_hash_prefix),
                    e
                );
            })?;
        }
        Ok(())
    }

    /// Evict cache
    async fn evict_cache(&self, id: u64, length: u32) -> Result<()> {
        let reader = self.new_reader(id, length as usize);
        let keys = reader.keys();
        for key in keys {
            self.cache_manager.remove(&key).await;
        }
        Ok(())
    }

    /// Check cache
    async fn check_cache(&self, id: u64, length: u32) -> Result<u64> {
        let reader = self.new_reader(id, length as usize);
        let keys = reader.keys();
        let mut miss_bytes = 0;
        for (i, key) in keys.iter().enumerate() {
            if let Ok(Some(_)) = self.cache_manager.get(&key).await {
                continue;
            }
            miss_bytes += reader.block_size(i) as u64;
        }
        Ok(miss_bytes)
    }

    /// Get cache size
    fn used_memory(&self) -> i64 {
        self.cache_manager.used_memory()
    }

    /// Update rate limit
    fn set_update_limit(&self, upload: i64, download: i64) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chrono::Duration;
    use opendal::{services::Memory, Buffer, Operator};
    use tempfile::tempdir;
    use tokio::{fs::OpenOptions, io::AsyncWriteExt, task::JoinSet, time::sleep};
    use tracing_test::traced_test;

    use crate::{
        api::{ChunkStore, SliceReader, SliceWriter},
        cache::CacheManager,
        cached_store::Config,
        error::Result,
        uploader::NormalUploader,
    };

    use super::CachedStore;

    fn new_op() -> Operator {
        let builder = Memory::default().root("/tmp");
        Operator::new(builder).unwrap().finish()
    }

    fn new_cached_store(config: Config, op: Operator) -> Result<Arc<CachedStore>> {
        let operator = Arc::new(op);
        let uploader = NormalUploader::new(operator.clone(), None);
        let cache_store = CachedStore::new(operator, config, uploader)?;
        Ok(Arc::new(cache_store))
    }

    fn new_config() -> Config {
        let config = Config::default();
        config
    }

    async fn forget_slice(
        store: Arc<impl ChunkStore + 'static>,
        slice_id: u64,
        size: usize,
    ) -> Result<()> {
        let mut writer = store.new_writer(slice_id);
        let buf = Buffer::from(vec![0x41_u8; size]);
        writer.write_all_at(buf, 0)?;
        let s = writer.finish().await?;
        assert_eq!(s, size);
        Ok(())
    }

    async fn test_store(store: Arc<impl ChunkStore + 'static>) {
        // write
        let mut writer = store.new_writer(1);
        let data = Buffer::from("hello world");
        writer.write_all_at(data.clone(), 0).expect("write failed");
        let conf = Config::default();
        let offset = conf.max_block_size - 3;
        writer
            .write_all_at(data.clone(), offset)
            .expect("write failed");
        writer
            .spawn_flush_until(conf.max_block_size + 3)
            .expect("flush failed");

        let size = offset + data.len();
        assert_eq!(size, writer.finish().await.expect("finish failed"));

        // read
        let reader = store.new_reader(1, size);
        let n = reader.read_at(6, 5).await.expect("read failed");
        assert_eq!(5, n.len());
        let word = String::from_utf8(n.to_vec());
        assert!(word.is_ok(), "word is not a valid utf8 string");
        assert_eq!(word.unwrap(), "world");

        let read_eof = reader.read_at(offset, 20).await;
        assert!(read_eof.is_err());
        assert!(read_eof.unwrap_err().is_eof());

        let n = reader.read_at(offset, 11).await.expect("read failed");
        let word = String::from_utf8(n.to_vec());
        assert!(word.is_ok(), "word is not a valid utf8 string");
        assert_eq!(word.unwrap(), "hello world");

        let b_size = conf.max_block_size / 2;
        let mut join_set = JoinSet::new();
        for i in 2_u64..5 {
            let store_cloned = store.clone();
            join_set.spawn(async move {
                forget_slice(store_cloned.clone(), i, b_size).await?;
                sleep(std::time::Duration::from_millis(100)).await;
                store_cloned.remove(i, b_size).await
            });
        }
        while let Some(res) = join_set.join_next().await {
            assert!(
                res.is_ok(),
                "test concurrent write failed: {}",
                res.unwrap_err()
            );
        }
        store.remove(1, size).await.expect("remove failed");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_store_default() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];

        let store = new_cached_store(config, op).expect("create chunk store failed");
        test_store(store.clone()).await;

        let used = store.used_memory();
        assert_eq!(used, 0);
        let stats = store.cache_manager.stats();
        assert_eq!(stats.0, 0);
        assert_eq!(stats.1, 0);
    }

    // todo
    async fn test_store_mem_cache() {}

    // todo
    async fn test_store_compressed() {}

    // todo
    async fn test_store_limited() {}

    #[traced_test]
    #[tokio::test]
    async fn test_store_full() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];
        config.free_space = 0.9999;

        let store = new_cached_store(config, op).expect("create chunk store failed");
        test_store(store.clone()).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn test_small_buffer() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];
        config.buffer_size = 1 << 20;

        let store = new_cached_store(config, op).expect("create chunk store failed");
        test_store(store.clone()).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn test_write_back_no_delay() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];
        config.writeback = true;

        let store = new_cached_store(config, op.clone()).expect("create chunk store failed");
        // because no deplay, so wirte data will be read immediately
        test_store(store.clone()).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn test_write_back_delayed() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];
        config.writeback = true;
        config.upload_delay = Some(Duration::milliseconds(200));

        let store = new_cached_store(config, op.clone()).expect("create chunk store failed");
        sleep(std::time::Duration::from_secs(1)).await; // wait scan cache finished
                                                        // because write data will be cached, so wirte data will be read immediately
        test_store(store.clone()).await;

        forget_slice(store.clone(), 10, 1024)
            .await
            .expect("forget slice failed");
        sleep(std::time::Duration::from_secs(1)).await; // wait upload finished
        op.stat("chunks/0/0/10_0_1024")
            .await
            .expect("head object 10_0_1024 failed failed");
    }

    #[traced_test]
    #[tokio::test]
    async fn test_multi_buckets() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];
        config.is_hash_prefix = true;

        let store = new_cached_store(config, op.clone()).expect("create chunk store failed");
        test_store(store.clone()).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn test_fill_cache() {
        let op = new_op();
        let dir = tempdir().unwrap();
        let mut config = new_config();
        config.cache_dirs = vec![dir.path().to_path_buf()];
        config.cache_size = 10 << 20;
        config.free_space = 0.01;

        let store =
            new_cached_store(config.clone(), op.clone()).expect("create chunk store failed");

        let block_size = config.max_block_size;
        forget_slice(store.clone(), 10, 1024)
            .await
            .expect("forget slice failed");
        forget_slice(store.clone(), 11, block_size)
            .await
            .expect("forget slice failed");
        sleep(std::time::Duration::from_millis(100)).await; // wait for cache flush to disk
        let stats = store.cache_manager.stats();
        assert_eq!(stats.0, 1, "10_0_1024 should be cache");
        assert_eq!(
            stats.1,
            1024 + 4096,
            "10_0_1024 should be cache(with 4096 padding)"
        );

        store.fill_cache(10, 1024).await.expect("fill cache failed");
        store
            .fill_cache(11, block_size as u32)
            .await
            .expect("fill cache failed");
        sleep(std::time::Duration::from_secs(1)).await;
        let stats = store.cache_manager.stats();
        assert_eq!(stats.0, 2, "10_0_1024 and 11_0_bsize should be cache");
        assert_eq!(
            stats.1,
            1024 + 4096 + block_size as i64 + 4096,
            "10_0_1024 and 11_0_bsize should be cache"
        );

        // check
        let miss_bytes = store
            .check_cache(10, 1024)
            .await
            .expect("check cache failed");
        assert_eq!(miss_bytes, 0);

        let miss_bytes = store
            .check_cache(11, block_size as u32)
            .await
            .expect("check cache failed");
        assert_eq!(miss_bytes, 0);

        store
            .evict_cache(11, block_size as u32)
            .await
            .expect("evict cache failed");
        let stats = store.cache_manager.stats();
        assert_eq!(stats.0, 1, "10_0_1024 should be cache");
        assert_eq!(
            stats.1,
            1024 + 4096,
            "10_0_1024 should be cache(with 4096 padding)"
        );

        // check again
        let miss_bytes = store
            .check_cache(11, block_size as u32)
            .await
            .expect("check cache failed");
        assert_eq!(miss_bytes, block_size as u64);
    }
}
