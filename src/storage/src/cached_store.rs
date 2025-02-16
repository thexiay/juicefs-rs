use std::cmp::min;
use std::env;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fs::Permissions, future::Future, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Duration, Utc};
use either::Either;
use futures::future::FutureExt;
use opendal::{Buffer, Operator};
use regex::Regex;
use snafu::whatever;
use tokio::task::JoinSet;
use tracing::{error, warn};

use crate::api::{ChunkStore, SliceWriter};
use crate::buffer::{ChecksumLevel, FileBuffer};
use crate::cache::CacheKey;
use crate::compress::CompressArgo;
use crate::error::Result;
use crate::uploader::Uploader;
use crate::{
    api::SliceReader, cache::CacheManager, compress::Compressor, pre_fetcher::PreFetcher,
    single_flight::SingleFlight,
};

const CHUNK_SIZE: usize = 64 << 20; // 64MB
const PAGE_SIZE: usize = 64 << 10; // 64KB

// object stroage file name format
pub static BLOCK_FILE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^chunks/\d+/\d+/(\d+)_(\d+)_(\d+)$").unwrap());

// Config contains options for cachedStore
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
            cache_file_mode: Permissions::from_mode(0o666),
            cache_size: 10 << 20,  // 10MB
            cache_checksum: ChecksumLevel::None,
            cache_eviction: false,
            cache_scan_interval: Duration::seconds(300),
            cache_expire: None,
            free_space: 0.0,
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
            max_block_size: 1 << 20,
            get_timeout: Duration::seconds(60),
            put_timeout: Duration::seconds(60),
            cache_full_block: false,
            buffer_size: 32 << 20,  // 32MB
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
    fn block_size(&self, indx: usize) -> usize {
        let bsize = self.length - indx * self.conf.max_block_size;
        if bsize > self.conf.max_block_size {
            self.conf.max_block_size
        } else {
            bsize
        }
    }

    fn key(&self, indx: usize) -> CacheKey {
        CacheKey::new(self.id, indx, self.block_size(indx))
    }
}

pub struct RSlice<CM: CacheManager> {
    slice_helper: SliceHelper,
    // timeout layer, retry layer
    storage: Arc<Operator>,
    cache_manager: Arc<CM>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,
}

impl<CM: CacheManager> Deref for RSlice<CM> {
    type Target = SliceHelper;

    fn deref(&self) -> &Self::Target {
        &self.slice_helper
    }
}

impl<CM: CacheManager> RSlice<CM> {
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

    fn parse_block_size(key: &str) -> usize {
        key.rfind('_')
            .map(usize::from)
            .expect("failed to parse block size")
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

impl<CM> SliceReader for RSlice<CM>
where
    CM: CacheManager,
{
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

            let indx = self.block_index(off);
            let boff = off % self.conf.max_block_size;
            let block_size = self.block_size(indx);
            assert!(block_size > boff);
            let block_avail_size = block_size - boff;
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
            let key = self.key(indx);
            let key_path = key.to_path(self.conf.is_hash_prefix);
            match self.cache_manager.get(&key).await {
                Ok(Some(Either::Left(p))) => {
                    return Ok(p.slice(0..min(len, p.len())).into());
                }
                Ok(Some(Either::Right(mut p))) => {
                    return Ok(p.read_at(0, min(len, p.len())).await?);
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
                let range = boff as u64..(boff + len) as u64;
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
            Ok(cache.slice(boff..cache.len()).into())
        }
    }
}

pub struct WSlice<CM, U, const N: usize>
where
    CM: CacheManager,
    U: Uploader,
{
    slice_helper: SliceHelper,
    storage: Arc<Operator>,
    cache_manager: Arc<CM>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
    uploader: Arc<U>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,

    /// For every block, has a vector of pages
    /// block pages always be equal or larger than block size
    block_pages: [Vec<BytesMut>; N],
    /// The length of submitted uploading data length, but maybe not finished
    uploaded_len: usize,
    pengind_upload_tasks: JoinSet<Result<Either<Buffer, FileBuffer>>>,
}

impl<CM: CacheManager, U: Uploader, const N: usize> Deref for WSlice<CM, U, N> {
    type Target = SliceHelper;

    fn deref(&self) -> &Self::Target {
        &self.slice_helper
    }
}

impl<CM: CacheManager, U: Uploader, const N: usize> DerefMut for WSlice<CM, U, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slice_helper
    }
}

impl<CM: CacheManager, U: Uploader, const N: usize> SliceWriter for WSlice<CM, U, N> {
    fn id(&self) -> u64 {
        self.id
    }

    fn write_all_at(
        &mut self,
        buffer: Buffer,
        off: usize,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
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
                self.write_all_at(zeros, self.length).boxed().await?;
            }

            let mut n = 0;
            while n < buffer.len() {
                let indx = self.block_index(off + n);
                let b_off = (off + n) % self.conf.max_block_size;
                // first block have multiple pages, the rest have only one page
                let p_size = if indx > 0 || self.conf.max_block_size < PAGE_SIZE {
                    self.conf.max_block_size
                } else {
                    PAGE_SIZE
                };
                let p_idx = b_off / p_size;
                let p_off = b_off % p_size;
                let page_cnt = self.block_pages[indx].len();
                let page = if p_idx < page_cnt {
                    &mut self.block_pages[indx][p_idx]
                } else {
                    let page = BytesMut::with_capacity(p_size);
                    self.block_pages[indx].push(page);
                    &mut self.block_pages[indx][page_cnt]
                };
                let left = buffer.len() - n;
                // every time copy until page finish
                let copied = min(page.len() - p_off, left);
                page[p_off..p_off + copied]
                    .copy_from_slice(&buffer.slice(n..n + copied).to_bytes());
                n += copied;
            }
            // if write beyond current length, update length
            if off + n > self.length {
                self.length = off + n;
            }

            Ok(())
        }
    }

    fn spawn_flush_to(&mut self, offset: usize) -> Result<()> {
        if offset < self.uploaded_len {
            whatever!("Invalid offset: {} < {}", offset, self.uploaded_len);
        }

        for i in 0..self.block_pages.len() {
            let start = i * self.conf.max_block_size;
            let end = start + self.conf.max_block_size;
            if start >= self.uploaded_len && end <= offset {
                if !self.block_pages[i].is_empty() {
                    let block_len = self.block_size(i);
                    let key_path = self.key(i).to_path(self.conf.is_hash_prefix);
                    let block = std::mem::take(&mut self.block_pages[i])
                        .into_iter()
                        .map(|a| a.freeze())
                        .flatten()
                        .collect::<Buffer>()
                        .slice(0..block_len);
                    let uploader = self.uploader.clone();
                    self.pengind_upload_tasks
                        .spawn(async move { uploader.upload(&key_path, block).await });
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

    async fn finish(&mut self) -> Result<()> {
        let n = (self.length - 1) / self.conf.max_block_size + 1;
        self.spawn_flush_to(n * self.conf.max_block_size)?;
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
        Ok(())
    }
}

struct CachedStore<CM, U, const N: usize>
where
    CM: CacheManager,
    U: Uploader,
{
    /// concurrent limit and rate limit and retry limit
    /// all of those could put it in accesssor layer
    /// TODO: modify upload limit and download limit at runtime, add compressor at runtime
    /// retry 2 times, timeouit layer
    storage: Arc<Operator>,
    cache_manager: Arc<CM>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
    uploader: Arc<U>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,
    conf: Arc<Config>,
    start_hour: u32,
    end_hour: u32,
    _phantom: PhantomData<[(); N]>,
}

#[async_trait]
impl<CM, U, const N: usize> ChunkStore for CachedStore<CM, U, N>
where
    CM: CacheManager,
    U: Uploader,
{
    type Writer = WSlice<CM, U, 1>;
    type Reader = RSlice<CM>;

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
            block_pages: Default::default(),
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
