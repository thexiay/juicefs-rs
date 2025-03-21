mod disk;
mod mem;

use async_trait::async_trait;
use disk::DiskCacheManager;
pub use disk::DiskEvent;
use either::Either;
use mem::MemCacheManager;
use opendal::{Buffer, Operator};
use snafu::whatever;
use std::{future::Future, sync::Arc};

use crate::{
    buffer::FileBuffer,
    cached_store::Config,
    error::Result,
    uploader::{NormalUploader, Uploader},
};

pub type CacheCntAndSize = (i64, i64);

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct CacheKey {
    /// slice id
    id: u64,
    /// block index
    indx: usize,
    /// cache block size
    size: usize,
}

impl CacheKey {
    pub fn new(id: u64, indx: usize, size: usize) -> Self {
        CacheKey { id, indx, size }
    }

    pub fn to_path(&self, is_hash_prefix: bool) -> String {
        if is_hash_prefix {
            format!(
                "chunks/{:02X}/{}/{}_{}_{}",
                self.id % 256,
                self.id / 1000 / 1000,
                self.id,
                self.indx,
                self.size
            )
        } else {
            format!(
                "chunks/{}/{}/{}_{}_{}",
                self.id / 1000 / 1000,
                self.id / 1000,
                self.id,
                self.indx,
                self.size
            )
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

/// CacheManager is a trait for managing cache
/// Key: {slice}_{block_idx}_{block_size}
/// Cache: bytes, aligned at block and at least larger than block size
pub trait CacheManager: Send + Sync + 'static {
    // ----------------- Cache API -----------------
    /// Cache a page.
    /// Put a page don't mean to persist it immediately. It may be staged to disk first.
    /// If `force` is true, it will make sure cache will be ok
    fn put(
        &self,
        key: &CacheKey,
        p: Either<Buffer, FileBuffer>,
        force: bool,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Remove a page
    fn remove(&self, key: &CacheKey) -> impl Future<Output = ()> + Send;

    /// Load a page reader
    fn get(
        &self,
        key: &CacheKey,
    ) -> impl Future<Output = Result<Option<Either<Buffer, FileBuffer>>>> + Send;

    /// --------------- Metadata API ---------------
    /// Returns the number of cached items and the total size of the cache
    fn stats(&self) -> CacheCntAndSize;

    /// Cost memory
    fn used_memory(&self) -> i64;

    /// Whether the cache is invalid, it's use for fallback CacheManager
    fn is_invalid(&self) -> bool;
}

#[async_trait]
pub trait CacheReader: Send + 'static {
    /// read all data from cache. It ensure output `Buffer` length is same as input `len`
    async fn read_all_at(&mut self, off: usize, len: usize) -> Result<Buffer>;

    fn len(&self) -> usize;
}

#[async_trait]
impl CacheReader for Box<dyn CacheReader> {
    async fn read_all_at(&mut self, off: usize, len: usize) -> Result<Buffer> {
        self.read_all_at(off, len).await
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

pub enum CacheManagerImpl {
    Disk(DiskCacheManager),
    Mem(MemCacheManager),
}

impl CacheManagerImpl {
    pub fn new(config: &Config, operator: Arc<Operator>) -> Result<Self> {
        match config.cache_type.to_lowercase().as_str() {
            "mem" => {
                let mem = MemCacheManager::new();
                Ok(CacheManagerImpl::Mem(mem))
            },
            "disk" => {
                if config.cache_size == 0 {
                    whatever!("cache_size must be set when cache_type is disk");
                }
                Ok(CacheManagerImpl::Disk(DiskCacheManager::new(config, operator)?))
            },
            _ => whatever!("invalid cache_type"),
        }
    }

    pub fn uploader(&self) -> Option<Arc<dyn Uploader>> {
        match self {
            CacheManagerImpl::Disk(d) => Some(Arc::new(d.clone())),
            CacheManagerImpl::Mem(_) => None,
        }
    }
}

impl CacheManager for CacheManagerImpl {
    async fn put(
        &self,
        key: &CacheKey,
        p: Either<Buffer, FileBuffer>,
        force: bool,
    ) -> Result<()> {
        match self {
            CacheManagerImpl::Disk(d) => d.put(key, p, force).await,
            CacheManagerImpl::Mem(m) => m.put(key, p, force).await,
        }
    }

    async fn remove(&self, key: &CacheKey) {
        match self {
            CacheManagerImpl::Disk(d) => d.remove(key).await,
            CacheManagerImpl::Mem(m) => m.remove(key).await,
        }
    }

    async fn get(
        &self,
        key: &CacheKey,
    ) -> Result<Option<Either<Buffer, FileBuffer>>> {
        match self {
            CacheManagerImpl::Disk(d) => d.get(key).await,
            CacheManagerImpl::Mem(m) => m.get(key).await,
        }
    }

    fn stats(&self) -> CacheCntAndSize {
        match self {
            CacheManagerImpl::Disk(d) => d.stats(),
            CacheManagerImpl::Mem(m) => m.stats(),
        }
    }

    fn used_memory(&self) -> i64 {
        match self {
            CacheManagerImpl::Disk(d) => d.used_memory(),
            CacheManagerImpl::Mem(m) => m.used_memory(),
        }
    }

    fn is_invalid(&self) -> bool {
        match self {
            CacheManagerImpl::Disk(d) => d.is_invalid(),
            CacheManagerImpl::Mem(m) => m.is_invalid(),
        }
    }
}
