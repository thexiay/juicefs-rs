mod disk;
mod mem;

use async_trait::async_trait;
use either::Either;
use opendal::Buffer;
use std::future::Future;

use crate::{buffer::FileBuffer, error::Result};

pub type TotalAndUsed = (i64, i64);

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct CacheKey {
    id: u64,
    indx: usize,
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
    fn put(&self, key: &CacheKey, p: Either<Buffer, FileBuffer>, force: bool) -> impl Future<Output = Result<()>> + Send;

    /// Remove a page
    fn remove(&self, key: &CacheKey) -> impl Future<Output = ()> + Send;

    /// Load a page reader
    fn get(&self, key: &CacheKey) -> impl Future<Output = Result<Option<Either<Buffer, FileBuffer>>>> + Send;

    /// --------------- Metadata API ---------------
    /// Returns the number of cached items and the total size of the cache
    fn stats(&self) -> TotalAndUsed;
    
    /// Cost memory
    fn used_memory(&self) -> i64;

    /// Whether the cache is invalid
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