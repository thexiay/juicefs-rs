use std::{future::Future, sync::Arc};

use opendal::{Buffer, Operator};

pub use crate::cached_store::CachedStore;
pub use crate::cached_store::Config;
pub use crate::cached_store::RSlice;
pub use crate::cached_store::WSlice;
use crate::{error::Result, uploader::NormalUploader};

pub trait SliceWriter: Send + Sync {
    fn id(&self) -> u64;

    /// Read data from [`buffer`], and write data into slice at offset.
    /// This is a overwrite write.
    ///
    /// [`buffer`]: written data
    /// [`off`]: offset in slice
    fn write_all_at(&mut self, buffer: Buffer, off: usize) -> Result<()>;

    /// Flush data from `0..offset`(Relative to slice writen before) in slice into storage
    /// 
    /// [`offset`]: offset in slice
    fn spawn_flush_until(&mut self, offset: usize) -> Result<()>;

    /// Abort the write and flush progress.
    fn abort(&mut self) -> impl Future<Output = ()> + Send;

    /// Finish the flush progress, return whole written size from 0 to length.
    fn finish(&mut self) -> impl Future<Output = Result<usize>> + Send;
}

pub trait SliceReader: Send + Sync {
    fn id(&self) -> u64;

    /// Read data from slice at offset, and write it into buffer.
    /// Returns the bytes readed.
    ///
    /// [`off`]: offset in slice
    /// [`len`]: length to read
    fn read_at(&self, off: usize, len: usize) -> impl Future<Output = Result<Buffer>> + Send;

    fn read_all_at(&self, off: usize, len: usize) -> impl Future<Output = Result<Buffer>> + Send {
        async move {
            let mut bufs = vec![];
            let mut off = off;
            let mut len = len;
            loop {
                match self.read_at(off, len).await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => {
                        off += buf.len();
                        len -= buf.len();
                        bufs.push(buf);
                    }
                    Err(err) => return Err(err),
                }
            }
            Ok(bufs.into_iter().flatten().collect())
        }
    }
}

pub trait ChunkStore: Send + Sync {
    type Writer: SliceWriter;
    type Reader: SliceReader;

    /// Create a new reader for a slice
    fn new_reader(&self, id: u64, length: usize) -> Self::Reader;

    /// Create a new writer for a slice
    fn new_writer(&self, id: u64) -> Self::Writer;

    /// Remove a slice from the store
    fn remove(&self, id: u64, length: usize) -> impl Future<Output = Result<()>> + Send;

    /// Fill cache for a slice
    fn fill_cache(&self, id: u64, length: u32) -> impl Future<Output = Result<()>> + Send;

    /// Evict cache for a slice
    fn evict_cache(&self, id: u64, length: u32) -> impl Future<Output = Result<()>> + Send;

    /// Check missed cache for a slice
    fn check_cache(&self, id: u64, length: u32) -> impl Future<Output = Result<u64>> + Send;

    /// Get cache size
    fn used_memory(&self) -> i64;

    /// Update rate limit
    fn set_update_limit(&self, upload: i64, download: i64);
}

pub fn new_chunk_store(config: Config, operator: Operator) -> Result<CachedStore> {
    let operator = Arc::new(operator);
    let cache_store = CachedStore::new(operator, config)?;
    Ok(cache_store)
    // todo: add switch disk cache manager to mem cache manager
}
