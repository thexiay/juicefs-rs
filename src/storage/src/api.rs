use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::error::Result;

pub trait SliceWriter {
    fn id(&self) -> u64;

    /// write whole buffer at offset of slice
    fn write_at(&mut self, buffer: &[u8], off: usize) -> impl Future<Output = Result<usize>> + Send;

    /// flush data from 0 to offset into storage
    fn flush_to(&mut self, offset: usize) -> JoinHandle<Result<()>>;

    /// Abort the write and flush progress.
    fn abort(&mut self);
}

pub trait SliceReader {
    fn id(&self) -> u64;

    fn read_at(&mut self, buffer: &mut [u8], off: usize) -> impl Future<Output = Result<usize>> + Send;
}

#[async_trait]
pub trait ChunkStore {
    type Writer: SliceWriter;
    type Reader: SliceReader;

    /// Create a new reader for a slice
    fn new_reader(self: Arc<Self>, id: u64, length: usize) -> Self::Reader;

    /// Create a new writer for a slice
    fn new_writer(self: Arc<Self>, id: u64) -> Self::Writer;

    /// Remove a slice from the store
    async fn remove(&self, id: u64, length: usize) -> Result<()>;

    /// Fill cache
    async fn fill_cache(&self, id: u64, length: u32) -> Result<()>;

    /// Evict cache
    async fn evict_cache(&self, id: u64, length: u32) -> Result<()>;

    /// Check cache
    async fn check_cache(&self, id: u64, length: u32) -> Result<u64>;

    /// Get cache size
    fn used_memory(&self) -> i64;

    /// Update rate limit
    fn set_update_limit(&self, upload: i64, download: i64);
}
