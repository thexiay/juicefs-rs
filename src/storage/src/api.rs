use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use opendal::Buffer;
use tokio::task::JoinHandle;

use crate::error::Result;

pub trait SliceWriter: Send + Sync {
    fn id(&self) -> u64;

    /// Read data from buffer, and write data into slice at offset.
    /// Returns the number of bytes written.
    fn write_all_at(&mut self, buffer: Buffer, off: usize) -> impl Future<Output = Result<()>> + Send;

    /// flush data from 0 to offset into storage
    fn flush_to(&mut self, offset: usize) -> JoinHandle<Result<()>>;

    /// Abort the write and flush progress.
    fn abort(&mut self);
}

pub trait SliceReader: Send + Sync {
    fn id(&self) -> u64;

    /// Read data from slice at offset, and write it into buffer.
    /// Returns the bytes readed.
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
                    },
                    Err(err) => return Err(err),
                }
            }
            Ok(bufs.into_iter().flatten().collect())
        }
    }
}

#[async_trait]
pub trait ChunkStore {
    type Writer: SliceWriter;
    type Reader: SliceReader;

    /// Create a new reader for a slice
    fn new_reader(&self, id: u64, length: usize) -> Self::Reader;

    /// Create a new writer for a slice
    fn new_writer(&self, id: u64) -> Self::Writer;

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
