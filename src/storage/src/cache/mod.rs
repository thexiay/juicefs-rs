mod disk;
mod mem;

use bytes::Bytes;
pub use disk::DiskCacheManager;
use opendal::Buffer;

use std::future::Future;

use crate::{api::SliceReader, error::Result};

pub type TotalAndUsed = (i64, i64);
/// CacheManager is a trait for managing cache
/// Key: {slice}_{block_idx}_{block_size}
/// Cache: bytes, aligned at block and at least larger than block size
pub trait CacheManager: Send + Sync + 'static {
    /// Cache a page
    fn put(&self, key: &str, p: Bytes, force: bool) -> impl Future<Output = Result<usize>> + Send;

    /// Remove a page
    fn remove(&self, key: &str) -> impl Future<Output = Result<()>> + Send;

    /// Load a page reader
    fn get(&self, key: &str) -> impl Future<Output = Result<Bytes>> + Send;


    fn uploaded(&self, key: &str, size: i32) -> impl Future<Output = ()> + Send;

    /// Stage data. 
    /// Returns a path to the staged data 
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to store the data under
    /// * `data` - The data to store
    /// * `keep_cache` - Whether to keep the data in the cache
    fn stage(&self, key: String, data: Vec<u8>, keep_cache: bool) -> impl Future<Output = Result<String>> + Send;

    /// Remove staged data.
    fn remove_stage(&self, key: String) -> impl Future<Output = Result<()>> + Send;

    /// Returns a staged data path according to the key
    fn stage_path(&self, key: String) -> String;

    /// Returns the number of cached items and the total size of the cache
    fn stats(&self) -> TotalAndUsed;
    fn used_memory(&self) -> i64;
    fn is_empty(&self) -> bool;
}
