use std::{collections::HashMap, fs::Permissions, sync::{atomic::AtomicI64, Arc, Mutex, RwLock}};

use chrono::Duration;
use dashmap::DashMap;
use hashring::HashRing;
use tokio::task::JoinHandle;

use crate::page::Page;
use crate::error::Result;

/// A cache manager that manage lots of [ `DiskCache` ] keyed by a object key
pub struct DiskCacheManager(RwLock<ConsistentHashDiskCache>);

impl DiskCacheManager {
    pub fn new() -> Self {
        DiskCacheManager(RwLock::new(ConsistentHashDiskCache {
            hash_ring: HashRing::new(),
            caches: HashMap::new()
        }))
    }

    fn get() -> Arc<Cache> {
        unimplemented!()
    }

    fn remove(key: String) {
        unimplemented!()
    }

    fn is_empty() -> bool {
        unimplemented!()
    }
}

struct ConsistentHashDiskCache {
    hash_ring: HashRing<u64>,
    caches: HashMap<u64, Arc<Cache>>
}

struct CacheKey {
    key: String,
    hash: u64,
}

struct CacheItem {
    size: i32,
    atime: u32,
}

enum CacheState {
    Unknowed,
    Normal,
    Unstable,
    Down,
    Unchanged,
}

/// A cache store that stores pages on disk
struct Cache {
    /// cache unique id
    id: String,
    /// cached pages
    pages: DashMap<String, Page>,
    /// cached pages statistics, it will be scanned to correct it.
    pages_stats: RwLock<HashMap<CacheKey, CacheItem>>,
    /// used page space in bytes
    used_page: AtomicI64,
    /// used space in bytes
    used_space: i64,
    /// max space for current cache
    max_space: i64,
    free_space_ratio: f32,
    /// cache dir
    dir: String,
    /// cache file permission
    mode: Permissions,
    hash_prefix: bool,
    scan_interval: Duration,
    cache_expire: Duration,
    
    scanned: bool,
    stage_full: bool,
    raw_full: bool,
    eviction: String,
    checksum: String,  // checksum level

    op_ts: HashMap<Duration, Box<dyn FnOnce() -> Result<()>>>,
    state: Mutex<CacheState>,
}

impl Cache {
    /// Flush pending file into disk intevally
    fn spawn_flush(&self) -> JoinHandle<()> {
        unimplemented!()
    }

    /// Check lock file exists.
    fn spawn_check_lock_file(&self) -> JoinHandle<()> {
        unimplemented!()
    }

    /// Check free space ratio and evict cache pages if necessary
    fn spawn_check_free_space_ratio(&self) -> JoinHandle<()> {
        unimplemented!()
    }
    
    /// Check cache pages expire and evict cache pages if necessary
    fn spawn_check_expire(&self) -> JoinHandle<()> {
        unimplemented!()
    }
    
    /// Check cache dir to auto correct cache file real size
    fn spawn_check_cache_item(&self) -> JoinHandle<()> {
        unimplemented!()
    }

    /// Check io timeout operations to mark it as unstable
    fn spawn_check_io_timeout(&self) -> JoinHandle<()> {
        unimplemented!()
    }

    /// Upload staging file to object storage
    fn spawn_upload_staging(&self) -> JoinHandle<()> {
        unimplemented!()
    }

    
}