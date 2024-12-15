use std::{fs::Permissions, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use opendal::Operator;

use crate::{cache::CacheManager, compress::Compressor, pre_fetcher::PreFetcher, single_flight::Controller};

// Config contains options for cachedStore
pub struct Config {
    cache_dir: String,
    cache_mode: Permissions,
    cache_size: u64,
    cache_checksum: String,
    cache_eviction: String,
    cache_scan_interval: Duration,
    cache_expire: Duration,
    free_space: f32,
    auto_create: bool,
    compress: String,
    max_upload: isize,
    max_stage_write: isize,
    max_retries: isize,
    upload_limit: i64,
    download_limit: i64,
    writeback: bool,
    upload_delay: Duration,
    upload_hours: String,
    hash_prefix: bool,
    block_size: isize,
    get_timeout: Duration,
    put_timeout: Duration,
    cache_full_block: bool,
    buffer_size: u64,
    readahead: isize,
    prefetch: isize,
}

pub struct PendingItem {
    key: String,
    /// full path of local file corresponding 
    fpath: String,
    /// timestamp when this item is added
    ts: DateTime<Utc>,
    uploading: bool,
}

struct CachedStore<CM, COMP>
where
    CM: CacheManager,
    COMP: Compressor,
{
    /// concurrent limit and rate limit and retry limit
    /// all of those could put it in accesssor layer
    /// TODO: modify upload limit and download limit at runtime, add compressor at runtime
    storage: Arc<Operator>,
    cache_manager: Arc<CM>,
    compressor: Arc<COMP>,
    fetcher: Arc<PreFetcher>,
    group: Arc<Controller>,
    conf: Config,
    start_hour: u32,
    end_hour: u32,
}

