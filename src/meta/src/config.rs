use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::Result;

// Config for clients.
#[derive(Default)]
pub struct Config {
	pub strict: bool,  // update ctime
	pub retries: isize, // number of retries
    pub max_deletes: isize, 
    pub skip_dir_nlink: isize,
    pub case_insensi: bool,
    pub read_only: bool,
    pub no_bg_job: bool,  // disable background jobs like clean-up, backup, etc.
    pub open_cache: Duration,
    pub open_cache_limit: u64, // max number of files to cache (soft limit)
    pub heartbeat: Duration,
    pub mount_point: String,
    pub subdir: String,
    pub atime_mode: String,
    pub dir_stat_flush_period: Duration,
    pub skip_dir_mtime: Duration,
    pub sid: u64,
}

impl Config {
    pub fn check(&mut self) {
        if self.max_deletes == 0 {
            warn!("Deleting object will be disabled since max-deletes is 0")
        }
        if self.heartbeat < Duration::from_secs(1) {
            warn!("heartbeat should not be less than 1 second");
            self.heartbeat = Duration::from_secs(1);
        }
        if self.heartbeat > Duration::from_mins(10) {
            warn!("heartbeat shouldd not be greater than 10 minutes");
            self.heartbeat = Duration::from_mins(10)
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Format {
    pub name: String,
    pub uuid: String,
    pub storage: String,
    pub storage_class: Option<String>,
    pub bucket: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub block_size: i32,
    pub compression: Option<String>,
    pub shards: Option<i32>,
    pub hash_prefix: Option<bool>,
    pub capacity: Option<u64>,
    pub inodes: Option<u64>,
    pub encrypt_key: Option<String>,
    pub encrypt_algo: Option<String>,
    pub key_encrypted: Option<bool>,
    pub upload_limit: Option<i64>, // Mbps
    pub download_limit: Option<i64>, // Mbps
    pub trash_days: i32,
    pub meta_version: Option<i32>,
    pub min_client_version: Option<String>,
    pub max_client_version: Option<String>,
    pub dir_stats: bool,
    pub enable_acl: bool,
}

impl Format {
    /// check format can be update or not
    pub fn check_ugrade(&self, old: &Format, force: bool) -> Result<()> {
        Ok(())
    }
}