use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{api::MAX_VERSION, error::{NotIncompatibleClientSnafu, Result}};

// Config for clients.
#[derive(Clone)]
pub struct Config {
	pub strict: bool,  // update ctime
	pub retries: isize, // number of retries
    // max delete gc threads
    pub max_deletes_threads: isize, 
    // max delete task in queue
    pub max_deletes_task: isize, 
    pub skip_dir_nlink: isize,
    pub case_insensi: bool,
    pub read_only: bool,
    pub no_bg_job: bool,  // disable background jobs like clean-up, backup, etc.
    pub open_cache: Duration,
    /// max number of files to cache (soft limit)
    pub open_cache_limit: u64,
    pub heartbeat: Duration,
    pub mount_point: String,
    pub subdir: String,
    pub atime_mode: String,
    pub dir_stat_flush_period: Duration,
    /// skip directory mtime update if the difference is less than this value
    pub skip_dir_mtime: Duration,
    pub sid: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            strict: true,
            retries: 10,
            max_deletes_threads: 2,
            max_deletes_task: 100,
            skip_dir_nlink: 0,
            case_insensi: false,
            read_only: false,
            no_bg_job: false,
            open_cache: Duration::from_secs(60),
            open_cache_limit: 100000,
            heartbeat: Duration::from_secs(10),
            mount_point: "/".to_string(),
            subdir: "juicefs".to_string(),
            atime_mode: "relatime".to_string(),
            dir_stat_flush_period: Duration::from_secs(60),
            skip_dir_mtime: Duration::from_secs(1),
            sid: None,
        }
    }
}

impl Config {
    pub fn check(&mut self) {
        if self.max_deletes_threads == 0 {
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

/// Config for server
#[derive(Default, Clone, Serialize, Deserialize)]
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
    pub capacity: u64,
    pub inodes: u64,
    pub encrypt_key: Option<String>,
    pub encrypt_algo: Option<String>,
    pub key_encrypted: Option<bool>,
    pub upload_limit: Option<i64>, // Mbps
    pub download_limit: Option<i64>, // Mbps
    pub trash_days: i32,
    pub meta_version: i32,
    pub min_client_version: Option<String>,
    pub max_client_version: Option<String>,
    pub enable_dir_stats: bool,
    pub enable_acl: bool,
}

impl Format {
    /// check format can be update or not
    pub fn check_ugrade(&self, old: &Format, force: bool) -> Result<()> {
        Ok(())
    }

    pub fn check_version(&self) -> Result<()> {
        if self.meta_version > MAX_VERSION {
            return NotIncompatibleClientSnafu{ 
                version: self.meta_version 
            }.fail();
        }
    
        // TODO: check client version
        Ok(())
    }
}