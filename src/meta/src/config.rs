use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    api::MAX_VERSION,
    error::{NotIncompatibleClientSnafu, Result, UpgradeFormatSnafu},
};

// Config for clients.
#[derive(Clone)]
pub struct Config {
    pub strict: bool,   // update ctime
    pub retries: isize, // number of retries
    // max delete gc threads
    pub max_deletes_threads: isize,
    // max delete task in queue
    pub max_deletes_task: isize,
    pub skip_dir_nlink: isize,
    pub case_insensi: bool,
    pub read_only: bool,
    // enable background jobs like clean-up, backup, etc.
    pub enable_bg_job: bool,
    // open files cache expire time
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
    /// user set sid
    pub sid: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            strict: true,
            retries: 3,
            max_deletes_threads: 2,
            max_deletes_task: 100,
            skip_dir_nlink: 0,
            case_insensi: false,
            read_only: false,
            enable_bg_job: true,
            open_cache: Duration::from_secs(60),
            open_cache_limit: 100000,
            heartbeat: Duration::from_secs(10),
            mount_point: "/".to_string(),
            subdir: "juicefs".to_string(),
            atime_mode: "relatime".to_string(),
            dir_stat_flush_period: Duration::from_secs(1),
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
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
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
    pub upload_limit: Option<i64>,   // Mbps
    pub download_limit: Option<i64>, // Mbps
    // max days to remain for trash(trash dir and trash slices)
    pub trash_days: u8,
    pub meta_version: i32,
    pub min_client_version: Option<String>,
    pub max_client_version: Option<String>,
    pub enable_dir_stats: bool,
    pub enable_acl: bool,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            name: Default::default(),
            uuid: Default::default(),
            storage: Default::default(),
            storage_class: Default::default(),
            bucket: Default::default(),
            access_key: Default::default(),
            secret_key: Default::default(),
            session_token: Default::default(),
            block_size: Default::default(),
            compression: Default::default(),
            shards: Default::default(),
            hash_prefix: Default::default(),
            capacity: Default::default(),
            inodes: Default::default(),
            encrypt_key: Default::default(),
            encrypt_algo: Default::default(),
            key_encrypted: Default::default(),
            upload_limit: Default::default(),
            download_limit: Default::default(),
            trash_days: Default::default(),
            meta_version: Default::default(),
            min_client_version: Default::default(),
            max_client_version: Default::default(),
            enable_dir_stats: false,
            enable_acl: false,
        }
    }
}

impl Format {
    /// check format can be update or not
    pub fn check_ugrade(&mut self, old: Format, force: bool) -> Result<()> {
        if force {
            warn!("Existing volume will be overwrited: {:?}", old);
        } else {
            if self.name != old.name {
                return UpgradeFormatSnafu {
                    detail: format!("name {} -> {}", old.name, self.name),
                }
                .fail();
            } else if self.block_size != old.block_size {
                return UpgradeFormatSnafu {
                    detail: format!("block size {} -> {}", old.block_size, self.block_size),
                }
                .fail();
            } else if self.compression != old.compression {
                return UpgradeFormatSnafu {
                    detail: format!(
                        "compression {:?} -> {:?}",
                        old.compression, self.compression
                    ),
                }
                .fail();
            } else if self.shards != old.shards {
                return UpgradeFormatSnafu {
                    detail: format!("shards {:?} -> {:?}", old.shards, self.shards),
                }
                .fail();
            } else if self.hash_prefix != old.hash_prefix {
                return UpgradeFormatSnafu {
                    detail: format!(
                        "hash prefix {:?} -> {:?}",
                        old.hash_prefix, self.hash_prefix
                    ),
                }
                .fail();
            } else if self.meta_version != old.meta_version {
                return UpgradeFormatSnafu {
                    detail: format!("meta version {} -> {}", old.meta_version, self.meta_version),
                }
                .fail();
            }
        }
        Ok(())
    }

    pub fn check_version(&self) -> Result<()> {
        if self.meta_version > MAX_VERSION {
            return NotIncompatibleClientSnafu {
                version: self.meta_version,
            }
            .fail();
        }

        // TODO: check client version
        Ok(())
    }
}
