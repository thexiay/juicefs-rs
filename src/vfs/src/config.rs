use std::sync::Arc;

use chrono::Duration;
use juice_meta::config::{Config as MetaConfig, Format as MetaFormat};
use juice_storage::api::Config as ChunkConfig;

pub struct Port {
    pub prometheus_agent: String,
    pub debug_agent: String,
    pub consul_addr: String,
    pub pyroscope_addr: String,
}

pub struct RootSquash {
    pub uid: u32,
    pub gid: u32,
}

/// FuseOptions contains options for fuse mount, keep the same structure with `fuse.MountOptions`
pub struct FuseOptions {
    pub allow_other: bool,
    pub options: Vec<String>,
    pub max_background: i32,
    pub max_write: i32,
    pub max_read_ahead: i32,
    pub max_pages: i32,
    pub ignore_security_labels: bool,
    pub remember_inodes: bool,
    pub fs_name: String,
    pub name: String,
    pub single_threaded: bool,
    pub disable_xattrs: bool,
    pub debug: bool,
    pub enable_locks: bool,
    pub explicit_data_cache_control: bool,
    pub direct_mount: bool,
    pub direct_mount_flags: u32,
    pub enable_acl: bool,
    pub enable_writeback: bool,
    pub enable_ioctl: bool,
    pub dont_umask: bool,
    pub other_caps: u32,
    pub no_alloc_for_read: bool,
}

pub struct Config {
    pub meta: Arc<MetaConfig>,
    pub format: MetaFormat,
    pub chunk: Arc<ChunkConfig>,
    pub port: Port,
    pub version: String ,
    pub attr_timeout: Duration,
    pub dir_entry_timeout: Duration,
    pub entry_timeout: Duration,
    pub backup_meta: Duration,
    pub backup_skip_trash: bool,
    pub fast_resolve: bool,
    pub access_log: String,
    pub prefix_internal: bool,
    pub hide_internal: bool,
    pub root_squash: Arc<RootSquash>,
    pub non_default_permission: bool,
    
    pub pid: i32,
    pub ppid: i32,
    pub comm_path: String,
    pub state_path: String,
    pub fuse_opts: Arc<FuseOptions>,
}