use std::sync::Arc;

use chrono::Duration;
use juice_meta::config::{Config as MetaConfig, Format as MetaFormat};
use juice_storage::api::Config as ChunkConfig;
use nix::unistd::{getpid, getppid};

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

pub struct BackupMeta {
    pub interval: Duration,
    pub skip_trash: bool,
}


pub struct Config {
    // TODO: meta, format, chunk is redundant, we can remove them late, we only use part of it.
    pub meta: MetaConfig,
    pub format: MetaFormat,
    pub chunk: ChunkConfig,
    pub port: Option<Port>,
    pub version: String,
    pub attr_timeout: Option<Duration>,
    pub dir_entry_timeout: Option<Duration>,
    pub entry_timeout: Option<Duration>,
    pub backup_meta: Option<BackupMeta>,
    pub fast_resolve: Option<bool>,
    pub access_log: Option<String>,
    /// Whether to add prefix ".jfs" for internal files
    pub prefix_internal: bool,
    /// Whether hide internal files
    pub hide_internal: bool,
    pub root_squash: Option<Arc<RootSquash>>,
    pub non_default_permission: Option<bool>,
    
    pub pid: i32,
    pub ppid: i32,
    pub comm_path: Option<String>,
    pub state_path: Option<String>,
    pub fuse_opts: Option<Arc<FuseOptions>>,
}


