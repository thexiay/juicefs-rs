use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::IpAddr;
use std::sync::atomic::AtomicU64;
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::base::{CommonMeta, PendingFileScan, PendingSliceScan, TrashFileScan, TrashSliceScan};
use crate::config::{Config, Format};
use crate::error::Result;
use crate::quota::Quota;
use crate::rds::RedisEngine;
use crate::utils::{FLockItem, PLockItem, PlockRecord};

pub const ROOT_INODE: Ino = 1;
pub const TRASH_INODE: Ino = 0x7FFFFFFF10000000; // larger than vfs.minInternalNode
	// ChunkSize is size of a chunk
pub const CHUNK_SIZE: u64 = 1 << 26; // 64M

pub type Ino = u64;

#[derive(Default, Serialize, Deserialize)]
pub enum INodeType {
    #[default]
    TypeFile      = 1,  // type for regular file
	TypeDirectory = 2,  // type for directory
	TypeSymlink   = 3,  // type for symlink
	TypeFIFO      = 4,  // type for FIFO node
	TypeBlockDev  = 5,  // type for block device
	TypeCharDev   = 6,  // type for character device
	TypeSocket    = 7,  // type for socket
}

// Slice is a slice of a chunk.
// Multiple slices could be combined together as a chunk.
#[derive(Default, Serialize, Deserialize)]
pub struct Slice {
    pub id: u64,
    pub size: u32,
    pub off: u32,
    pub len: u32,
}

pub struct Slices(pub Vec<Slice>);

#[derive(Serialize, Deserialize)]
pub struct SessionInfo {
    pub version: String,
    pub host_name: String,
    pub ip_addrs: Vec<IpAddr>,
    pub mount_point: String,
    pub mount_time: SystemTime,
    pub process_id: u32,
}

pub struct Flock {
    pub inode: Ino,
    pub owner: u64,
    pub l_type: String,
}

pub struct Plock {
    pub inode: Ino,
    pub owner: u64,
    pub records: Vec<PlockRecord>,
}

pub struct Session {
    pub sid: u64,
    pub expire: SystemTime,
    pub session_info: SessionInfo,
    pub sustained: Option<Vec<Ino>>,
    pub flocks: Option<Vec<Flock>>,
    pub plocks: Option<Vec<Plock>>,
}

// Attr represents attributes of a node.
#[derive(Default, Serialize, Deserialize)]
pub struct Attr {
    pub flags: u8,         // flags
    pub typ: INodeType,    // type of a node
    pub mode: u16,         // permission mode
    pub uid: u32,          // owner id
    pub gid: u32,          // group id of owner
    pub rdev: u32,         // device number
    pub atime: u64,        // last access time
    pub mtime: u64,        // last modified time
    pub ctime: u64,        // last change time for meta
    pub atime_nsec: u32,   // nanosecond part of atime
    pub mtime_nsec: u32,   // nanosecond part of mtime
    pub ctime_nsec: u32,   // nanosecond part of ctime
    pub nlink: u32,        // number of links (sub-directories or hardlinks)
    pub length: u64,       // length of regular file
    pub parent: Ino,       // inode of parent; 0 means tracked by parentKey (for hardlinks)
    pub full: bool,        // the attributes are completed or not
    pub keep_cache: bool,  // whether to keep the cached page or not
    pub access_acl: u32,   // access ACL id (identical ACL rules share the same access ACL ID.)
    pub default_acl: u32,  // default ACL id (default ACL and the access ACL share the same cache and store)
}

// Meta is a interface for a meta service for file system.
#[async_trait]
pub trait Meta: Send + Sync + 'static {
    // Name of database
    fn name(&self) -> String;

    // Init is used to initialize a meta service.
    async fn init(&self, format: &Format, force: bool) -> Result<()>;

    // Shutdown close current database connections.
    async fn shutdown(&self) -> Result<()>;

    // Reset cleans up all metadata, VERY DANGEROUS!
    async fn reset(&self) -> Result<()>;

    // Load loads the existing setting of a formatted volume from meta service.
    async fn load(&self, check_version: bool) -> Result<Format>;

    // NewSession creates or update client session.
    async fn new_session(&self, record: bool) -> Result<()>;

    // CloseSession does cleanup and close the session.
    async fn close_session(&self) -> Result<()>;

    // GetSession retrieves information of session with sid
    async fn get_session(&self, sid: u64, detail: bool) -> Result<Session>;

    // ListSessions returns all client sessions.
    async fn list_sessions(&self) -> Result<Vec<Session>>;

    // ScanDeletedObject scan deleted objects by customized scanner.
    async fn scan_deleted_object(
        &self,
        trash_slice_scan: TrashSliceScan,
        pending_slice_scan: PendingSliceScan,
        trash_file_scan: TrashFileScan,
        pending_file_scan: PendingFileScan,
    ) -> Result<()>;

    // ListLocks returns all locks of a inode.
    async fn list_locks(&self, inode: Ino) -> Result<(Vec<PLockItem>, Vec<FLockItem>)>;

    // CleanStaleSessions cleans up sessions not active for more than 5 minutes
    async fn clean_stale_sessions(&self, edge: SystemTime, incre_process: Box<dyn Fn(isize) + Send>);

    // CleanupDetachedNodesBefore deletes all detached nodes before the given time.
    async fn cleanup_detached_nodes_before(&self, edge: SystemTime, incre_process: Box<dyn Fn(isize) + Send>);

    // GetPaths returns all paths of an inode
    async fn get_paths(&self, inode: Ino) -> Vec<String>;

    // Check integrity of an absolute path and repair it if asked
    async fn check(&self, fpath: String, repair: bool, recursive: bool, stat_all: bool) -> Result<()>;

    // Get a copy of the current format
    async fn get_format(&self) -> Format;

    // OnMsg add a callback for the given message type.
    /*async fn on_msg(mtype: u32, cb: MsgCallback);*/
    
    // OnReload register a callback for any change founded after reloaded.
    async fn on_reload(&self, cb: Box<dyn Fn(Format) + Send>);

    async fn handle_quota(
        &self, 
        cmd: u8,
        dpath: String,
        quotas: HashMap<String, Quota>,
        strict: bool,
        repair: bool,
    ) -> Result<()>;

    // Dump the tree under root, which may be modified by checkRoot
    async fn dump_meta(
        &self, 
        w: Box<dyn Write + Send>,
        root: Ino,
        threads: isize,
        keep_secret: bool,
        fast: bool,
        skip_trash: bool,
    ) -> Result<()>;
    async fn load_meta(&self, r: Box<dyn Read + Send>) -> Result<()>;
    // ---------------------------------------- sys call -----------------------------------------------------------
    // StatFS returns summary statistics of a volume.
    async fn stat_fs(
        &self,
        inode: Ino,
        totalspace: AtomicU64,
        availspace: AtomicU64,
        iused: AtomicU64,
        iavail: AtomicU64,
    ) -> Result<()>;
}

// NewClient creates a Meta client for given uri.
pub fn new_client(uri: String, conf: Config) -> Option<Box<dyn Meta>> {
    let uri = if !uri.contains("://") {
        format!("redis://{}", uri)
    } else {
        uri
    };
    let driver = match uri.find("://") {
        Some(p) => &uri[..p],
        None => return None,
    };
    match driver {
        "redis" => {
            Some(Box::new(CommonMeta::new(RedisEngine::new())))
        },
        _ => None,
    }
}
