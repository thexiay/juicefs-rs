use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::IpAddr;
use std::ops::BitAnd;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bitflags::bitflags;
use juice_utils::process::Bar;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::base::{
    CommonMeta, DirStat, PendingFileScan, PendingSliceScan, TrashFileScan, TrashSliceScan,
};
use crate::config::{Config, Format};
use crate::error::{DriverSnafu, Result};
use crate::quota::Quota;
use crate::rds::RedisEngine;
use crate::utils::{FLockItem, PLockItem, PlockRecord};

pub const RESERVED_INODE: Ino = 0;
pub const ROOT_INODE: Ino = 1;
pub const TRASH_INODE: Ino = 0x7FFFFFFF10000000; // larger than vfs.minInternalNode
pub const TRASH_NAME: &str = ".Trash";
// ChunkSize is size of a chunk
pub const CHUNK_SIZE: u64 = 1 << 26; // ChunkSize is size of a chunk, default 64M
pub const MAX_VERSION: i32 = 1; // MaxVersion is the max of supported versions.
pub const MAX_FILE_NAME_LEN: usize = 255; // MaxNameLen is the max length of a name.

pub type Ino = u64;

pub trait InoExt {
    fn is_trash(&self) -> bool;
    fn is_root(&self) -> bool;
    fn transfer_root(&self, real_root: Ino) -> Ino;
}

impl InoExt for Ino {
    fn is_trash(&self) -> bool {
        *self >= TRASH_INODE
    }

    fn is_root(&self) -> bool {
        *self == ROOT_INODE
    }

    fn transfer_root(&self, real_root: Ino) -> Ino {
        match *self {
            0 => ROOT_INODE, // force using Root inode
            ROOT_INODE => real_root,
            _ => *self,
        }
    }
}

// TODO: encapsulated uid, gid
pub fn uid() -> u32 {
    0
}

pub fn gid() -> u32 {
    0
}

pub fn gids() -> Vec<u32> {
    vec![0]
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum INodeType {
    #[default]
    TypeFile = 1, // type for regular file
    TypeDirectory = 2, // type for directory
    TypeSymlink = 3,   // type for symlink
    TypeFIFO = 4,      // type for FIFO node
    TypeBlockDev = 5,  // type for block device
    TypeCharDev = 6,   // type for character device
    TypeSocket = 7,    // type for socket
}

// Entry is an entry inside a directory.
pub struct Entry {
    pub inode: Ino,
    pub name: String,
    pub attr: Attr,
}

// Slice is a slice of a chunk.
// Multiple slices could be combined together as a chunk.
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Slice {
    pub id: u64,
    pub size: u32,
    pub off: u32,
    pub len: u32,
}

pub struct Slices(pub Vec<Slice>);

#[derive(Serialize, Deserialize)]
// Summary represents the total number of files/directories and
// total length of all files inside a directory.
pub struct Summary {
    pub length: u64,
    pub size: u64,
    pub files: u64,
    pub dirs: u64,
}

#[derive(Serialize, Deserialize)]
pub struct TreeSummary {
    pub inode: Ino,
    pub path: String,
    pub r#type: u8,
    pub size: u64,
    pub files: u64,
    pub dirs: u64,
    pub children: Option<Vec<TreeSummary>>,
}

/// info about mount info. E.g, mount ip, mount host, mount time.
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    pub version: String,
    pub host_name: String,
    pub ip_addrs: Vec<IpAddr>,
    pub mount_point: String,
    pub mount_time: SystemTime,
    pub process_id: u32,
}

#[derive(Debug)]
pub struct Flock {
    pub inode: Ino,
    pub owner: u64,
    pub l_type: String,
}

#[derive(Debug)]
pub struct Plock {
    pub inode: Ino,
    pub owner: u64,
    pub records: Vec<PlockRecord>,
}

#[derive(Debug)]
pub struct Session {
    pub sid: u64,
    pub expire: Duration,
    pub session_info: Option<SessionInfo>,
    pub sustained: Option<Vec<Ino>>,
    pub flocks: Option<Vec<Flock>>,
    pub plocks: Option<Vec<Plock>>,
}

bitflags! {
    pub struct ModeMask: u8 {
        const READ    = 0b100;
        const WRITE   = 0b010;
        const EXECUTE = 0b001;
        const FULL    = Self::READ.bits | Self::WRITE.bits | Self::EXECUTE.bits;
        // TODO: 特殊权限，文件类型
    }

    #[derive(Default, Serialize, Deserialize)]
    pub struct Flag: u8 {
        const IMMUTABLE  = 0b001;
        const APPEND     = 0b010;
    }
}

// Attr represents attributes of a node.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Attr {
    pub flags: Flag,      // special flags
    pub typ: INodeType,   // type of a node
    pub mode: u16,        // permission mode
    pub uid: u32,         // owner id
    pub gid: u32,         // group id of owner
    pub rdev: u32,        // device number
    pub atime: u128,      // last access time, nacos time
    pub mtime: u128,      // last modified time, nacos time
    pub ctime: u128,      // last change time for meta, nacos time
    pub nlink: u32,       // number of links (sub-directories or hardlinks)
    pub length: u64,      // length of regular file
    pub parent: Ino,      // inode of parent; 0 means tracked by parentKey (for hardlinks)
    pub full: bool,       // the attributes are completed or not
    pub keep_cache: bool, // whether to keep the cached page or not
    pub access_acl: u32,  // access ACL id (identical ACL rules share the same access ACL ID.)
    pub default_acl: u32, // default ACL id (default ACL and the access ACL share the same cache and store)
}

// Meta is a interface for a meta service for file system.
#[async_trait]
pub trait Meta: Send + Sync + 'static {
    // Name of database
    fn name(&self) -> String;

    // Get a copy of the current format
    fn get_format(&self) -> Arc<Format>;

    /// Init init a formatted volumn for meta service.
    /// This is used for juice filesystem initialization and for a totally empty meta.
    ///
    /// # Example
    ///
    /// ```
    /// use juice_meta::api::Meta;
    /// use juice_meta::api::new_client;
    /// use juice_meta::config::Config;
    ///
    /// let meta = new_client("redis://localhost:6379".to_string(), Config::default()).unwrap();
    /// let format = Format::default();
    /// meta.init(format, false).await;
    /// ```
    async fn init(&self, format: Format, force: bool) -> Result<()>;

    /// Load loads the existing setting of a formatted volume from meta service.
    /// This must call after this meta is initialized.E.g `juicefs format`
    async fn load(&self, check_version: bool) -> Result<Arc<Format>>;

    // Shutdown close current database connections.
    async fn shutdown(&self) -> Result<()>;

    // Reset cleans up all metadata, VERY DANGEROUS!
    async fn reset(&self) -> Result<()>;

    /// ------------------------------- Session func ----------------------------------------

    /// Mount func.NewSession creates or update client session.
    ///
    /// New session will start serveal different coroutine to do background job, those backgroud job no need to consider
    /// be closed, because once session close, this process exit.
    async fn new_session(self: Arc<Self>, persist: bool) -> Result<()>;

    // UnMount func.CloseSession does cleanup and close the session.
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

    // CleanStaleSessions cleans up sessions which is expired.
    async fn cleanup_stale_sessions(&self);

    // CleanupDetachedNodesBefore deletes all detached nodes before the given time.
    async fn cleanup_detached_nodes_before(
        &self,
        edge: SystemTime,
        incre_process: Box<dyn Fn(isize) + Send>,
    );

    // GetPaths returns all paths of an inode
    async fn get_paths(&self, inode: Ino) -> Vec<String>;

    // Check integrity of an absolute path and repair it if asked
    async fn check(
        &self,
        fpath: String,
        repair: bool,
        recursive: bool,
        stat_all: bool,
    ) -> Result<()>;

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

    // Access checks the access permission on given inode.
    async fn access(&self, inode: Ino, mode_mask: ModeMask, attr: &Attr) -> Result<()>;

    // Lookup returns the inode and attributes for the given entry in a directory.
    async fn lookup(
        &self,
        parent: Ino,
        name: &str,
        check_permission: bool,
    ) -> Result<(Ino, Attr)>;

    // Resolve fetches the inode and attributes for an entry identified by the given path.
    // ENOTSUP will be returned if there's no natural implementation for this operation or
    // if there are any symlink following involved.
    // The different between with lookup and resolve is that resolve deep search in path, 
    // but lookup only search in current directory.
    async fn resolve(&self, parent: Ino, path: String) -> Result<(Ino, Attr)>;

    // GetAttr returns the attributes for given node.
    async fn get_attr(&self, inode: Ino) -> Result<Attr>;

    // SetAttr updates the attributes for given node.
    async fn set_attr(&self, inode: Ino, set: u16, sggid_clear_mode: u8, attr: &Attr)
        -> Result<()>;

    // Check setting attr is allowed or not
    async fn check_set_attr(&self, inode: Ino, set: u16, attr: &Attr) -> Result<()>;

    // Truncate changes the length for given file.
    async fn truncate(
        self: Arc<Self>,
        inode: Ino,
        flags: u8,
        attr_length: u64,
        skip_perm_check: bool,
    ) -> Result<Attr>;

    // Fallocate preallocate given space for given file.
    async fn fallocate(
        &self,
        inode: Ino,
        mode: u8,
        off: u64,
        size: u64,
        length: &u64,
    ) -> Result<()>;

    // ReadLink returns the target of a symlink.
    async fn read_link(&self, inode: Ino, path: &Vec<u8>) -> Result<()>;

    // Symlink creates a symlink in a directory with given name.
    async fn symlink(
        &self,
        parent: Ino,
        name: String,
        path: String,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()>;

    // Mknod creates a node in a directory with given name, type and permissions.
    async fn mknod(
        &self,
        parent: Ino,
        name: &str,
        r#type: INodeType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: &str,
    ) -> Result<(Ino, Attr)>;

    // Mkdir creates a sub-directory with given name and mode.
    async fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        copysgid: u8,
    ) -> Result<(Ino, Attr)>;

    // Unlink removes a file entry from a directory.
    // The file will be deleted if it's not linked by any entries and not open by any sessions.
    async fn unlink(self: Arc<Self>, parent: Ino, name: &str, skip_check_trash: bool)
        -> Result<()>;

    // Rmdir removes an empty sub-directory.
    async fn rmdir(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Ino>;

    // Rename move an entry from a source directory to another with given name.
    // The targeted entry will be overwrited if it's a file or empty directory.
    // For Hadoop, the target should not be overwritten.
    async fn rename(
        &self,
        parent_src: Ino,
        name_src: String,
        parent_dst: Ino,
        name_dst: String,
        flags: u32,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()>;

    // Link creates an entry for node.
    // inode_src: source inode
    // parent + name: dst inode, parent is dst inode's parent inode, name is the link name
    async fn link(&self, inode_src: Ino, parent: Ino, name: String, attr: &Attr) -> Result<()>;

    // Readdir returns all entries for given directory, which include attributes if plus is true.
    async fn readdir(&self, inode: Ino, wantattr: u8, entries: &mut Vec<Entry>) -> Result<()>;

    // Create creates a file in a directory with given name.
    async fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: i32,
    ) -> Result<(Ino, Attr)>;

    // Open checks permission on a node and track it as open.
    async fn open(&self, inode: Ino, flags: i32, attr: &mut Attr) -> Result<()>;

    // Close a file.
    async fn close(&self, inode: Ino) -> Result<()>;

    // Read returns the list of slices on the given chunk.
    async fn read(&self, inode: Ino, indx: u32, slices: &Vec<Slice>) -> Result<()>;

    // NewSlice returns an id for new slice.
    async fn new_slice(&self) -> Result<u64>;

    // Write put a slice of data on top of the given chunk.
    async fn write(
        &self,
        inode: Ino,
        indx: u32,
        off: u32,
        slice: Slice,
        mtime: SystemTime,
    ) -> Result<()>;

    // InvalidateChunkCache invalidate chunk cache
    async fn invalidate_chunk_cache(&self, inode: Ino, indx: u32) -> Result<()>;

    // CopyFileRange copies part of a file to another one.
    async fn copy_file_range(
        &self,
        fin: Ino,
        off_in: u64,
        fout: Ino,
        off_out: u64,
        size: u64,
        flags: u32,
        copied: &u64,
        out_length: &u64,
    ) -> Result<()>;

    // GetParents returns a map of node parents (> 1 parents if hardlinked)
    async fn get_parents(&self, inode: Ino) -> HashMap<Ino, isize>;

    // GetDirStat returns the space and inodes usage of a directory.
    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat>;

    // GetXattr returns the value of extended attribute for given name.
    async fn get_xattr(&self, inode: Ino, name: String, v_buff: &Vec<u8>) -> Result<()>;

    // ListXattr returns all extended attributes of a node.
    async fn list_xattr(&self, inode: Ino, dbuff: &Vec<u8>) -> Result<()>;

    // SetXattr update the extended attribute of a node.
    async fn set_xattr(&self, inode: Ino, name: String, value: &Vec<u8>, flags: u32) -> Result<()>;

    // RemoveXattr removes the extended attribute of a node.
    async fn remove_xattr(&self, inode: Ino, name: String) -> Result<()>;

    // Flock tries to put a lock on given file.
    async fn flock(&self, inode: Ino, owner: u64, ltype: u32, block: bool) -> Result<()>;

    // Getlk returns the current lock owner for a range on a file.
    async fn getlk(
        &self,
        inode: Ino,
        owner: u64,
        ltype: &u32,
        start: &u64,
        end: &u64,
        pid: &u32,
    ) -> Result<()>;

    // Setlk sets a file range lock on given file.
    async fn setlk(
        &self,
        inode: Ino,
        owner: u64,
        block: bool,
        ltype: u32,
        start: u64,
        end: u64,
        pid: u32,
    ) -> Result<()>;

    // Compact all the chunks by merge small slices together
    async fn compact_all(&self, threads: isize, bar: Bar) -> Result<()>;

    // Compact chunks for specified path
    async fn compact(
        &self,
        inode: Ino,
        concurrency: isize,
        pre_func: Box<dyn Fn() + Send>,
        post_func: Box<dyn Fn() + Send>,
    ) -> Result<()>;

    // ListSlices returns all slices used by all files.
    async fn list_slices(
        &self,
        delete: bool,
        show_progress: Box<dyn Fn() + Send>,
    ) -> Result<HashMap<Ino, Vec<Slice>>>;

    // Remove all files and directories recursively.
    // count represents the number of attempted deletions of entries (even if failed).
    async fn remove(&self, parent: Ino, name: &str, count: &mut u64) -> Result<()>;

    // Get summary of a node; for a directory it will accumulate all its child nodes
    async fn get_summary(
        &self,
        inode: Ino,
        summary: &Summary,
        recursive: bool,
        strict: bool,
    ) -> Result<()>;

    // GetTreeSummary returns a summary in tree structure
    async fn get_tree_summary(
        &self,
        root: &TreeSummary,
        depth: u8,
        topn: u8,
        strict: bool,
        update_progress: Box<dyn Fn(u64, u64) + Send>,
    ) -> Result<()>;

    // Clone a file or directory
    async fn clone(
        &self,
        src_ino: Ino,
        dst_parent_ino: Ino,
        dst_name: String,
        cmode: u8,
        cumask: u16,
        count: &u64,
        total: &u64,
    ) -> Result<()>;

    // Change root to a directory specified by subdir
    async fn chroot(&self, subdir: String) -> Result<()>;

    // just for test
    fn get_base(&self) -> &CommonMeta;
}

// NewClient creates a Meta client for given uri.
pub async fn new_client(uri: String, conf: Config) -> Box<Arc<dyn Meta>> {
    let uri = if !uri.contains("://") {
        format!("redis://{}", uri)
    } else {
        uri
    };
    let (driver, addr) = match uri.find("://") {
        Some(p) => (&uri[..p], &uri[p + 3..]),
        None => panic!("invalid uri {}", uri),
    };
    let res: Result<Box<Arc<dyn Meta>>> = match driver {
        "redis" => RedisEngine::new(driver, addr, conf).await,
        _ => DriverSnafu {
            driver: driver.to_string(),
        }
        .fail(),
    };
    res.expect(&format!("Meta {uri} is not available."))
}
