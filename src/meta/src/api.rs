use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::IpAddr;
use std::ops::AddAssign;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bitflags::bitflags;
use chrono::{DateTime, Utc};
use dyn_clone::clone_trait_object;
use juice_utils::process::Bar;
use serde::{Deserialize, Serialize};

use crate::acl::AclId;
use crate::base::{
    CommonMeta, DirStat, PendingFileScan, PendingSliceScan, TrashFileScan, TrashSliceScan,
};
use crate::config::{Config, Format};
use crate::context::{Gid, Uid, WithContext};
use crate::error::Result;
use crate::quota::QuotaView;
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
    fn is_root_trash(&self) -> bool;
    fn is_sub_trash(&self) -> bool;
    fn is_root(&self) -> bool;
}

impl InoExt for Ino {
    fn is_trash(&self) -> bool {
        *self >= TRASH_INODE
    }

    fn is_root_trash(&self) -> bool {
        *self == TRASH_INODE
    }

    fn is_sub_trash(&self) -> bool {
        *self > TRASH_INODE
    }

    fn is_root(&self) -> bool {
        *self == ROOT_INODE
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum INodeType {
    #[default]
    File = 1, // type for regular file
    Directory = 2, // type for directory
    Symlink = 3,   // type for symlink
    FIFO = 4,      // type for FIFO node
    BlockDev = 5,  // type for block device
    CharDev = 6,   // type for character device
    Socket = 7,    // type for socket
}

#[derive(Debug, PartialEq, Eq)]
pub enum QuotaOp {
    Set(QuotaView),
    Get,
    Del,
    List,
    Check,
}

// Entry is an entry inside a directory.
#[derive(Debug)]
pub struct Entry {
    pub inode: Ino,
    pub name: String,
    pub attr: Attr,
}

/*
   |-----------------------chunk---------------------|
   |------|---------|--------------|------|----------|
   |	  ↑                               ↑          |
   |     coff(in `PSlice`)               cend        |
   |                ↑              ↑                 |
   |	           off            end                |
   |                |<----len----->|                 |
   |      |<---------------size---------->|          |
   |-------------------------------------------------|
*/
/// Slice is a continuous write in a chunk
/// Multiple slices could be combined together as a chunk.
/// One slice can only in one chunk.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Slice {
    // slice id
    pub id: u64,
    // slice total length, do not know the start position in chunk
    pub size: u32,
    // valid data offset in slice(not in chunk)
    pub off: u32,
    // valid data length
    // actually not all data in slice is valid, only `slice_array[off:off+len]` is valid data
    pub len: u32,
}

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
// Summary represents the total number of files/directories and
// total length of all files inside a directory.
pub struct Summary {
    pub length: u64,
    pub size: u64,
    pub files: u64,
    pub dirs: u64,
}

impl AddAssign<Summary> for Summary {
    fn add_assign(&mut self, other: Self) {
        self.length += other.length;
        self.size += other.size;
        self.files += other.files;
        self.dirs += other.dirs;
    }
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

    pub struct SetAttrMask: u16 {
        const SET_MODE = 1 << 0;
        const SET_UID  = 1 << 2;
        const SET_GID  = 1 << 3;
        const SET_SIZE = 1 << 4;
        const SET_ATIME = 1 << 5;
        const SET_MTIME = 1 << 6;
        const SET_CTIME = 1 << 7;
        const SET_ATIME_NOW = 1 << 8;
        const SET_MTIME_NOW = 1 << 9;
        const SET_FLAG = 1 << 15;
    }

    pub struct RenameMask: u32 {
        /// NOREPLCAE: not allow to overwride the destination entry if the destination exists
        const NOREPLACE = 1 << 0;
        /// EXCHANGE: will exchange the two entry content if the destination exists,
        /// but name retains the same, will not exchange
        const EXCHANGE = 1 << 1;
        const WHITEOUT = 1 << 2;
        /// RESTORE: TODO: ????
        const RESTORE = 1 << 5;
    }

    pub struct OFlag: u32 {
        const O_RDONLY = 0 << 0;
        const O_WRONLY = 1 << 0;
        const O_RDWR = 1 << 1;
        const O_EXCL = 1 << 7;
        const O_TRUNC = 1 << 9;
        const O_APPEND = 1 << 10;
    }

    pub struct Falloc: u8 {
        const KEEP_SIZE = 1 << 0;
        const PUNCH_HOLE = 1 << 1;
        // const NO_HODE_STALE = 1 << 2;  RESERVED
        const COLLAPES_RANGE = 1 << 3;
        const ZERO_RANGE = 1 << 4;
        const INSERT_RANGE = 1 << 5;
    }

    pub struct XattrF: u32 {
        const CREATE_OR_REPLACE = 0;
        const CREATE = 1 << 0;
        const REPLACE = 1 << 1;
    }
}

// Attr represents attributes of a node.
#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attr {
    pub flags: Flag,        // special flags
    pub typ: INodeType,     // type of a node
    pub mode: u16,          // permission mode
    pub uid: Uid,           // owner id
    pub gid: Gid,           // group id of owner
    pub rdev: u32,          // device number
    pub atime: u128,        // last access time, nacos time
    pub mtime: u128,        // last modified time, nacos time
    pub ctime: u128,        // last change time for meta, nacos time
    pub nlink: u32, // For symbol file, number of hardlinks; For directory, number of sub-directories
    pub length: u64, // length of regular file
    pub parent: Ino, // inode of parent; 0 means tracked by parentKey (for hardlinks)
    pub full: bool, // the attributes are completed or not
    pub keep_cache: bool, // whether to keep the cached page or not
    pub access_acl: AclId, // access ACL id (identical ACL rules share the same access ACL ID.)
    pub default_acl: AclId, // default ACL id (default ACL and the access ACL share the same cache and store)
}

// Meta is a interface for a meta service for file system.
#[async_trait]
pub trait Meta: WithContext + Send + Sync + 'static {
    // Name of database
    fn name(&self) -> String;

    // Get a copy of the current format
    fn get_format(&self) -> Arc<Format>;

    /// ------------------------------- Lifetime func ----------------------------------------
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
    async fn new_session(&self, persist: bool) -> Result<()>;

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

    /// GetPaths returns all paths of an inode.
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

    /// Handle quota operation
    ///
    /// # Arguments
    ///
    /// * `op` - operation type
    /// * `dpath` - directory path relative to the current root path
    /// * `strict` - if true, it will check the quota of the parent directory
    /// * `repair` - if true, it will repair the quota if it's not correct
    async fn handle_quota(
        &self,
        op: QuotaOp,
        dpath: &str,
        strict: bool,
        repair: bool,
    ) -> Result<HashMap<String, QuotaView>>;

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
    /// StatFS returns summary statistics of a volume.
    ///
    /// # Returns
    ///
    /// * `(u64, u64, u64, u64)` - total space, available space, used inodes, available inodes
    ///    total space is the total size of the inode.
    async fn stat_fs(&self, inode: Ino) -> Result<(u64, u64, u64, u64)>;

    // Access checks the current user can access (mode)permission on given inode.
    async fn access(&self, inode: Ino, mode_mask: ModeMask, attr: &Attr) -> Result<()>;

    // Lookup returns the inode and attributes for the given entry in a directory.
    async fn lookup(&self, parent: Ino, name: &str, check_permission: bool) -> Result<(Ino, Attr)>;

    /// Resolve fetches the inode and attributes for an entry identified by the given path
    /// (The last file of the path).
    /// The different between with lookup and resolve is that resolve deep search in path,
    /// but lookup only search in current directory.
    ///
    /// # Arguments
    ///
    /// * `parent` - parent inode
    /// * `path` - path of the multiple level entry
    /// * `fallback` - if true, it will use lookup to find the entry
    ///
    /// # Errors
    ///
    /// * `ENOTSUP` - will be returned if there's no natural implementation for this operation or
    /// if there are any symlink following involved.
    async fn resolve(&self, parent: Ino, path: &str, fallback: bool) -> Result<(Ino, Attr)>;

    /// GetAttr returns the attributes for given node.
    ///
    /// For root inode or trash inode, default attributes will be returned if request timieout
    async fn get_attr(&self, inode: Ino) -> Result<Attr>;

    // SetAttr updates the attributes for given node.
    async fn set_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        sggid_clear_mode: u8,
        attr: &Attr,
    ) -> Result<()>;

    // Check setting attr is allowed or not
    async fn check_set_attr(&self, inode: Ino, set: u16, attr: &Attr) -> Result<()>;

    // Truncate changes the length for given file.
    async fn truncate(
        &self,
        inode: Ino,
        flags: u8,
        attr_length: u64,
        skip_perm_check: bool,
    ) -> Result<Attr>;

    /// Fallocate preallocate given space for given file.
    /// Returns the length of the file after fallocate.
    async fn fallocate(&self, inode: Ino, flag: Falloc, off: u64, size: u64) -> Result<u64>;

    // ReadLink returns the target of a symlink.
    ///
    /// # Arguments
    ///
    /// * `inode` - the symbol inode
    async fn read_symlink(&self, inode: Ino) -> Result<String>;

    /// Symlink creates a symlink in a directory with given name.
    ///
    /// # Arguments
    ///
    /// * `parent`: parent inode
    /// * `name`: symlink name
    /// * `target_path`: target path of symlink
    async fn symlink(&self, parent: Ino, name: &str, target_path: &str) -> Result<(Ino, Attr)>;

    /// Mknod creates a node in a directory with given name, type and permissions.
    ///
    /// [`exists_node`]: represet the node is already exists or not, if exists, the node will return exists node.
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

    /// Unlink removes a file [entry] (not dirctory) from a directory.
    /// The file will be deleted if it's not linked by any entries and not open by any sessions.
    ///
    /// # Arguments
    ///
    /// * `parent`: parent inode
    /// * `name`: entry name
    /// * `skip_check_trash`: skip check trash or not
    ///
    /// # Erorr
    ///
    /// * `EPERM` - if current user has no permission
    ///
    /// [entry]: Entry
    async fn unlink(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<()>;

    /// Rmdir removes an empty sub-directory.
    ///
    /// This will return ENOTEMPTY if the directory is not empty.
    async fn rmdir(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Ino>;

    /// Rename move an entry from a source directory to another with given name.
    /// The targeted entry will be overwrited if it's a file or empty directory.
    /// For Hadoop, the target should not be overwritten.
    ///
    /// # Error:
    ///
    /// * `EPERM`: name_src or name_src is trash
    /// * `EPERM`: parent_src is trash
    /// * `EPERM`: non root user want to move to trash dir
    ///
    /// # Arguments:
    ///
    /// * `parent_src`: source dir inode
    /// * `name_src`: source entry name
    /// * `parent_dst`: destination dir inode
    /// * `name_dst`: destination entry name
    ///
    /// # Return
    ///
    /// if renamed happen(if src equals dst, it would't happen), return the inode and attributes of the renamed entry.
    async fn rename(
        &self,
        parent_src: Ino,
        name_src: &str,
        parent_dst: Ino,
        name_dst: &str,
        flags: RenameMask,
    ) -> Result<Option<(Ino, Attr)>>;

    /// Link create an hardlink (not dirctory) from a directory.
    ///
    /// # Arguments
    ///
    /// * `inode`: source inode to be hard link
    /// * `parent`: destination inode
    /// * `name`: hard link name
    ///
    /// # Return
    ///
    /// return the inode and attributes of the new hard link entry.
    async fn link(&self, inode: Ino, parent: Ino, name: &str) -> Result<Attr>;

    /// Readdir returns all entries for given directory, which include attributes if wantattr is true.
    /// This func will return "." and ".." entry as well.
    async fn readdir(&self, inode: Ino, wantattr: bool) -> Result<Vec<Entry>>;

    // Create creates a file in a directory with given name.
    async fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: OFlag,
    ) -> Result<(Ino, Attr)>;

    /// Open checks permission on a node and track it as open.
    ///
    /// # Arguments
    ///
    /// * `inode`: inode of the node to be opened
    /// * `flags`: open flags
    async fn open(&self, inode: Ino, flags: OFlag) -> Result<Attr>;

    // Close a file.
    async fn close(&self, inode: Ino) -> Result<()>;

    // Read returns the list of slices on the given chunk.
    // All returned slices valid range continuously fills the entire chunk range
    async fn read(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>>;

    // NewSlice returns an id for new slice.
    async fn new_slice(&self) -> Result<u64>;

    /// Write put a slice of data on top of the given chunk.
    ///
    /// # Arguments
    ///
    /// * `inode` - inode of the file
    /// * `indx` - index of the chunk
    /// * `off` - offset of the chunk
    /// * `slice` - slice description
    /// * `mtime` - modified time
    async fn write(
        &self,
        inode: Ino,
        indx: u32,
        off: u32,
        slice: Slice,
        mtime: DateTime<Utc>,
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

    // GetDirStat returns the space and inodes usage of a directory.
    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat>;

    // GetXattr returns the value of extended attribute for given name.
    async fn get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>>;

    // ListXattr returns all extended attributes of a node.
    async fn list_xattr(&self, inode: Ino) -> Result<Vec<u8>>;

    // SetXattr update the extended attribute of a node.
    async fn set_xattr(&self, inode: Ino, name: &str, value: Vec<u8>, flag: XattrF) -> Result<()>;

    // RemoveXattr removes the extended attribute of a node.
    async fn remove_xattr(&self, inode: Ino, name: &str) -> Result<()>;

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

    /// Remove all files and directories recursively.
    ///
    /// Return count, represents the number of attempted deletions of entries (maybe delete failed)
    async fn remove(&self, parent: Ino, name: &str) -> Result<u64>;

    // Get summary of a node; for a directory it will accumulate all its child nodes
    async fn get_summary(&self, ino: Ino, recursive: bool, strict: bool) -> Result<Summary>;

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
    async fn clone_ino(
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
    async fn chroot(&self, subdir: &str) -> Result<Ino>;

    // GetParents returns a map of node parents (> 1 parents if hardlinked)
    async fn get_parents(&self, inode: Ino) -> HashMap<Ino, u32>;
    // ---------------------------------------- sys call -----------------------------------------------------------

    // just for test
    fn get_base(&self) -> &CommonMeta;
}
clone_trait_object!(Meta);

// NewClient creates a Meta client for given uri.
pub async fn new_client(uri: String, conf: Config) -> Box<dyn Meta> {
    let uri = if !uri.contains("://") {
        format!("redis://{}", uri)
    } else {
        uri
    };
    let (driver, addr) = match uri.find("://") {
        Some(p) => (&uri[..p], &uri[p + 3..]),
        None => panic!("invalid uri {}", uri),
    };
    match driver {
        "redis" => Box::new(
            RedisEngine::new(driver, addr, conf)
                .await
                .expect(&format!("Meta {uri} is not available")),
        ),
        _ => panic!("unknown driver {driver}"),
    }
}
