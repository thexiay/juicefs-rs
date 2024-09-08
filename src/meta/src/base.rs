use async_trait::async_trait;
use bytes::Bytes;
use juice_utils::process::Bar;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::{Write, Read};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::SystemTime;
use sysinfo::{get_current_pid, PidExt};
use tracing::warn;

use crate::acl::Rule;
use crate::api::{Attr, Entry, Ino, Meta, Session, SessionInfo, Slice, Summary, TreeSummary};
use crate::config::{Config, Format};
use crate::error::Result;
use crate::openfile::OpenFile;
use crate::quota::Quota;
use crate::utils::{FLockItem, FreeID, PLockItem};

pub const INODE_BATCH: isize = 1 << 10;
pub const SLICE_ID_BATCH: isize = 1 << 10;
pub const N_LOCK: usize = 1 << 10;
// (ss, ts) -> clean
pub type TrashSliceScan = Box<dyn Fn(Vec<Slice>, i64) -> Result<bool> + Send>;
// (id, size) -> clean
pub type PendingSliceScan = Box<dyn Fn(u64, u32) -> Result<bool> + Send>;
// (ino, size, ts) -> clean
pub type TrashFileScan = Box<dyn Fn(u64, u64, SystemTime) -> Result<bool> + Send>;
// (ino, size, ts) -> clean
pub type PendingFileScan = Box<dyn Fn(u64, u64, i64) -> Result<bool> + Send>;

pub struct Cchunk {
    inode: Ino,
    indx: u32,
    slices: isize,
}

impl Cchunk {
    pub fn new(inode: Ino, indx: u32, slices: isize) -> Self {
        Self {
            inode,
            indx,
            slices,
        }
    }
}

// fsStat aligned for atomic operations
// nolint:structcheck
pub struct FsStat {
    new_space: AtomicI64,
    new_inodes: AtomicI64,
    used_space: AtomicI64,
    used_inodes: AtomicI64,
}

impl FsStat {
    pub fn update_stats(&self, space: i64, inodes: i64) {
        self.new_space.fetch_add(space, Ordering::SeqCst);
        self.used_inodes.fetch_add(inodes, Ordering::SeqCst);
    }
}

pub struct DirStat {
    length: i64,
    space: i64,
    inodes: i64,
}

#[async_trait]
pub trait Engine {
    // Get the value of counter name.
    async fn get_counter(&self, name: &str) -> Result<i64>;
    // Increase counter name by value. Do not use this if value is 0, use getCounter instead.
    async fn incr_counter(&self, name: &str, value: i64) -> Result<i64>;
    // Set counter name to value if old <= value - diff.
    async fn set_if_small(&self, name: &str, value: i64, diff: i64) -> Result<bool>;
    async fn update_stats(&self, space: i64, inodes: i64);
    async fn flush_stats(&self);
    async fn do_load(&self) -> Result<Option<Vec<u8>>>;
    async fn do_new_session(self: Arc<Self>, sinfo: &[u8], update: bool) -> Result<()>;
    async fn do_refresh_session(&self) -> Result<()>;

    /// find stale session
    async fn do_find_stale_sessions(&self, limit: isize) -> Result<Vec<u64>>;
    async fn do_clean_stale_session(&self, sid: u64) -> Result<()>;
    async fn do_init(&self, format: Format, force: bool) -> Result<()>;
    async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()>;
    async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()>;
    async fn do_find_deleted_files(&self, ts: i64, limit: isize) -> Result<HashMap<Ino, u64>>;
    async fn do_delete_file_data(&self, inode: Ino, length: u64);
    async fn do_cleanup_slices(&self);
    async fn do_cleanup_delayed_slices(&self, edge: i64) -> Result<i32>;
    async fn do_delete_slice(&self, id: u64, size: u32) -> Result<()>;
    async fn do_clone_entry(
        &self,
        src_ino: Ino,
        parent: Ino,
        name: &str,
        ino: Ino,
        attr: &Attr,
        cmode: u8,
        cumask: u16,
        top: bool,
    ) -> Result<()>;
    async fn do_attach_dir_node(&self, parent: Ino, dst_ino: Ino, name: &str) -> Result<()>;
    async fn do_find_detached_nodes(&self, t: SystemTime) -> Vec<Ino>;
    async fn do_cleanup_detached_node(&self, detached_node: Ino) -> Result<()>;
    async fn do_get_quota(&self, inode: Ino) -> Result<Quota>;
    async fn do_del_quota(&self, inode: Ino) -> Result<()>;
    async fn do_load_quotas(&self) -> Result<HashMap<Ino, Quota>>;
    async fn do_flush_quotas(&self, quotas: HashMap<Ino, Quota>) -> Result<()>;
    async fn do_get_attr(&self, inode: Ino, attr: &Attr) -> Result<()>;
    async fn do_set_attr(
        &self,
        inode: Ino,
        set: u16,
        sugidclearmode: u8,
        attr: &Attr,
    ) -> Result<()>;
    async fn do_lookup(&self, parent: Ino, name: &str, inode: &Ino, attr: &Attr) -> Result<()>;
    async fn do_mknod(
        &self,
        parent: Ino,
        name: &str,
        _type: u8,
        mode: u16,
        cumask: u16,
        path: &str,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()>;
    async fn do_link(&self, inode: Ino, parent: Ino, name: &str, attr: &Attr) -> Result<()>;
    async fn do_unlink(
        &self,
        parent: Ino,
        name: &str,
        attr: &Attr,
        skip_check_trash: bool,
    ) -> Result<()>;
    async fn do_rmdir(
        &self,
        parent: Ino,
        name: &str,
        inode: &Ino,
        skip_check_trash: bool,
    ) -> Result<()>;
    async fn do_readlink(&self, inode: Ino, noatime: bool) -> Result<(i64, Vec<u8>)>;
    async fn do_readdir(
        &self,
        inode: Ino,
        plus: u8,
        entries: &mut Vec<Entry>,
        limit: i32,
    ) -> Result<()>;
    async fn do_rename(
        &self,
        parent_src: Ino,
        name_src: String,
        parent_dst: Ino,
        name_dst: String,
        flags: u32,
        inode: &mut Ino,
        tinode: &mut Ino,
        attr: &mut Attr,
        tattr: &mut Attr,
    ) -> Result<()>;
    async fn do_set_xattr(&self, inode: Ino, name: String, value: Bytes, flags: u32) -> Result<()>;
    async fn do_remove_xattr(&self, inode: Ino, name: String) -> Result<()>;
    async fn do_repair(&self, inode: Ino, attr: &mut Attr) -> Result<()>;
    async fn do_touch_atime(&self, inode: Ino, attr: Attr, ts: SystemTime) -> Result<bool>;
    async fn do_read(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>>;
    async fn do_write(
        &self,
        inode: Ino,
        indx: u32,
        off: u32,
        slice: Slice,
        mtime: SystemTime,
        num_slices: &mut i32,
        delta: &mut DirStat,
        attr: &mut Attr,
    ) -> Result<()>;

    async fn do_truncate(
        &self,
        inode: Ino,
        flags: u8,
        length: u64,
        delta: &mut DirStat,
        attr: &mut Attr,
        skip_perm_check: bool,
    ) -> Result<()>;

    async fn do_fallocate(
        &self,
        inode: Ino,
        mode: u8,
        off: u64,
        size: u64,
        delta: &mut DirStat,
        attr: &mut Attr,
    ) -> Result<()>;

    async fn do_compact_chunk(
        &self,
        inode: Ino,
        indx: u32,
        origin: &[u8],
        ss: &mut [Slice],
        skipped: i32,
        pos: u32,
        id: u64,
        size: u32,
        delayed: &[u8],
    ) -> Result<()>;

    async fn do_get_parents(&self, inode: Ino) -> HashMap<Ino, i32>;
    async fn do_update_dir_stat(&self, batch: HashMap<Ino, DirStat>) -> Result<()>;
    // @trySync: try sync dir stat if broken or not existed
    async fn do_get_dir_stat(&self, ino: Ino, try_sync: bool) -> Result<DirStat>;
    async fn do_sync_dir_stat(&self, ino: Ino) -> Result<DirStat>;

    async fn scan_trash_slices(&self, trash_slice_scan: TrashSliceScan) -> Result<()>;
    async fn scan_pending_slices(&self, pending_slice_scan: PendingSliceScan) -> Result<()>;
    async fn scan_pending_files(&self, pending_file_scan: PendingFileScan) -> Result<()>;

    async fn get_session(&self, sid: u64, detail: bool) -> Result<Session>;

    async fn do_set_facl(&self, ino: Ino, acl_type: u8, rule: &Rule) -> Result<()>;
    async fn do_get_facl(&self, ino: Ino, acl_type: u8, acl_id: u32, rule: &Rule) -> Result<()>;
    async fn cache_acls(&self) -> Result<()>;
}

pub struct InternalNode {
    inode: Ino,
    name: String,
}

pub struct SessionState {
    pub conf: Config,
    pub fmt: RwLock<Format>,
    pub sid: AtomicU64,
    pub open_file: OpenFile,
    pub fs_stat: FsStat,
    // TODO: encapsulation it into Segmented Lock
    // Pessimistic locks to reduce conflicts
    pub txn_locks: [Mutex<()>; N_LOCK],
    pub canceled: AtomicBool,
}

impl SessionState {
    pub fn new_session_info(&self) -> String {
        let host = match hostname::get() {
            Ok(host) => host.to_string_lossy().to_string(),
            Err(err) => {
                warn!("Failed to get hostname: {}", err);
                "localhost".to_string()
            }
        };
        let ips = match get_if_addrs::get_if_addrs() {
            Ok(ips) => ips.iter().map(|i| i.ip()).collect(),
            Err(err) => {
                warn!("Failed to get local IP: {}", err);
                Vec::new()
            }
        };
        let session_info = SessionInfo {
            version: "0.1".to_string(), // TODO: get it in runtime
            host_name: host,
            ip_addrs: ips,
            mount_point: self.conf.mount_point.clone(),
            mount_time: SystemTime::now(),
            process_id: get_current_pid().unwrap().as_u32(),
        };
        serde_json::to_string(&session_info)
            .expect("session info serialization should never failed")
    }
}

pub struct CommonMeta<E> {
    engine: E,
    session_state: Arc<SessionState>,
    addr: String,
    root: Ino,
    sub_trash: InternalNode,
    removed_files: HashMap<Ino, bool>,
    compacting: HashMap<u64, bool>,
    max_deleting: Sender<()>,
    dslices: Sender<Slice>, // slices to delete
    symlinks: HashMap<Ino, String>,
    umounting: bool,
    ses_mu: Mutex<()>,
    dir_stats_lock: Mutex<()>,
    dir_stats: HashMap<Ino, DirStat>,
    fs_stat: FsStat,
    parent_mu: Mutex<()>,
    quota_mu: RwLock<()>,            // protect dirParents
    dir_parents: HashMap<Ino, Ino>,  // protect dirQuotas
    dir_quotas: HashMap<Ino, Quota>, // directory inode -> parent inode
    free_mu: Mutex<()>,              // directory inode -> quota
    free_inodes: FreeID,
    free_slices: FreeID,
}

impl<E: Send + Sync + 'static + Engine> CommonMeta<E> {
    pub fn new(e: E) -> Self {
        todo!()
    }
}

#[async_trait]
impl<E: Send + Sync + 'static + Engine> Meta for CommonMeta<E>

{
    // Name of database
    fn name(&self) -> String {
        todo!()
    }

    // Init is used to initialize a meta service.
    async fn init(&self, format: &Format, force: bool) -> Result<()> {
        todo!()
    }

    // Shutdown close current database connections.
    async fn shutdown(&self) -> Result<()> {
        todo!()
    }

    // Reset cleans up all metadata, VERY DANGEROUS!
    async fn reset(&self) -> Result<()> {
        todo!()
    }

    // Load loads the existing setting of a formatted volume from meta service.
    async fn load(&self, check_version: bool) -> Result<Format> {
        todo!()
    }

    // NewSession creates or update client session.
    async fn new_session(&self, record: bool) -> Result<()> {
        todo!()
    }

    // CloseSession does cleanup and close the session.
    async fn close_session(&self) -> Result<()> {
        todo!()
    }

    // GetSession retrieves information of session with sid
    async fn get_session(&self, sid: u64, detail: bool) -> Result<Session> {
        todo!()
    }

    // ListSessions returns all client sessions.
    async fn list_sessions(&self) -> Result<Vec<Session>> {
        todo!()
    }

    // ScanDeletedObject scan deleted objects by customized scanner.
    async fn scan_deleted_object(
        &self,
        trash_slice_scan: TrashSliceScan,
        pending_slice_scan: PendingSliceScan,
        trash_file_scan: TrashFileScan,
        pending_file_scan: PendingFileScan,
    ) -> Result<()> {
        todo!()
    }

    // ListLocks returns all locks of a inode.
    async fn list_locks(&self, inode: Ino) -> Result<(Vec<PLockItem>, Vec<FLockItem>)> {
        todo!()
    }

    // CleanStaleSessions cleans up sessions not active for more than 5 minutes
    async fn clean_stale_sessions(&self, edge: SystemTime, incre_process: Box<dyn Fn(isize) + Send>) {
        todo!()
    }

    // CleanupDetachedNodesBefore deletes all detached nodes before the given time.
    async fn cleanup_detached_nodes_before(&self, edge: SystemTime, incre_process: Box<dyn Fn(isize) + Send>) {
        todo!()
    }

    // GetPaths returns all paths of an inode
    async fn get_paths(&self, inode: Ino) -> Vec<String> {
        todo!()
    }

    // Check integrity of an absolute path and repair it if asked
    async fn check(&self, fpath: String, repair: bool, recursive: bool, stat_all: bool) -> Result<()> {
        todo!()
    }

    // Get a copy of the current format
    async fn get_format(&self) -> Format {
        todo!()
    }

    // OnMsg add a callback for the given message type.
    /*async fn on_msg(mtype: u32, cb: MsgCallback);*/
    
    // OnReload register a callback for any change founded after reloaded.
    async fn on_reload(&self, cb: Box<dyn Fn(Format) + Send>) {
        todo!()
    }

    async fn handle_quota(
        &self, 
        cmd: u8,
        dpath: String,
        quotas: HashMap<String, Quota>,
        strict: bool,
        repair: bool,
    ) -> Result<()> {
        todo!()
    }

    // Dump the tree under root, which may be modified by checkRoot
    async fn dump_meta(
        &self, 
        w: Box<dyn Write + Send>,
        root: Ino,
        threads: isize,
        keep_secret: bool,
        fast: bool,
        skip_trash: bool,
    ) -> Result<()> {
        todo!()
    }

    async fn load_meta(&self, r: Box<dyn Read + Send>) -> Result<()> {
        todo!()
    }

    // ---------------------------------------- sys call -----------------------------------------------------------
    // StatFS returns summary statistics of a volume.
    async fn stat_fs(
        &self,
        inode: Ino,
        totalspace: AtomicU64,
        availspace: AtomicU64,
        iused: AtomicU64,
        iavail: AtomicU64,
    ) -> Result<()> {
        todo!()
    }

    // Access checks the access permission on given inode.
    async fn access(&self, inode: Ino, mode_mask: u8, attr: &Attr) -> Result<()> {
        todo!()
    }

    // Lookup returns the inode and attributes for the given entry in a directory.
    async fn lookup(
        &self,
        parent: Ino,
        name: String,
        inode: &Ino,
        attr: &Attr,
        check_perm: bool,
    ) -> Result<()> {
        todo!()
    }
    // Resolve fetches the inode and attributes for an entry identified by the given path.
    // ENOTSUP will be returned if there's no natural implementation for this operation or
    // if there are any symlink following involved.
    async fn resolve(&self, parent: Ino, path: String, inode: &Ino, attr: &Attr) -> Result<()> {
        todo!()
    }

    // GetAttr returns the attributes for given node.
    async fn get_attr(&self, inode: Ino, attr: &Attr) -> Result<()> {
        todo!()
    }

    // SetAttr updates the attributes for given node.
    async fn set_attr(&self, inode: Ino, set: u16, sggid_clear_mode: u8, attr: &Attr)
        -> Result<()> {
        todo!()
    }

    // Check setting attr is allowed or not
    async fn check_set_attr(&self, inode: Ino, set: u16, attr: &Attr) -> Result<()> {
        todo!()
    }

    // Truncate changes the length for given file.
    async fn truncate(
        &self,
        inode: Ino,
        flags: u8,
        attr_length: u64,
        attr: &Attr,
        skip_perm_check: bool,
    ) -> Result<()> {
        todo!()
    }

    // Fallocate preallocate given space for given file.
    async fn fallocate(
        &self,
        inode: Ino,
        mode: u8,
        off: u64,
        size: u64,
        length: &u64,
    ) -> Result<()> {
        todo!()
    }

    // ReadLink returns the target of a symlink.
    async fn read_link(&self, inode: Ino, path: &Vec<u8>) -> Result<()> {
        todo!()
    }

    // Symlink creates a symlink in a directory with given name.
    async fn symlink(
        &self,
        parent: Ino,
        name: String,
        path: String,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()> {
        todo!()
    }

    // Mknod creates a node in a directory with given name, type and permissions.
    async fn mknod(
        &self,
        parent: Ino,
        name: String,
        r#type: u8,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: String,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()> {
        todo!()
    }

    // Mkdir creates a sub-directory with given name and mode.
    async fn mkdir(
        &self,
        parent: Ino,
        name: String,
        mode: u16,
        cumask: u16,
        copysgid: u8,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()> {
        todo!()
    }

    // Unlink removes a file entry from a directory.
    // The file will be deleted if it's not linked by any entries and not open by any sessions.
    async fn unlink(&self, parent: Ino, name: String, skip_check_trash: bool) -> Result<()> {
        todo!()
    }

    // Rmdir removes an empty sub-directory.
    async fn rmdir(&self, parent: Ino, name: String, skip_check_trash: bool) -> Result<()> {
        todo!()
    }

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
    ) -> Result<()> {
        todo!()
    }

    // Link creates an entry for node.
    async fn link(&self, inode_src: Ino, parent: Ino, name: String, attr: &Attr) -> Result<()> {
        todo!()
    }

    // Readdir returns all entries for given directory, which include attributes if plus is true.
    async fn readdir(&self, inode: Ino, wantattr: u8, entries: &Vec<Entry>) -> Result<()> {
        todo!()
    }

    // Create creates a file in a directory with given name.
    async fn create(
        &self,
        parent: Ino,
        name: String,
        mode: u16,
        cumask: u16,
        flags: u32,
        inode: &Ino,
        attr: &Attr,
    ) -> Result<()> {
        todo!()
    }

    // Open checks permission on a node and track it as open.
    async fn open(&self, inode: Ino, flags: u32, attr: &Attr) -> Result<()> {
        todo!()
    }

    // Close a file.
    async fn close(&self, inode: Ino) -> Result<()> {
        todo!()
    }

    // Read returns the list of slices on the given chunk.
    async fn read(&self, inode: Ino, indx: u32, slices: &Vec<Slice>) -> Result<()> {
        todo!()
    }

    // NewSlice returns an id for new slice.
    async fn new_slice(&self, id: &u64) -> Result<()> {
        todo!()
    }

    // Write put a slice of data on top of the given chunk.
    async fn write(&self, inode: Ino, indx: u32, off: u32, slice: &Slice, mtime: u64)
        -> Result<()> {
        todo!()
    }

    // InvalidateChunkCache invalidate chunk cache
    async fn invalidate_chunk_cache(&self, inode: Ino, indx: u32) -> Result<()> {
        todo!()
    }

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
    ) -> Result<()> {
        todo!()
    }

    // GetParents returns a map of node parents (> 1 parents if hardlinked)
    async fn get_parents(&self, inode: Ino) -> HashMap<Ino, isize> {
        todo!()
    }

    // GetDirStat returns the space and inodes usage of a directory.
    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat> {
        todo!()
    }

    // GetXattr returns the value of extended attribute for given name.
    async fn get_xattr(&self, inode: Ino, name: String, v_buff: &Vec<u8>) -> Result<()> {
        todo!()
    }

    // ListXattr returns all extended attributes of a node.
    async fn list_xattr(&self, inode: Ino, dbuff: &Vec<u8>) -> Result<()> {
        todo!()
    }

    // SetXattr update the extended attribute of a node.
    async fn set_xattr(&self, inode: Ino, name: String, value: &Vec<u8>, flags: u32) -> Result<()> {
        todo!()
    }

    // RemoveXattr removes the extended attribute of a node.
    async fn remove_xattr(&self, inode: Ino, name: String) -> Result<()> {
        todo!()
    }

    // Flock tries to put a lock on given file.
    async fn flock(&self, inode: Ino, owner: u64, ltype: u32, block: bool) -> Result<()> {
        todo!()
    }

    // Getlk returns the current lock owner for a range on a file.
    async fn getlk(
        &self,
        inode: Ino,
        owner: u64,
        ltype: &u32,
        start: &u64,
        end: &u64,
        pid: &u32,
    ) -> Result<()> {
        todo!()
    }

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
    ) -> Result<()> {
        todo!()
    }

    // Compact all the chunks by merge small slices together
    async fn compact_all(&self, threads: isize, bar: juice_utils::process::Bar) -> Result<()> {
        todo!()
    }

    // Compact chunks for specified path
    async fn compact(
        &self,
        inode: Ino,
        concurrency: isize,
        pre_func: Box<dyn Fn() + Send>,
        post_func: Box<dyn Fn() + Send>,
    ) -> Result<()> {
        todo!()
    }

    // ListSlices returns all slices used by all files.
    async fn list_slices(
        &self,
        slices: &HashMap<Ino, Vec<Slice>>,
        delete: bool,
        show_progress: Box<dyn Fn() + Send>,
    ) -> Result<()> {
        todo!()
    }

    // Remove all files and directories recursively.
    // count represents the number of attempted deletions of entries (even if failed).
    async fn remove(&self, parent: Ino, name: String, count: &u64) -> Result<()> {
        todo!()
    }

    // Get summary of a node; for a directory it will accumulate all its child nodes
    async fn get_summary(
        &self,
        inode: Ino,
        summary: &Summary,
        recursive: bool,
        strict: bool,
    ) -> Result<()> {
        todo!()
    }

    // GetTreeSummary returns a summary in tree structure
    async fn get_tree_summary(
        &self,
        root: &TreeSummary,
        depth: u8,
        topn: u8,
        strict: bool,
        update_progress: Box<dyn Fn(u64, u64) + Send>,
    ) -> Result<()> {
        todo!()
    }

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
    ) -> Result<()> {
        todo!()
    }

    // Change root to a directory specified by subdir
    async fn chroot(&self, subdir: String) -> Result<()> {
        todo!()
    }
}
