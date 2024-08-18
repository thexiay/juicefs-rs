use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use crate::acl::{Entry, Rule};
use crate::api::{Attr, BaseMeta, Ino, Session, Slice};
use crate::config::Format;
use crate::error::Result;
use crate::openfile::OpenFile;
use crate::quota::Quota;
use crate::utils::{Bar, FreeID};

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

// fsStat aligned for atomic operations
// nolint:structcheck
pub struct FsStat {
    new_space: i64,
    new_inodes: i64,
    used_space: i64,
    used_inodes: i64,
}

pub struct DirStat {
	length: i64,
	space: i64,
	inodes:i64,
}

#[async_trait]
pub trait Engine: 'static {
    // Get the value of counter name.
    async fn get_counter(&self, name: String) -> Result<i64>;
    // Increase counter name by value. Do not use this if value is 0, use getCounter instead.
    async fn incr_counter(&self, name: String, value: i64) -> Result<i64>;
    // Set counter name to value if old <= value - diff.
    async fn set_if_small(&self, name: String, value: i64, diff: i64) -> Result<bool>;
    async fn update_stats(&self, space: i64, inodes: i64);
    async fn flush_stats(&self);
    async fn do_load(&self) -> Result<Vec<u8>>;
    async fn do_new_session(&self, sinfo: &[u8], update: bool) -> Result<()>;
    async fn do_refresh_session(&self) -> Result<()>;
    async fn do_find_stale_sessions(&self, limit: i32) -> Result<Vec<u64>>;
    async fn do_clean_stale_session(&self, sid: u64) -> Result<()>;
    async fn do_init(&self, format: &Format, force: bool) -> Result<()>;
    async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()>;
    async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()>;
    async fn do_find_deleted_files(&self, ts: i64, limit: i32) -> Result<HashMap<Ino, u64>>;
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
    async fn do_set_attr(&self, inode: Ino, set: u16, sugidclearmode: u8, attr: &Attr) -> Result<()>;
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
    async fn do_unlink(&self, parent: Ino, name: &str, attr: &Attr, skip_check_trash: bool) -> Result<()>;
    async fn do_rmdir(&self, parent: Ino, name: &str, inode: &Ino, skip_check_trash: bool) -> Result<()>;
    async fn do_readlink(&self, inode: Ino, noatime: bool) -> Result<(i64, Vec<u8>)>;
    async fn do_readdir(&self, inode: Ino, plus: u8, entries: &mut Vec<Entry>, limit: i32) -> Result<()>;
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

pub struct CommonMeta<T> {
    pub addr: String,
    pub conf: Format,
    pub fmt: Format,
    pub root: Ino,
    pub sub_trash: InternalNode,
    pub sid: u64,
    pub open_file: OpenFile,
    pub removed_files: HashMap<Ino, bool>,
    pub compacting: HashMap<u64, bool>,
    pub max_deleting: Sender<()>,
    pub dslices: Sender<Slice>,  // slices to delete
    pub symlinks: HashMap<Ino, String>,
    pub reload_cb: Vec<Box<dyn Fn(&Format)>>,
    pub umounting: bool,
    pub ses_mu: Mutex<()>,
    pub dir_stats_lock: Mutex<()>,
    pub dir_stats: HashMap<Ino, DirStat>,
    pub fs_stat: FsStat,
    pub parent_mu: Mutex<()>,
    pub quota_mu: RwLock<()>,  // protect dirParents
    pub dir_parents: HashMap<Ino, Ino>,  // protect dirQuotas
    pub dir_quotas: HashMap<Ino, Quota>,  // directory inode -> parent inode
    pub free_mu: Mutex<()>,  // directory inode -> quota
    pub free_inodes: FreeID,
    pub free_slices: FreeID,
    pub en: Arc<T>,
}

impl <T: AsRef<CommonMeta<T>>> BaseMeta for T {
    
}