use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime}};

use async_channel::Sender;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use juice_utils::process::Bar;
use parking_lot::Mutex;
use snafu::ensure;

use crate::{acl::{AclType, Rule}, api::{Attr, Entry, Falloc, INodeType, Ino, RenameMask, Session, SetAttrMask, Slice, XattrF}, base::{Cchunk, CommonMeta, DirStat, Engine, PendingFileScan, PendingSliceScan, SetQuota, TrashSliceScan}, config::{Config, Format}, context::FsContext, error::{NoEntryFound2Snafu, OpNotPermittedSnafu, Result}, quota::QuotaView, slice::PSlices};


#[derive(Clone)]
pub struct MemEngine {
    meta: Arc<CommonMeta>,
    handle: Arc<Mutex<MemHandle>>,
    context: FsContext,
}

struct MemHandle {
    attrs: HashMap<Ino, Attr>,
}

impl AsRef<FsContext> for MemEngine {
    fn as_ref(&self) -> &FsContext {
        &self.context
    }
}

impl AsMut<FsContext> for MemEngine {
    fn as_mut(&mut self) -> &mut FsContext {
        &mut self.context
    }
}

impl AsRef<CommonMeta> for MemEngine {
    fn as_ref(&self) -> &CommonMeta {
        &self.meta
    }
}

impl MemEngine {
    pub fn new(conf: Config) -> Self {
        let meta = CommonMeta::new(conf);
        Self {
            handle: Arc::new(Mutex::new(
                todo!()
            )),
            context: FsContext::default(),
            meta: Arc::new(meta),
        }
    }
}

#[async_trait]
impl Engine for MemEngine {
    // Get the value of counter name.
    async fn get_counter(&self, name: &str) -> Result<Option<i64>> {
        todo!()
    }
    // Increase counter name by value. Do not use this if value is 0, use getCounter instead.
    async fn incr_counter(&self, name: &str, value: i64) -> Result<i64> {
        todo!()
    }
    // Set counter name to value if old <= value - diff.
    async fn set_if_small(&self, name: &str, value: Duration, diff: Duration) -> Result<bool> {
        todo!()
    }
    async fn flush_stats(&self) {
        todo!()
    }
    async fn do_load(&self) -> Result<Option<Format>> {
        todo!()
    }
    async fn do_new_session(&self, sid: u64, sinfo: &[u8], update: bool) -> Result<()> {
        todo!()
    }
    async fn do_refresh_session(&self, sid: u64) -> Result<()> {
        todo!()
    }
    async fn do_list_sessions(&self) -> Result<Vec<Session>> {
        todo!()
    }
    async fn do_get_session(&self, sid: u64, detail: bool) -> Result<Session> {
        todo!()
    }

    /// find stale session
    async fn do_find_stale_sessions(&self, limit: isize) -> Result<Vec<u64>> {
        todo!()
    }
    async fn do_clean_stale_session(&self, sid: u64) -> Result<()> {
        todo!()
    }
    async fn do_init(&self, format: Format, force: bool) -> Result<()> {
        todo!()
    }
    async fn do_reset(&self) -> Result<()> {
        todo!()
    }
    async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()> {
        todo!()
    }
    async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()> {
        todo!()
    }
    async fn do_find_deleted_files(&self, ts: Duration, limit: isize) -> Result<HashMap<Ino, u64>> {
        todo!()
    }
    async fn do_delete_file_data(&self, inode: Ino, length: u64) {
        todo!()
    }
    async fn do_cleanup_slices(&self) {
        todo!()
    }
    async fn do_cleanup_delayed_slices(&self, edge: i64) -> Result<i32> {
        todo!()
    }
    async fn do_delete_slice(&self, id: u64, size: u32) -> Result<()> {
        todo!()
    }
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
    ) -> Result<()> {
        todo!()
    }
    async fn do_attach_dir_node(&self, parent: Ino, dst_ino: Ino, name: &str) -> Result<()> {
        todo!()
    }
    async fn do_find_detached_nodes(&self, t: SystemTime) -> Vec<Ino> {
        todo!()
    }
    async fn do_cleanup_detached_node(&self, detached_node: Ino) -> Result<()> {
        todo!()
    }
    // quota manage
    async fn do_get_quota(&self, inode: Ino) -> Result<Option<QuotaView>> {
        todo!()
    }
    // this function is dangerous, it will cause quota inconsistency
    async fn do_set_quota(&self, inode: Ino, set: SetQuota) -> Result<Option<(u64, u64)>> {
        todo!()
    }
    async fn do_del_quota(&self, inode: Ino) -> Result<()> {
        todo!()
    }
    async fn do_load_quotas(&self) -> Result<HashMap<Ino, QuotaView>> {
        todo!()
    }
    async fn do_flush_quotas(&self, quotas: &HashMap<Ino, (i64, i64)>) -> Result<()> {
        todo!()
    }
    // meta functions
    async fn do_get_attr(&self, inode: Ino) -> Result<Attr> {
        // TODO: Implement the logic to get the attributes of the specified inode.
        todo!()
    }
    async fn do_set_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        sugidclearmode: u8,
        attr: &Attr,
    ) -> Result<Option<Attr>> {
        // TODO: Implement the logic to set the attributes of the specified inode.
        todo!()
    }
    async fn do_exists(&self, inode: Ino) -> Result<bool> {
        // TODO: Implement the logic to check if the specified inode exists.
        todo!()
    }
    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr)> {
        // TODO: Implement the logic to lookup the specified name in the specified parent directory.
        todo!()
    }
    async fn do_resolve(&self, parent: Ino, path: &str) -> Result<(Ino, Attr)> {
        // TODO: Implement the logic to resolve the specified path in the specified parent directory.
        todo!()
    }
    async fn do_mknod(
        &self,
        parent: Ino,
        name: &str,
        cumask: u16,
        path: &str,
        inode: Ino,
        attr: Attr,
    ) -> Result<Attr> {
        // TODO: Implement the logic to create a new node with the specified attributes in the specified parent directory.
        todo!()
    }
    async fn do_link(&self, inode: Ino, parent: Ino, name: &str) -> Result<Attr> {
        // TODO: Implement the logic to create a hard link to the specified inode in the specified parent directory.
        todo!()
    }
    async fn do_unlink(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Attr> {
        // TODO: Implement the logic to unlink the specified name from the specified parent directory.
        todo!()
    }
    async fn do_rmdir(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Ino> {
        // TODO: Implement the logic to remove the specified directory from the specified parent directory.
        todo!()
    }
    async fn do_read_symlink(&self, inode: Ino, no_atime: bool) -> Result<(Option<u128>, String)> {
        // TODO: Implement the logic to read the target of the specified symbolic link.
        todo!()
    }
    async fn do_readdir(
        &self,
        inode: Ino,
        wantattr: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Entry>> {
        // TODO: Implement the logic to read the entries in the specified directory.
        todo!()
    }
    async fn do_rename(
        &self,
        parent_src: Ino,
        name_src: &str,
        parent_dst: Ino,
        name_dst: &str,
        flags: RenameMask,
    ) -> Result<(Ino, Attr, Option<(Ino, Attr)>)> {
        // TODO: Implement the logic to rename the specified source name in the specified source parent directory to the specified destination name in the specified destination parent directory.
        todo!()
    }
    async fn do_set_xattr(
        &self,
        inode: Ino,
        name: &str,
        value: Vec<u8>,
        flag: XattrF,
    ) -> Result<()> {
        // TODO: Implement the logic to set the extended attribute with the specified name and value on the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>> {
        // TODO: Implement the logic to get the extended attribute with the specified name on the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_list_xattr(&self, inode: Ino) -> Result<Vec<u8>> {
        // TODO: Implement the logic to list all extended attributes on the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_remove_xattr(&self, inode: Ino, name: &str) -> Result<()> {
        // TODO: Implement the logic to remove the extended attribute with the specified name from the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_repair(&self, inode: Ino, attr: &mut Attr) -> Result<()> {
        // TODO: Implement the logic to repair the specified inode and its attributes.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_touch_atime(&self, inode: Ino, ts: Duration) -> Result<Attr> {
        // TODO: Implement the logic to update the access time of the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_read(&self, inode: Ino, indx: u32) -> Result<PSlices> {
        // TODO: Implement the logic to read data from the specified inode at the given index.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_write(
        &self,
        inode: Ino,
        indx: u32,
        coff: u32,
        slice: Slice,
        mtime: DateTime<Utc>,
    ) -> Result<(u32, DirStat, Attr)> {
        let handle = self.handle.lock();
        let attr = handle.attrs.get(&inode).ok_or(NoEntryFound2Snafu { ino: inode }.build())?;
        ensure!(
            attr.typ == INodeType::File,
            OpNotPermittedSnafu {
                op: "write into a non file"
            }
        );
        
        // TODO: Implement the logic to write data to the specified inode at the given index and offset.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    // Returns (truncated attr, delta dirstat)
    async fn do_truncate(
        &self,
        inode: Ino,
        flags: u8,
        length: u64,
        skip_perm_check: bool,
    ) -> Result<(Attr, DirStat)> {
        // TODO: Implement the logic to truncate the specified inode to the given length.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    async fn do_copy_file_range(
        &self,
        src: Ino,
        src_off: u64,
        dst: Ino,
        dst_off: u64,
        len: u64,
        flags: u32,
    ) -> Result<Option<(u64, u64, (Ino, DirStat))>> {
        // TODO: Implement the logic to copy a range of data from the source inode to the destination inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    async fn do_fallocate(
        &self,
        inode: Ino,
        flag: Falloc,
        off: u64,
        size: u64,
    ) -> Result<(DirStat, Attr)> {
        // TODO: Implement the logic to allocate space for the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

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
    ) -> Result<()> {
        // TODO: Implement the logic to compact the chunk of data in the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    async fn do_list_slices(&self, delete: bool) -> Result<HashMap<Ino, Vec<Slice>>> {
        // TODO: Implement the logic to list all slices in the file system.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    async fn do_get_parents(&self, inode: Ino) -> HashMap<Ino, u32> {
        // TODO: Implement the logic to get the parent inodes of the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    /// do add dirstats batch periodly
    async fn do_flush_dir_stat(&self, batch: HashMap<Ino, DirStat>) -> Result<()> {
        // TODO: Implement the logic to flush the directory statistics for the specified inodes.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    // @trySync: try sync dir stat if broken or not existed
    async fn do_get_dir_stat(&self, ino: Ino, try_sync: bool) -> Result<Option<DirStat>> {
        // TODO: Implement the logic to get the directory statistics for the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_sync_dir_stat(&self, ino: Ino) -> Result<DirStat> {
        // TODO: Implement the logic to sync the directory statistics for the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    async fn scan_trash_slices(&self, trash_slice_scan: TrashSliceScan) -> Result<()> {
        // TODO: Implement the logic to scan and process the trash slices.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn scan_pending_slices(&self, pending_slice_scan: PendingSliceScan) -> Result<()> {
        // TODO: Implement the logic to scan and process the pending slices.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn scan_pending_files(&self, pending_file_scan: PendingFileScan) -> Result<()> {
        // TODO: Implement the logic to scan and process the pending files.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }

    async fn do_set_facl(&self, ino: Ino, acl_type: AclType, rule: &Rule) -> Result<()> {
        // TODO: Implement the logic to set the file access control list (ACL) for the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn do_get_facl(&self, ino: Ino, acl_type: AclType, acl_id: u32) -> Result<Rule> {
        // TODO: Implement the logic to get the file access control list (ACL) for the specified inode.
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
    async fn cache_acls(&self) -> Result<()> {
        // TODO: Implement the logic to cache the file access control lists (ACLs).
        // Replace `todo!()` with the implementation logic.
        todo!()
    }
}