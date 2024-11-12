use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use juice_utils::process::Bar;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{mem, process};
use sysinfo::{get_current_pid, PidExt};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, TryAcquireError};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use crate::acl::{self, AclCache, AclType, Rule};
use crate::api::{
    gid, gids, uid, Attr, Entry, INodeType, Ino, InoExt, Meta, ModeMask, Session, SessionInfo,
    Slice, Summary, TreeSummary, MAX_VERSION, RESERVED_INODE, ROOT_INODE, TRASH_INODE, TRASH_NAME,
};
use crate::config::{Config, Format};
use crate::error::{MyError, NotInitializedSnafu, Result, SysSnafu};
use crate::openfile::{OpenFile, OpenFiles};
use crate::quota::Quota;
use crate::utils::{access_mode, align_4k, sleep_with_jitter, FLockItem, FreeID, PLockItem};

pub const INODE_BATCH: i64 = 1 << 10;
pub const SLICE_ID_BATCH: i64 = 1 << 10;
pub const N_LOCK: usize = 1 << 10;
pub const CHANNEL_BUFFER: usize = 1024;
const SEGMENT_LOCK: Mutex<()> = Mutex::new(());
const UMOUNT_EXIT_CODE: i32 = 11;

pub const NEXT_INODE: &str = "nextinode";
pub const NEXT_CHUNK: &str = "nextchunk";
pub const NEXT_SESSION: &str = "nextsession";
pub const NEXT_TRASH: &str = "nexttrash";
pub const USED_SPACE: &str = "usedSpace";
pub const TOTAL_INODES: &str = "totalInodes";

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

/// fsStat aligned for atomic operations
/// nolint:structcheck
///
/// new: ?? useless for redis
/// used: used resource
#[derive(Default)]
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

#[derive(Default)]
pub struct DirStat {
    length: i64,
    space: i64,
    inodes: i64,
}

pub struct InternalNode {
    inode: Ino,
    name: String,
}

#[async_trait]
pub trait Engine {
    // Get the value of counter name.
    async fn get_counter(&self, name: &str) -> Result<i64>;
    // Increase counter name by value. Do not use this if value is 0, use getCounter instead.
    async fn incr_counter(&self, name: &str, value: i64) -> Result<i64>;
    // Set counter name to value if old <= value - diff.
    async fn set_if_small(&self, name: &str, value: Duration, diff: Duration) -> Result<bool>;
    async fn update_stats(&self, space: i64, inodes: i64);
    async fn flush_stats(&self);
    async fn do_load(&self) -> Result<Option<Format>>;
    async fn do_new_session(self: Arc<Self>, sid: u64, sinfo: &[u8], update: bool) -> Result<()>;
    async fn do_refresh_session(&self, sid: u64) -> Result<()>;
    async fn do_list_sessions(&self) -> Result<Vec<Session>>;
    async fn do_get_session(&self, sid: u64, detail: bool) -> Result<Session>;

    /// find stale session
    async fn do_find_stale_sessions(&self, limit: isize) -> Result<Vec<u64>>;
    async fn do_clean_stale_session(&self, sid: u64) -> Result<()>;
    async fn do_init(&self, format: Format, force: bool) -> Result<()>;
    async fn do_reset(&self) -> Result<()>;
    async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()>;
    async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()>;
    async fn do_find_deleted_files(&self, ts: Duration, limit: isize) -> Result<HashMap<Ino, u64>>;
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
    // quota manage
    async fn do_get_quota(&self, inode: Ino) -> Result<Quota>;
    async fn do_del_quota(&self, inode: Ino) -> Result<()>;
    async fn do_load_quotas(&self) -> Result<HashMap<Ino, Quota>>;
    async fn do_flush_quotas(&self, quotas: &HashMap<Ino, (i64, i64)>) -> Result<()>;
    // meta functions
    async fn do_get_attr(&self, inode: Ino) -> Result<Attr>;
    async fn do_set_attr(
        &self,
        inode: Ino,
        set: u16,
        sugidclearmode: u8,
        attr: &Attr,
    ) -> Result<()>;
    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr)>;
    async fn do_mknod(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        path: &str,
        inode: Ino,
        attr: Attr,
    ) -> Result<Attr>;
    async fn do_link(&self, inode: Ino, parent: Ino, name: &str, attr: &Attr) -> Result<()>;
    async fn do_unlink(
        self: Arc<Self>,
        parent: Ino,
        name: &str,
        skip_check_trash: bool,
    ) -> Result<Attr>;
    async fn do_rmdir(
        &self,
        parent: Ino,
        name: &str,
        skip_check_trash: bool,
    ) -> Result<Ino>;
    async fn do_readlink(&self, inode: Ino, noatime: bool) -> Result<(i64, Vec<u8>)>;
    async fn do_readdir(&self, inode: Ino, plus: u8, limit: i32) -> Result<Option<Vec<Entry>>>;
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
        skip_perm_check: bool,
    ) -> Result<(Attr, DirStat)>;

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
    /// do add dirstats batch periodly
    async fn do_flush_dir_stat(&self, batch: HashMap<Ino, DirStat>) -> Result<()>;
    // @trySync: try sync dir stat if broken or not existed
    async fn do_get_dir_stat(&self, ino: Ino, try_sync: bool) -> Result<DirStat>;
    async fn do_sync_dir_stat(&self, ino: Ino) -> Result<DirStat>;

    async fn scan_trash_slices(&self, trash_slice_scan: TrashSliceScan) -> Result<()>;
    async fn scan_pending_slices(&self, pending_slice_scan: PendingSliceScan) -> Result<()>;
    async fn scan_pending_files(&self, pending_file_scan: PendingFileScan) -> Result<()>;

    async fn do_set_facl(&self, ino: Ino, acl_type: AclType, rule: &Rule) -> Result<()>;
    async fn do_get_facl(&self, ino: Ino, acl_type: AclType, acl_id: u32) -> Result<Rule>;
    async fn cache_acls(&self) -> Result<()>;
}

#[async_trait]
pub trait MetaOtherFunction {
    async fn get_dir_parent(&self, inode: Ino) -> Result<Ino>;
    fn update_dir_stats(&self, inode: Ino, length: i64, space: i64, inodes: i64);
    async fn update_parent_stats(self: Arc<Self>, inode: Ino, parent: Ino, length: i64, space: i64);
    async fn next_inode(&self) -> Result<Ino>;
    async fn resolve_case(&self, parent: Ino, name: &str) -> Result<Option<Entry>>;

    /// Add a temporary folder to keep removed files for a certain time
    /// If rm, unlink, rm dir.e.g happens, they can be moved to `.trash` dir to keep some times.
    ///
    /// Returns the new trash inode if this ino should be moved to trash
    async fn check_trash(&self, parent: Ino) -> Result<Option<Ino>>;

    /// Immediately spawn a task to delete task
    /// force: wait unitl delete task has free handler
    /// delete_on_close: delete file when close
    async fn try_spawn_delete_file(
        self: Arc<Self>,
        inode: Ino,
        length: u64,
        force: bool,
        delete_on_close: bool,
    );

    /// ------------------------------ quota func ------------------------------
    /// check if the space and inodes exceed the quota limit for parents ino
    async fn check_quotas(&self, space: i64, inodes: i64, parents: Vec<Ino>) -> Result<()>;

    /// check if the space and inodes exceed the quota limit for single ino
    async fn check_quota(&self, ino: Ino, space: i64, inodes: i64) -> bool;

    /// update the new space and new inode for quota
    async fn update_quota(&self, inode: Ino, space: i64, inodes: i64);

    /// load quotas from persist layer
    async fn load_quotas(&self);

    /// flush quotas once
    async fn flush_quotas(&self);

    /// flush dir stats once
    async fn flush_dir_stat(&self);

    /// ------------------------------ session func ------------------------------
    /// do persist session info into meta persist layer periodly
    async fn refresh_session(self: Arc<Self>);

    /// cleanup trash which have not been deleted for days
    async fn cleanup_trash(&self, days: u8, force: bool);

    /// cleanup trash which is earlier than edge
    async fn cleanup_trash_before(&self, edge: Duration);

    /// cleanup delayed deleted slices which have not been deleted for days
    async fn cleanup_trash_slices(&self, days: u8);
}

pub struct CommonMeta {
    pub addr: String,
    pub root: Ino,
    pub sub_trash: Mutex<Option<InternalNode>>,
    pub removed_files: Mutex<HashMap<Ino, bool>>,
    pub compacting: HashMap<u64, bool>,
    pub max_deleting: Arc<Semaphore>,
    pub symlinks: HashMap<Ino, String>,
    pub reload_format_callbacks: Vec<Box<dyn Fn(Arc<Format>) + Send + Sync>>,
    pub acl_cache: AsyncMutex<AclCache>,
    pub dir_stats_batch: Mutex<HashMap<Ino, DirStat>>,
    pub fs_stat: FsStat,
    pub dir_parents: Mutex<HashMap<Ino, Ino>>, // directory inode -> parent inode
    // dir quotas, it storage every inode(dir type)'s quota
    pub quotas: RwLock<HashMap<Ino, Quota>>,
    pub free_inodes: AsyncMutex<FreeID>,
    pub free_slices: AsyncMutex<FreeID>,
    pub conf: Config,
    pub fmt: RwLock<Arc<Format>>,
    // current session id
    // session id can from
    // - set from user config
    // - new session greater than persist layer session id
    pub sid: RwLock<Option<u64>>,
    // session umounting, once meta begin, fs mounting; once session is closed, unmounting
    pub ses_umounting: AsyncMutex<bool>,
    pub open_files: Arc<OpenFiles>,
    // TODO: encapsulation it into Segmented Lock
    // Pessimistic locks to reduce conflicts
    pub txn_locks: [Mutex<()>; N_LOCK],
    pub canceled: AtomicBool,
}

impl CommonMeta {
    pub fn new(addr: &str, conf: Config) -> Self {
        let max_deleting = Arc::new(Semaphore::new(conf.max_deletes_task as usize));
        let open_files = OpenFiles::new(conf.open_cache, conf.open_cache_limit);
        CommonMeta {
            addr: addr.to_string(),
            root: ROOT_INODE,
            sub_trash: Mutex::new(None),
            removed_files: Mutex::new(HashMap::new()),
            compacting: HashMap::new(),
            max_deleting,
            symlinks: HashMap::new(),
            reload_format_callbacks: Vec::new(),
            ses_umounting: AsyncMutex::new(false),
            acl_cache: AsyncMutex::new(AclCache::default()),
            dir_stats_batch: Mutex::new(HashMap::new()),
            fs_stat: FsStat::default(),
            dir_parents: Mutex::new(HashMap::new()),
            quotas: RwLock::new(HashMap::new()),
            free_inodes: AsyncMutex::new(FreeID::default()),
            free_slices: AsyncMutex::new(FreeID::default()),
            conf,
            fmt: RwLock::new(Arc::new(Format::default())),
            sid: RwLock::new(None),
            open_files,
            txn_locks: [SEGMENT_LOCK; N_LOCK],
            canceled: AtomicBool::new(false),
        }
    }

    fn check_root(&self, ino: Ino) -> Ino {
        match ino {
            RESERVED_INODE => ROOT_INODE,
            ROOT_INODE => self.root,
            _ => ino,
        }
    }

    fn get_format(&self) -> Arc<Format> {
        self.fmt.read().clone()
    }

    pub fn new_session_info(&self) -> SessionInfo {
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
        SessionInfo {
            version: "0.1".to_string(), // TODO: get it in runtime
            host_name: host,
            ip_addrs: ips,
            mount_point: self.conf.mount_point.clone(),
            mount_time: SystemTime::now(),
            process_id: get_current_pid().unwrap().as_u32(),
        }
    }
}

#[async_trait]
impl<E> MetaOtherFunction for E
where
    E: Send + Sync + 'static + Engine + AsRef<CommonMeta>,
{
    async fn check_quotas(&self, space: i64, inodes: i64, parents: Vec<Ino>) -> Result<()> {
        if space <= 0 && inodes <= 0 {
            return Ok(());
        }
        let format = self.get_format();
        let meta = self.as_ref();
        if space > 0
            && format.capacity > 0
            && meta.fs_stat.used_space.load(Ordering::SeqCst)
                + meta.fs_stat.new_space.load(Ordering::SeqCst)
                + space
                > (format.capacity as i64)
        {
            return SysSnafu { code: libc::ENOSPC }.fail();
        }
        if inodes > 0
            && format.inodes > 0
            && meta.fs_stat.used_inodes.load(Ordering::SeqCst)
                + meta.fs_stat.new_inodes.load(Ordering::SeqCst)
                + inodes
                > (format.capacity as i64)
        {
            return SysSnafu { code: libc::ENOSPC }.fail();
        }
        if !format.enable_dir_stats {
            return Ok(());
        }
        for ino in parents {
            if self.check_quota(ino, space, inodes).await {
                return SysSnafu { code: libc::EDQUOT }.fail();
            }
        }
        Ok(())
    }

    async fn check_quota(&self, ino: Ino, space: i64, inodes: i64) -> bool {
        if !self.get_format().enable_dir_stats {
            return false;
        }

        let mut inode = ino;
        loop {
            {
                let guard = self.as_ref().quotas.read();
                if let Some(quota) = guard.get(&inode)
                    && quota.check(space, inodes)
                {
                    return true;
                }
            }
            if inode <= ROOT_INODE {
                break;
            }
            let last_ino = inode;
            match self.get_dir_parent(inode).await {
                Ok(i) => inode = i,
                Err(e) => {
                    warn!("Get directory parent of inode {}: {}", last_ino, e);
                    break;
                }
            }
        }
        false
    }

    async fn load_quotas(&self) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        // Here can not replcae directly, quotas's quota maybe concurrently accessed by other thread,
        // so we must modify it in place
        match self.do_load_quotas().await {
            Ok(loaded_quotas) => {
                let base = self.as_ref();
                let mut quotas = base.quotas.write();

                quotas.iter().for_each(|(ino, _)| {
                    if !loaded_quotas.contains_key(ino) {
                        // quota cache should be consistent with the meta persist layer
                        error!("Quota for inode {} is deleted", ino);
                    }
                });

                loaded_quotas.iter().for_each(|(ino, quota)| {
                    if let Some(q) = quotas.get(ino) {
                        quota
                            .new_space
                            .fetch_add(q.new_space.load(Ordering::SeqCst), Ordering::SeqCst);
                        quota
                            .new_inodes
                            .fetch_add(q.new_inodes.load(Ordering::SeqCst), Ordering::SeqCst);
                    }
                });
                *quotas = loaded_quotas;
            }
            Err(e) => warn!("Load quotas: {}", e),
        }
    }

    async fn get_dir_parent(&self, inode: Ino) -> Result<Ino> {
        let parent = {
            let guard = self.as_ref().dir_parents.lock();
            guard.get(&inode).map(|p| *p)
        };

        if let Some(parent) = parent {
            return Ok(parent);
        }
        debug!("Get directory parent of inode {}: cache miss", inode);
        let st = self.get_attr(inode).await?;
        Ok(st.parent)
    }

    fn update_dir_stats(&self, inode: Ino, length: i64, space: i64, inodes: i64) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        let mut dir_stats = self.as_ref().dir_stats_batch.lock();
        let stats = dir_stats.entry(inode).or_insert(DirStat::default());
        stats.length += length;
        stats.space += space;
        stats.inodes += inodes;
    }

    async fn update_parent_stats(
        self: Arc<Self>,
        inode: Ino,
        parent: Ino,
        length: i64,
        space: i64,
    ) {
        if length == 0 && space == 0 {
            return;
        }
        self.update_stats(space, 0).await;
        if !self.get_format().enable_dir_stats {
            return;
        }

        if parent > 0 {
            self.update_dir_stats(parent, length, space, 0);
            self.update_quota(parent, space, 0).await;
        } else {
            // spawn a task to shorten the time of updating parent stats
            tokio::spawn(async move {
                for (parent, _) in self.get_parents(inode).await {
                    self.update_dir_stats(parent, length, space, 0);
                    self.update_quota(parent, space, 0).await;
                }
            });
        }

        /*
            if length == 0 && space == 0 {
            return
        }
        m.en.updateStats(space, 0)
        if !m.getFormat().DirStats {
            return
        }
        if parent > 0 {
            m.updateDirStat(ctx, parent, length, space, 0)
            m.updateDirQuota(ctx, parent, space, 0)
        } else {
            go func() {
                for p := range m.en.doGetParents(ctx, inode) {
                    m.updateDirStat(ctx, p, length, space, 0)
                    m.updateDirQuota(ctx, p, space, 0)
                }
            }()
        }
             */
    }

    async fn update_quota(&self, mut inode: Ino, space: i64, inodes: i64) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        // recursively update the quota of the parent node until the root node is encountered
        loop {
            {
                let dir_quota = self.as_ref().quotas.read();
                if let Some(quota) = dir_quota.get(&inode) {
                    quota.update(space, inodes);
                }
            }
            if inode <= ROOT_INODE {
                break;
            }
            inode = match self.get_dir_parent(inode).await {
                Ok(parent) => parent,
                Err(err) => {
                    warn!("Get directory parent of inode {} err: {}", inode, err);
                    break;
                }
            };
        }
    }

    async fn next_inode(&self) -> Result<Ino> {
        let mut guard = self.as_ref().free_inodes.lock().await;
        if guard.next >= guard.maxid {
            let v = self.incr_counter("nextInode", INODE_BATCH).await?;
            guard.next = (v - INODE_BATCH) as u64;
            guard.maxid = v as u64;
        }
        let mut n = guard.next;
        guard.next += 1;
        // make sure inode >= 1
        while n <= 1 {
            n = guard.next;
            guard.next += 1;
        }
        Ok(n)
    }

    async fn resolve_case(&self, parent: Ino, name: &str) -> Result<Option<Entry>> {
        let entries = self.do_readdir(parent, 0, -1).await?;
        Ok(entries
            .map(|ens| ens.into_iter().find(|e| e.name.eq_ignore_ascii_case(&name)))
            .flatten())
    }

    async fn check_trash(&self, parent: Ino) -> Result<Option<Ino>> {
        let trash_enable = {
            if parent.is_trash() {
                false
            } else {
                self.as_ref().get_format().trash_days > 0
            }
        };

        if !trash_enable {
            return Ok(None);
        }
        let name = Utc::now().format("%Y-%m-%d-%H").to_string();
        {
            let sub_trash = self.as_ref().sub_trash.lock();
            if let Some(trash) = &*sub_trash
                && trash.name == name
            {
                return Ok(Some(trash.inode));
            }
        }

        let mut trash = None;
        // all trash file in one trash directory: `%Y-%m-%d-%H`
        // find trash name in trash root first, if this trash dir does not exist, create it.
        let lookup_res = match self.do_lookup(TRASH_INODE, &name).await {
            Err(MyError::SysError { code }) if code == libc::ENOENT => {
                let next = self.incr_counter(NEXT_TRASH, 1).await?;
                trash.insert(TRASH_INODE + next as u64);
                self.do_mknod(
                    TRASH_INODE,
                    &name,
                    0555,
                    0,
                    "",
                    trash.unwrap(),
                    Attr {
                        typ: INodeType::TypeDirectory,
                        nlink: 2,
                        length: 4 << 10,
                        parent: TRASH_INODE,
                        full: true,
                        ..Attr::default()
                    },
                )
                .await
            }
            Err(e) => Err(e),
            Ok((_, attr)) => Ok(attr),
        };
        match lookup_res {
            Err(e @ MyError::SysError { code }) if code != libc::EEXIST => {
                warn!("create subTrash {} failed {}", name, e);
                Err(e)
            }
            _ if trash.is_some_and(|trash_ino| trash_ino <= TRASH_INODE) => {
                warn!("invalid trash inode: {:?}", trash);
                Err(MyError::SysError { code: libc::EBADF }.into())
            }
            _ => Ok(trash.inspect(|trash| {
                let mut sub_trash = self.as_ref().sub_trash.lock();
                sub_trash.replace(InternalNode {
                    inode: *trash,
                    name,
                });
            })),
        }
    }

    async fn try_spawn_delete_file(
        self: Arc<Self>,
        inode: Ino,
        length: u64,
        force: bool,
        delete_on_close: bool,
    ) {
        let base = self.as_ref().as_ref();
        if delete_on_close {
            let mut remove_files = base.removed_files.lock();
            remove_files.insert(inode, true);
        } else {
            let meta = self.clone();
            let max_deleting = base.max_deleting.clone();
            tokio::spawn(async move {
                let permit = match force {
                    // TODO: 这里如果拿不到被关闭了，是不是要处理下
                    true => Some(max_deleting.acquire().await.unwrap()),
                    false => max_deleting
                        .try_acquire()
                        .inspect_err(|err| {
                            warn!("try acquire delete inode({}) permit failed: {}", inode, err);
                        })
                        .ok(),
                };
                if let Some(permit) = permit {
                    meta.do_delete_file_data(inode, length).await;
                    drop(permit);
                }
            });
        }
    }

    async fn refresh_session(self: Arc<Self>) {
        loop {
            let base = self.as_ref().as_ref();
            sleep_with_jitter(base.conf.heartbeat).await;
            {
                let unmounting = self.as_ref().as_ref().ses_umounting.lock().await;
                if *unmounting {
                    return;
                }
                let sid = { base.sid.read().map(|sid| sid) };
                if let Some(sid) = sid
                    && base.conf.read_only
                {
                    match self.do_refresh_session(sid).await {
                        Ok(_) => (),
                        Err(e) => error!("Refresh session: {}", e),
                    }
                }
            }

            // notify format changed
            let old = self.get_format();
            let format = self.load(false).await;
            match format {
                Ok(format) => {
                    if format.meta_version > MAX_VERSION {
                        error!(
                            "incompatible metadata version {} > max version {}",
                            format.meta_version, MAX_VERSION
                        );
                        process::exit(UMOUNT_EXIT_CODE);
                    }
                    if format.uuid != old.uuid {
                        error!("UUID changed from {} to {}", old.uuid, format.uuid);
                        process::exit(UMOUNT_EXIT_CODE);
                    }

                    if format != old {
                        for cb in base.reload_format_callbacks.iter() {
                            cb(format.clone());
                        }
                    }
                }
                Err(e @ MyError::NotInitializedError) => {
                    error!("reload setting: {}", e);
                    process::exit(UMOUNT_EXIT_CODE);
                }
                Err(e) => warn!("reload setting: {}", e),
            }

            match self.get_counter(USED_SPACE).await {
                Ok(v) => base.fs_stat.used_space.store(v, Ordering::SeqCst),
                Err(e) => warn!("Get counter {}: {}", USED_SPACE, e),
            }
            match self.get_counter(TOTAL_INODES).await {
                Ok(v) => base.fs_stat.used_inodes.store(v, Ordering::SeqCst),
                Err(e) => warn!("Get counter {}: {}", TOTAL_INODES, e),
            }
            self.load_quotas().await;

            if base.conf.read_only || !base.conf.enable_bg_job {
                continue;
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            match self
                .set_if_small("lastCleanupSessions", now, base.conf.heartbeat * 9 / 10)
                .await
            {
                Ok(true) => {
                    let meta = self.clone();
                    tokio::spawn(async move {
                        meta.cleanup_stale_sessions().await;
                    });
                }
                Err(e) => warn!("checking counter lastCleanupSessions: {}", e),
                _ => {}
            }
        }
    }

    async fn flush_quotas(&self) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        let base = self.as_ref();
        let stage_map = {
            let quotas = base.quotas.read();
            let mut stage_map = HashMap::new();
            for (ino, quota) in quotas.iter() {
                let new_space = quota.new_space.load(Ordering::SeqCst);
                let new_inodes = quota.new_inodes.load(Ordering::SeqCst);
                if new_space != 0 || new_inodes != 0 {
                    stage_map.insert(*ino, (new_space, new_inodes));
                }
            }
            stage_map
        };
        if stage_map.is_empty() {
            return;
        }

        match self.do_flush_quotas(&stage_map).await {
            Ok(_) => {
                let mut quotas = base.quotas.write();
                for (ino, snap) in stage_map {
                    let q = quotas.get_mut(&ino);
                    if let Some(q) = q {
                        q.new_space.fetch_sub(snap.0, Ordering::SeqCst);
                        q.used_space.fetch_add(snap.0, Ordering::SeqCst);
                        q.new_inodes.fetch_sub(snap.1, Ordering::SeqCst);
                        q.used_inodes.fetch_add(snap.1, Ordering::SeqCst);
                    }
                }
            }
            Err(e) => warn!("Flush quotas: {}", e),
        }
    }

    async fn flush_dir_stat(&self) {
        if !self.get_format().enable_dir_stats {
            return;
        }

        let base = self.as_ref();
        let dir_stats = {
            let mut dir_stats = base.dir_stats_batch.lock();
            mem::take(&mut *dir_stats)
        };
        if !dir_stats.is_empty() {
            self.do_flush_dir_stat(dir_stats).await;
        }
    }

    async fn cleanup_trash(&self, days: u8, force: bool) {
        todo!()
    }

    async fn cleanup_trash_before(&self, edge: Duration) {
        todo!()
    }

    async fn cleanup_trash_slices(&self, days: u8) {
        todo!()
    }
}

#[async_trait]
impl<E> Meta for E
where
    E: Send + Sync + 'static + Engine + AsRef<CommonMeta>,
{
    // Name of database
    fn name(&self) -> String {
        todo!()
    }

    // Get a copy of the current format
    fn get_format(&self) -> Arc<Format> {
        self.as_ref().fmt.read().clone()
    }

    // Init is used to initialize a meta service.
    async fn init(&self, format: Format, force: bool) -> Result<()> {
        self.do_init(format, force).await
    }

    // Shutdown close current database connections.
    async fn shutdown(&self) -> Result<()> {
        todo!()
    }

    // Reset cleans up all metadata, VERY DANGEROUS!
    async fn reset(&self) -> Result<()> {
        self.do_reset().await
    }

    // Load loads the existing setting of a formatted volume from meta service.
    async fn load(&self, check_version: bool) -> Result<Arc<Format>> {
        info!("Load format from meta service");
        let fmt = match self.do_load().await {
            Ok(None) => return NotInitializedSnafu.fail(),
            Ok(Some(body)) => body,
            Err(e) => return Err(e),
        };

        if check_version {
            fmt.check_version()?;
        }

        let mut guard = self.as_ref().fmt.write();
        *guard = Arc::new(fmt);
        Ok(guard.clone())
    }

    // NewSession creates or update client session.
    // TODO: 将部分需要new session修改的东西放在一个单独的结构体。还有比如在new session中启动的协程在close时进行关闭
    async fn new_session(self: Arc<Self>, persist: bool) -> Result<()> {
        let meta = self.clone();
        tokio::spawn(async {
            meta.refresh_session().await;
        });

        let base = self.as_ref().as_ref();
        self.cache_acls().await?;
        if base.conf.read_only {
            // TODO: add version detail
            info!("Create read-only session OK with version: ");
            return Ok(());
        }

        // use the original sid if it's not 0
        let (action, sid) = match base.conf.sid {
            Some(sid) => ("Update", sid),
            None => {
                let next_sid = self.incr_counter("nextSession", 1).await?;
                ("Create", next_sid as u64)
            }
        };
        {
            let mut sid_guard = base.sid.write();
            *sid_guard = Some(sid);
        }
        if persist {
            let session_info = base.new_session_info();
            self.clone()
                .do_new_session(
                    sid,
                    &bincode::serialize(&session_info).unwrap(),
                    action == "Update",
                )
                .await?;
            info!("{} session {} OK with version:", action, sid);
        }

        self.load_quotas().await;
        let meta = self.clone();
        // flush stats
        tokio::spawn(async move {
            meta.flush_stats().await;
        });
        let meta = self.clone();
        // flush dir stats
        tokio::spawn(async move {
            let period = meta.as_ref().as_ref().conf.dir_stat_flush_period;
            loop {
                sleep(period).await;
                meta.flush_dir_stat().await;
            }
        });
        // flush quota
        let meta = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(3)).await;
                meta.flush_quotas().await;
            }
        });

        if base.conf.enable_bg_job {
            // cleanup deleted files
            let meta = self.clone();
            tokio::spawn(async move {
                loop {
                    sleep_with_jitter(Duration::from_mins(1)).await;
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    match meta
                        .set_if_small("lastCleanupFiles", now, Duration::from_secs(54))
                        .await
                    {
                        Ok(true) => {
                            match meta
                                .do_find_deleted_files(now - Duration::from_hours(1), 10000)
                                .await
                            {
                                Ok(files) => {
                                    for (inode, length) in files {
                                        info!(
                                            "cleanup chunks of inode {} with {} bytes",
                                            inode, length
                                        );
                                        meta.do_delete_file_data(inode, length).await;
                                    }
                                }
                                Err(e) => warn!("scan deleted files: {}", e),
                            }
                        }
                        Ok(false) => (),
                        Err(e) => warn!("checking counter lastCleanupFiles: {}", e),
                    }
                }
            });
            // cleaup slices
            let meta = self.clone();
            tokio::spawn(async move {
                loop {
                    sleep_with_jitter(Duration::from_hours(1)).await;
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    match meta
                        .set_if_small("nextCleanupSlices", now, Duration::from_mins(54))
                        .await
                    {
                        Ok(true) => {
                            meta.do_cleanup_slices().await;
                        }
                        Ok(false) => (),
                        Err(e) => warn!("checking counter nextCleanupSlices: {}", e),
                    }
                }
            });
            // cleanup trash
            let meta = self.clone();
            tokio::spawn(async move {
                loop {
                    sleep_with_jitter(Duration::from_hours(1)).await;
                    if let Err(e) = meta.do_get_attr(TRASH_INODE).await {
                        if let MyError::SysError { code } = e
                            && code != libc::ENOENT
                        {
                            warn!("getattr inode {}: {}", TRASH_INODE, code);
                        }
                        continue;
                    }
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    match meta
                        .set_if_small("lastCleanupTrash", now, Duration::from_mins(54))
                        .await
                    {
                        Ok(true) => {
                            let days = meta.get_format().trash_days;
                            meta.cleanup_trash(days, false).await;
                            meta.cleanup_trash_slices(days).await;
                        }
                        Ok(false) => {}
                        Err(e) => warn!("checking counter lastCleanupTrash: {}", e),
                    }
                }
            });
        }
        Ok(())
    }

    // CloseSession does cleanup and close the session.
    async fn close_session(&self) -> Result<()> {
        let base = self.as_ref();
        if base.conf.read_only {
            return Ok(());
        }

        self.flush_dir_stat().await;
        self.flush_quotas().await;
        {
            let mut unmounting = base.ses_umounting.lock().await;
            *unmounting = true;
        }
        let sid = { base.sid.read().clone() };
        if let Some(sid) = sid {
            let res = self.do_clean_stale_session(sid).await;
            info!("close session {}: {:?}", sid, res);
            res
        } else {
            Ok(())
        }
    }

    // GetSession retrieves information of session with sid
    async fn get_session(&self, sid: u64, detail: bool) -> Result<Session> {
        todo!()
    }

    // ListSessions returns all client sessions.
    async fn list_sessions(&self) -> Result<Vec<Session>> {
        self.do_list_sessions().await
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

    // CleanStaleSessions cleans up sessions not active for edge times.
    async fn cleanup_stale_sessions(&self) {
        match self.do_find_stale_sessions(1000).await {
            Ok(sids) => {
                for sid in sids {
                    if let Ok(session) = self.get_session(sid, false).await {
                        warn!("Get stale session info {}: {:?}", sid, session);
                    }
                    info!(
                        "Clean up stale session {}: {:?}",
                        sid,
                        self.do_clean_stale_session(sid).await
                    );
                }
            }
            Err(e) => warn!("scan stale sessions: {}", e),
        }
    }

    // CleanupDetachedNodesBefore deletes all detached nodes before the given time.
    async fn cleanup_detached_nodes_before(
        &self,
        edge: SystemTime,
        incre_process: Box<dyn Fn(isize) + Send>,
    ) {
        todo!()
    }

    // GetPaths returns all paths of an inode
    async fn get_paths(&self, inode: Ino) -> Vec<String> {
        todo!()
    }

    // Check integrity of an absolute path and repair it if asked
    async fn check(
        &self,
        fpath: String,
        repair: bool,
        recursive: bool,
        stat_all: bool,
    ) -> Result<()> {
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
    async fn access(&self, inode: Ino, mode_mask: ModeMask, attr: &Attr) -> Result<()> {
        // dont check acl if mask is 0
        if attr.access_acl != acl::NONE && (attr.mode & 0o070) != 0 {
            let rule = self
                .do_get_facl(inode, AclType::Access, attr.access_acl)
                .await?;
            if rule.can_access(uid(), gids(), attr.uid, attr.gid, mode_mask) {
                return Ok(());
            }
            return SysSnafu { code: libc::EACCES }.fail();
        }

        let mode = access_mode(attr, uid(), gids());
        if mode & mode_mask != mode_mask {
            debug!(
                "Access inode {} {:o}, mode {:o}, request mode {:o}",
                inode, attr.mode, mode, mode_mask
            );
            SysSnafu { code: libc::EACCES }.fail()
        } else {
            Ok(())
        }
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
    async fn get_attr(&self, inode: Ino) -> Result<Attr> {
        let base = self.as_ref();
        let inode = inode.transfer_root(base.root);
        if let Some(attr) = base.open_files.get_attr(inode).await {
            return Ok(attr);
        }
        // for root and trash, maybe the attr is not persist, but we need it always return ok
        if inode.is_root() || inode.is_trash() {
            // do_get_attr could overwrite the `attr` after timeout
            if let Ok(Ok(attr)) = timeout(Duration::from_millis(300), async {
                self.do_get_attr(inode).await
            })
            .await
            {
                Ok(attr)
            } else {
                Ok(Attr {
                    typ: INodeType::TypeDirectory,
                    mode: if inode.is_trash() { 0o555 } else { 0o777 },
                    nlink: 2,
                    length: 4 << 10,
                    parent: ROOT_INODE,
                    full: true,
                    ..Attr::default()
                })
            }
        } else {
            let attr = self.do_get_attr(inode).await?;
            base.open_files.update_attr(inode, attr.clone()).await;
            if attr.typ == INodeType::TypeDirectory && inode.is_root() && !attr.parent.is_trash() {
                base.dir_parents.lock().insert(inode, attr.parent);
            }
            Ok(attr)
        }
    }

    // SetAttr updates the attributes for given node.
    async fn set_attr(
        &self,
        inode: Ino,
        set: u16,
        sggid_clear_mode: u8,
        attr: &Attr,
    ) -> Result<()> {
        todo!()
    }

    // Check setting attr is allowed or not
    async fn check_set_attr(&self, inode: Ino, set: u16, attr: &Attr) -> Result<()> {
        todo!()
    }

    // Truncate changes the length for given file.
    async fn truncate(
        self: Arc<Self>,
        inode: Ino,
        flags: u8,
        attr_length: u64,
        skip_perm_check: bool,
    ) -> Result<Attr> {
        if let Some(file) = self.as_ref().as_ref().open_files.find(inode) {
            let _ = file.lock().await;
        }

        let (attr, dir_stat) = self
            .do_truncate(inode, flags, attr_length, skip_perm_check)
            .await?;
        self.update_parent_stats(inode, attr.parent, dir_stat.length, dir_stat.space)
            .await;
        Ok(attr)
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
        name: &str,
        r#type: INodeType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: &str,
    ) -> Result<(Ino, Attr)> {
        if parent.is_trash() {
            return SysSnafu { code: libc::EPERM }.fail();
        }
        if parent == ROOT_INODE && name == TRASH_NAME {
            return SysSnafu { code: libc::EPERM }.fail();
        }
        if self.as_ref().conf.read_only {
            return SysSnafu { code: libc::EROFS }.fail();
        }
        if name.is_empty() {
            return SysSnafu { code: libc::ENOENT }.fail();
        }

        let parent = self.as_ref().check_root(parent);
        let (space, inodes) = (align_4k(0), 1);
        self.check_quotas(space, inodes, [parent].into()).await?;

        let mut attr = Attr::default();
        match r#type {
            INodeType::TypeDirectory => {
                attr.nlink = 2;
                attr.length = 4 << 10;
            }
            INodeType::TypeSymlink => {
                attr.nlink = 1;
                attr.length = path.len() as u64;
            }
            _ => {
                attr.nlink = 1;
                attr.length = 0;
                attr.rdev = rdev;
            }
        }
        attr.typ = r#type.clone();
        attr.uid = uid();
        attr.gid = gid();
        attr.parent = parent;
        attr.full = true;
        let ino = self.next_inode().await?;
        match self
            .do_mknod(parent, &name, mode, cumask, path, ino, attr)
            .await
        {
            Ok(attr) => {
                self.update_stats(space, inodes).await;
                self.update_dir_stats(parent, 0, space, inodes);
                self.update_quota(parent, space, inodes).await;
                Ok((ino, attr))
            }
            Err(e) => Err(e),
        }
    }

    // Mkdir creates a sub-directory with given name and mode.
    async fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        copysgid: u8,
    ) -> Result<(Ino, Attr)> {
        self.mknod(parent, name, INodeType::TypeDirectory, mode, cumask, 0, "").await
            .inspect(|(ino, _)| {
                let base = self.as_ref();
                let mut dir_parents = base.dir_parents.lock();
                dir_parents.insert(*ino, parent);
            })
    }

    // Unlink removes a file entry from a directory.
    // The file will be deleted if it's not linked by any entries and not open by any sessions.
    async fn unlink(
        self: Arc<Self>,
        parent: Ino,
        name: &str,
        skip_check_trash: bool,
    ) -> Result<()> {
        if (parent == ROOT_INODE && name == TRASH_NAME) || (parent.is_trash() && uid() != 0) {
            return SysSnafu { code: libc::EPERM }.fail();
        }

        let base = self.as_ref().as_ref();
        if base.conf.read_only {
            return SysSnafu { code: libc::EROFS }.fail();
        }

        let parent = parent.transfer_root(base.root);
        let attr = self
            .clone()
            .do_unlink(parent, name, skip_check_trash)
            .await?;
        let diff_length = if attr.typ == INodeType::TypeFile {
            attr.length
        } else {
            0
        };
        self.update_dir_stats(parent, -(diff_length as i64), -(align_4k(diff_length)), -1);
        self.update_quota(parent, -(align_4k(diff_length)), -1)
            .await;
        Ok(())
    }

    // Rmdir removes an empty sub-directory.
    async fn rmdir(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Ino> {
        match name {
            "." => return SysSnafu { code: libc::EINVAL }.fail(),
            ".." => return SysSnafu { code: libc::ENOTEMPTY }.fail(),
            _ => {},
        }
        if (parent.is_root() && name == TRASH_NAME) || (parent.is_trash() && uid() != 0) {
            return SysSnafu { code: libc::EPERM }.fail();
        }
        let base = self.as_ref();
        if base.conf.read_only {
            return SysSnafu { code: libc::EROFS }.fail();
        }
        
        let parent = parent.transfer_root(base.root);
        self.do_rmdir(parent, name, skip_check_trash).await
            .inspect(|ino| {
                if !parent.is_trash() {
                    let mut dir_parents = base.dir_parents.lock();
                    dir_parents.remove(ino);
                }
                self.update_dir_stats(parent, 0, -(align_4k(0)), -1);
                self.update_quota(parent, -(align_4k(0)), -1);
            })
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
    async fn readdir(&self, inode: Ino, wantattr: u8, entries: &mut Vec<Entry>) -> Result<()> {
        todo!()
    }

    // Create creates a file in a directory with given name.
    async fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: i32,
    ) -> Result<(Ino, Attr)> {
        match self
            .mknod(
                parent,
                name,
                INodeType::TypeFile,
                mode,
                cumask,
                0,
                "",
            )
            .await
        {
            Ok((ino, mut attr)) => {
                self.as_ref().open_files.open(ino, &mut attr).await;
                Ok((ino, attr))
            }
            Err(MyError::FileExistError { ino, mut attr })
                if (flags & libc::O_EXCL == 0 && attr.typ == INodeType::TypeFile) =>
            {
                self.as_ref().open_files.open(ino, &mut attr).await;
                Ok((ino, attr))
            }
            Err(e) => Err(e),
        }
    }

    // Open checks permission on a node and track it as open.
    async fn open(&self, inode: Ino, flags: i32, attr: &mut Attr) -> Result<()> {
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
    async fn new_slice(&self) -> Result<u64> {
        let mut slices = self.as_ref().free_slices.lock().await;
        if slices.next >= slices.maxid {
            let v = self.incr_counter("nextChunk", SLICE_ID_BATCH).await?;
            slices.next = (v - SLICE_ID_BATCH) as u64;
            slices.maxid = v as u64;
        }
        let next = slices.next;
        slices.next += 1;
        Ok(next)
    }

    // Write put a slice of data on top of the given chunk.
    async fn write(
        &self,
        inode: Ino,
        indx: u32,
        off: u32,
        slice: Slice,
        mtime: SystemTime,
    ) -> Result<()> {
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
        delete: bool,
        show_progress: Box<dyn Fn() + Send>,
    ) -> Result<HashMap<Ino, Vec<Slice>>> {
        todo!()
    }

    // Remove all files and directories recursively.
    // count represents the number of attempted deletions of entries (even if failed).
    async fn remove(&self, parent: Ino, name: &str, count: &mut u64) -> Result<()> {
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

    fn get_base(&self) -> &CommonMeta {
        self.as_ref()
    }
}
