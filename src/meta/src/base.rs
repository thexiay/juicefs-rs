use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use juice_utils::process::Bar;
use parking_lot::{Mutex, RwLock};
use scopeguard::defer;
use snafu::{ensure, ensure_whatever, whatever};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sysinfo::{get_current_pid, PidExt};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use crate::acl::{AclCache, AclExt, AclType, Rule};
use crate::api::{
    Attr, Entry, Falloc, Fcntl, Flag, INodeType, Ino, InoExt, Meta, ModeMask, OFlag, QuotaOp,
    RenameMask, Session, SessionInfo, SetAttrMask, Slice, StatFs, Summary, TreeSummary, XattrF,
    CHUNK_SIZE, MAX_VERSION, RESERVED_INODE, ROOT_INODE, TRASH_INODE, TRASH_NAME,
};
use crate::config::{Config, Format};
use crate::context::{UserExt, WithContext};
use crate::error::{
    BadFDSnafu, DirNotEmptySnafu, InterruptedSnafu, InvalidArgSnafu, MetaErrorEnum,
    NoEntryFound2Snafu, NoEntryFoundSnafu, NotDir2Snafu, NotInitializedSnafu, OpNotPermittedSnafu,
    OpNotSupportedSnafu, PermissionDeniedSnafu, QuotaExceededSnafu, ReadFSSnafu, Result,
    SetQuotaSnafu,
};
use crate::openfile::OpenFiles2;
use crate::quota::{MetaQuota, Quota, QuotaView};
use crate::slice::{MetaCompact, PSlices};
use crate::utils::{
    access_mode, align_4k, relation_need_update, sleep_with_jitter, DeleteFileOption, FLockItem,
    FreeID, PLockItem,
};

pub const INODE_BATCH: i64 = 1 << 10;
pub const SLICE_ID_BATCH: i64 = 1 << 10;
pub const N_LOCK: usize = 1 << 10;
pub const CHANNEL_BUFFER: usize = 1024;
const SEGMENT_LOCK: Mutex<()> = Mutex::new(());
const UMOUNT_EXIT_CODE: i32 = 11;
const MAX_SYMLINK: usize = 4096;
const MAX_SLICES: u32 = 2500;

pub const NEXT_INODE: &str = "nextinode";
pub const NEXT_CHUNK: &str = "nextchunk";
pub const NEXT_SESSION: &str = "nextsession";
pub const NEXT_TRASH: &str = "nexttrash";
pub const USED_SPACE: &str = "usedSpace";
pub const USED_INODES: &str = "usedInodes";

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
    pub(crate) new_space: AtomicI64,
    pub(crate) new_inodes: AtomicI64,
    pub(crate) used_space: AtomicI64,
    pub(crate) used_inodes: AtomicI64,
}

impl FsStat {
    pub fn update_used_stats(&self, space: i64, inodes: i64) {
        self.used_space.fetch_add(space, Ordering::SeqCst);
        self.used_inodes.fetch_add(inodes, Ordering::SeqCst);
    }
}

#[derive(Default, Clone)]
pub struct DirStat {
    // length of all files
    pub(crate) length: i64,
    // length of all files aligned
    pub(crate) space: i64,
    // number of inodes
    pub(crate) inodes: i64,
}

pub struct InternalNode {
    inode: Ino,
    name: String,
}

pub enum SetQuota {
    Capacity { space: u64, inodes: u64 },
    Force(QuotaView),
    Repair { space: u64, inodes: u64 },
}

#[async_trait]
pub trait Engine: WithContext + Send + Sync + 'static {
    // Get the value of counter name.
    async fn get_counter(&self, name: &str) -> Result<i64>;
    // Increase counter name by value. Do not use this if value is 0, use getCounter instead.
    async fn incr_counter(&self, name: &str, value: i64) -> Result<i64>;
    // Set counter name to value if old <= value - diff.
    async fn set_if_small(&self, name: &str, value: Duration, diff: Duration) -> Result<bool>;
    async fn update_stats(&self, space: i64, inodes: i64);
    async fn flush_stats(&self);
    async fn do_load(&self) -> Result<Option<Format>>;
    async fn do_new_session(&self, sid: u64, sinfo: &[u8], update: bool) -> Result<()>;
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
    async fn do_get_quota(&self, inode: Ino) -> Result<Option<QuotaView>>;
    // this function is dangerous, it will cause quota inconsistency
    async fn do_set_quota(&self, inode: Ino, set: SetQuota) -> Result<Option<(u64, u64)>>;
    async fn do_del_quota(&self, inode: Ino) -> Result<()>;
    async fn do_load_quotas(&self) -> Result<HashMap<Ino, QuotaView>>;
    async fn do_flush_quotas(&self, quotas: &HashMap<Ino, (i64, i64)>) -> Result<()>;
    // meta functions
    async fn do_get_attr(&self, inode: Ino) -> Result<Attr>;
    async fn do_set_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        sugidclearmode: u8,
        attr: &Attr,
    ) -> Result<Option<Attr>>;
    async fn do_exists(&self, inode: Ino) -> Result<bool>;
    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr)>;
    async fn do_resolve(&self, parent: Ino, path: &str) -> Result<(Ino, Attr)>;
    async fn do_mknod(
        &self,
        parent: Ino,
        name: &str,
        cumask: u16,
        path: &str,
        inode: Ino,
        attr: Attr,
    ) -> Result<Attr>;
    async fn do_link(&self, inode: Ino, parent: Ino, name: &str) -> Result<Attr>;
    async fn do_unlink(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Attr>;
    async fn do_rmdir(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Ino>;
    async fn do_read_symlink(&self, inode: Ino, no_atime: bool) -> Result<(Option<u128>, String)>;
    async fn do_readdir(
        &self,
        inode: Ino,
        wantattr: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Entry>>;
    async fn do_rename(
        &self,
        parent_src: Ino,
        name_src: &str,
        parent_dst: Ino,
        name_dst: &str,
        flags: RenameMask,
    ) -> Result<(Ino, Attr, Option<(Ino, Attr)>)>;
    async fn do_set_xattr(
        &self,
        inode: Ino,
        name: &str,
        value: Vec<u8>,
        flag: XattrF,
    ) -> Result<()>;
    async fn do_get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>>;
    async fn do_list_xattr(&self, inode: Ino) -> Result<Vec<u8>>;
    async fn do_remove_xattr(&self, inode: Ino, name: &str) -> Result<()>;
    async fn do_repair(&self, inode: Ino, attr: &mut Attr) -> Result<()>;
    async fn do_touch_atime(&self, inode: Ino, ts: Duration) -> Result<Attr>;
    async fn do_read(&self, inode: Ino, indx: u32) -> Result<PSlices>;
    async fn do_write(
        &self,
        inode: Ino,
        indx: u32,
        coff: u32,
        slice: Slice,
        mtime: DateTime<Utc>,
    ) -> Result<(u32, DirStat, Attr)>;

    // Returns (truncated attr, delta dirstat)
    async fn do_truncate(
        &self,
        inode: Ino,
        flags: u8,
        length: u64,
        skip_perm_check: bool,
    ) -> Result<(Attr, DirStat)>;

    async fn do_copy_file_range(
        &self,
        src: Ino,
        src_off: u64,
        dst: Ino,
        dst_off: u64,
        len: u64,
        flags: u32,
    ) -> Result<(u64, Option<(Ino, DirStat)>)>;

    async fn do_fallocate(
        &self,
        inode: Ino,
        flag: Falloc,
        off: u64,
        size: u64,
    ) -> Result<(DirStat, Attr)>;

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

    async fn do_list_slices(&self, delete: bool) -> Result<HashMap<Ino, Vec<Slice>>>;

    async fn do_get_parents(&self, inode: Ino) -> HashMap<Ino, u32>;
    /// do add dirstats batch periodly
    async fn do_flush_dir_stat(&self, batch: HashMap<Ino, DirStat>) -> Result<()>;
    // @trySync: try sync dir stat if broken or not existed
    async fn do_get_dir_stat(&self, ino: Ino, try_sync: bool) -> Result<Option<DirStat>>;
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
    /// An accumulator for the next inode to be allocated.
    async fn next_inode(&self) -> Result<Ino>;
    async fn lookup_ignore_ascii_case(&self, parent: Ino, name: &str) -> Result<Option<Entry>>;

    /// Add a temporary folder to keep removed files for a certain time
    /// If `rm`,`unlink`,`rm dir` E.g happens, they can be moved to `.trash` dir to keep some times.
    ///
    /// # Arguments
    ///
    /// * `parent` - the parent inode
    ///
    /// # Returns
    ///
    /// * it will return Some(new_trash_ino) if the entry under the parent should be moved to trash
    async fn check_trash(&self, parent: Ino) -> Result<Option<Ino>>;

    /// Immediately spawn a task to delete task
    ///
    /// # Arguments
    ///
    /// * `inode` - the inode to be delete  
    /// * `length` - the length this file shrink to
    /// * `delete_option` - the delete options, e.g. immerate or defer delete
    async fn try_spawn_delete_file(&self, inode: Ino, length: u64, delete_option: DeleteFileOption);

    /// ------------------------------ remove func ------------------------------
    /// Remove specific entry of name in dir recursivly
    /// Caller should make should the parent's name's Ino is inode
    ///
    /// # Arguments
    ///
    /// * `parent` - parent inode
    /// * `name` - entry name
    /// * `inode` - entry inode
    async fn remove_entry(
        &self,
        parent: Ino,
        name: &str,
        inode: Ino,
        skip_check_trash: bool,
        count: Arc<AtomicU64>,
        max_removing: Arc<Semaphore>,
    ) -> Result<()>;

    /// Remove all entry in dir, not include the dir inode itself
    /// Caller should make should the inode is dir.
    ///
    /// # Arguments
    ///
    /// * `inode` - dir inode
    async fn remove_dir(
        &self,
        inode: Ino,
        skip_check_trash: bool,
        count: Arc<AtomicU64>,
        max_removing: Arc<Semaphore>,
    ) -> Result<()>;

    async fn remove_slice(&self, id: u64, size: u32);

    /// ------------------------------ attr func ------------------------------
    fn clear_sugid(&self, cur: &mut Attr, set: &mut u16);

    /// Alter attr atime, it will use cache attr or persist attr to decide whether to update atime.
    ///
    /// caller makes sure inode is not special inode.
    async fn update_attr_atime(&self, ino: Ino);

    fn is_atime_need_update(&self, attr: &Attr, now: Duration) -> bool;

    /// merge a incoming attr into a current attr
    ///
    /// return a option attr represent mereged attr,
    /// - if some, represent need to update attr
    /// - if none, represent no need to update attr
    async fn merge_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        current: &Attr,
        incoming: &Attr,
        now: Duration,
        rule: &mut Option<Rule>,
    ) -> Result<Option<Attr>>;

    /// ------------------------------ session func ------------------------------
    /// do persist session info into meta persist layer periodly
    async fn refresh_session(&self);

    /// cleanup trash which have not been deleted for days
    async fn cleanup_trash(&self, days: u8, force: bool);

    /// cleanup trash which is earlier than edge
    async fn cleanup_trash_before(&self, edge: Duration);

    /// cleanup delayed deleted slices which have not been deleted for days
    async fn cleanup_trash_slices(&self, days: u8);
}

pub struct CommonMeta {
    pub addr: String,
    root: AtomicU64,
    pub current_trash_dir: Mutex<Option<InternalNode>>,
    pub removed_files: Mutex<HashMap<Ino, bool>>,
    pub compacting: HashMap<u64, bool>,
    /// The max amount of concurrency of tasks that delete files
    pub max_deleting_file: Arc<Semaphore>,
    /// Channel of to be delete slice(id, size)
    pub deleting_slice_ctx: Option<(Sender<(u64, u32)>, Receiver<(u64, u32)>)>,
    pub symlinks: Mutex<HashMap<Ino, (Option<u128>, String)>>, // ino -> (atime, path)
    pub reload_format_callbacks: Vec<Box<dyn Fn(Arc<Format>) + Send + Sync>>,
    pub acl_cache: AsyncMutex<AclCache>,
    pub dir_stats_batch: Mutex<HashMap<Ino, DirStat>>,
    pub fs_stat: FsStat,
    pub dir_parents: Mutex<HashMap<Ino, Ino>>, // directory inode -> parent inode
    // dir quotas, it storage every inode(dir type)'s quota
    pub dir_quotas: RwLock<HashMap<Ino, Quota>>,
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
    pub open_files: Arc<OpenFiles2>,
    // TODO: encapsulation it into Segmented Lock
    // Pessimistic locks to reduce conflicts
    pub txn_locks: [Mutex<()>; N_LOCK],
    pub canceled: AtomicBool,
}

impl CommonMeta {
    pub fn new(addr: &str, conf: Config) -> Self {
        let deleting_slice_ctx = if conf.max_deletes_task > 0 {
            let (tx, rx) = async_channel::bounded(conf.max_deletes_task as usize * 10 * 1024);
            Some((tx, rx))
        } else {
            None
        };
        let open_files = OpenFiles2::new(conf.open_cache, conf.open_cache_limit);
        CommonMeta {
            addr: addr.to_string(),
            root: AtomicU64::new(ROOT_INODE),
            current_trash_dir: Mutex::new(None),
            removed_files: Mutex::new(HashMap::new()),
            compacting: HashMap::new(),
            max_deleting_file: Arc::new(Semaphore::new(100)),
            deleting_slice_ctx,
            symlinks: Mutex::new(HashMap::new()),
            reload_format_callbacks: Vec::new(),
            ses_umounting: AsyncMutex::new(false),
            acl_cache: AsyncMutex::new(AclCache::default()),
            dir_stats_batch: Mutex::new(HashMap::new()),
            fs_stat: FsStat::default(),
            dir_parents: Mutex::new(HashMap::new()),
            dir_quotas: RwLock::new(HashMap::new()),
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

    // -------------- root func --------------
    pub fn check_root(&self, ino: Ino) -> Ino {
        match ino {
            RESERVED_INODE => ROOT_INODE,
            ROOT_INODE => self.root(),
            _ => ino,
        }
    }

    pub fn root(&self) -> Ino {
        self.root.load(Ordering::SeqCst)
    }

    pub fn chroot(&self, ino: Ino) {
        self.root.store(ino, Ordering::SeqCst);
    }

    pub fn get_format(&self) -> Arc<Format> {
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
    E: Engine + AsRef<CommonMeta>,
{
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

    async fn lookup_ignore_ascii_case(&self, parent: Ino, name: &str) -> Result<Option<Entry>> {
        let entries = self.do_readdir(parent, false, None).await?;
        Ok(entries
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case(&name)))
    }

    async fn check_trash(&self, parent: Ino) -> Result<Option<Ino>> {
        let no_need_trash = parent.is_trash();
        let trash_enable = self.as_ref().get_format().trash_days > 0;
        if no_need_trash || !trash_enable {
            return Ok(None);
        }
        // use current trash dir as cache
        let name = Utc::now().format("%Y-%m-%d-%H").to_string();
        {
            let sub_trash = self.as_ref().current_trash_dir.lock();
            if let Some(trash) = &*sub_trash
                && trash.name == name
            {
                return Ok(Some(trash.inode));
            }
        }

        // all trash file in one trash directory: `%Y-%m-%d-%H`
        // Create if trash dir not exists.
        match self.do_lookup(TRASH_INODE, &name).await {
            Ok((trash_ino, _)) => Ok(Some(trash_ino)),
            Err(e) => {
                if let MetaErrorEnum::NoEntryFound {
                    parent: TRASH_INODE,
                    name: no_name,
                } = e.inner()
                    && name == *no_name
                {
                    // create new trash dir
                    let next = self.incr_counter(NEXT_TRASH, 1).await?;
                    let trash_ino = TRASH_INODE + next as u64;
                    if trash_ino < TRASH_INODE {
                        return BadFDSnafu { fd: trash_ino }.fail()?;
                    }

                    match self
                        .do_mknod(
                            TRASH_INODE,
                            &name,
                            0,
                            "",
                            trash_ino,
                            Attr {
                                typ: INodeType::Directory,
                                nlink: 2,
                                length: 4 << 10,
                                mode: 0o555,
                                parent: TRASH_INODE,
                                full: true,
                                ..Attr::default()
                            },
                        )
                        .await
                    {
                        Ok(_) => {
                            info!("Created trash dir {}", name);
                            let mut current_trash_dir = self.as_ref().current_trash_dir.lock();
                            current_trash_dir.replace(InternalNode {
                                inode: trash_ino,
                                name,
                            });
                            Ok(Some(trash_ino))
                        }
                        Err(err) => {
                            if let MetaErrorEnum::EntryExists {
                                parent: TRASH_INODE,
                                name: no_name,
                                ..
                            } = err.inner()
                                && *no_name == name
                            {
                                warn!(
                                    "Found trash dir {} already exists, maybe create by other user",
                                    name
                                );
                                Ok(None)
                            } else {
                                Err(err)
                            }
                        }
                    }
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn try_spawn_delete_file(
        &self,
        inode: Ino,
        length: u64,
        delete_option: DeleteFileOption,
    ) {
        let base = self.as_ref();
        match delete_option {
            DeleteFileOption::Deferred => {
                let mut remove_files = base.removed_files.lock();
                remove_files.insert(inode, true);
            }
            DeleteFileOption::Immediate { force } => {
                let mut meta = dyn_clone::clone_box(self);
                meta.with_cancel(self.token().child_token());
                let max_deleting = base.max_deleting_file.clone();
                tokio::spawn(async move {
                    let permit = match force {
                        // TODO: 这里如果拿不到被关闭了，是不是要处理下
                        true => Some(max_deleting.acquire().await.unwrap()),
                        false => max_deleting
                            .try_acquire()
                            .inspect_err(|err| {
                                warn!("Have no resource to delete inode({}), {}. It may can't delete it immediate.", inode, err);
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
    }

    // ------------------------------ remove func ------------------------------
    async fn remove_entry(
        &self,
        parent: Ino,
        name: &str,
        inode: Ino,
        skip_check_trash: bool,
        count: Arc<AtomicU64>,
        max_removing: Arc<Semaphore>,
    ) -> Result<()> {
        info!("remove dir {inode} start");
        self.remove_dir(inode, skip_check_trash, count.clone(), max_removing.clone())
            .await?;
        info!("remove dir {inode} end");
        if !inode.is_trash() {
            info!("rmdir {parent} {name} start");
            match self.rmdir(parent, name, skip_check_trash).await {
                Err(err) => {
                    if err.is_dir_not_empty(&parent, name) {
                        // redo when concurrent conflict may happen
                        self.remove_entry(
                            parent,
                            name,
                            inode,
                            skip_check_trash,
                            count,
                            max_removing.clone(),
                        )
                        .await?;
                    } else {
                        warn!(parent = %parent, name = %name, "remove entry failed: {}", err);
                    }
                }
                _ => {
                    info!("rmdir {parent} {name} end");
                    count.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    async fn remove_dir(
        &self,
        inode: Ino,
        skip_check_trash: bool,
        count: Arc<AtomicU64>,
        max_removing: Arc<Semaphore>,
    ) -> Result<()> {
        loop {
            // readdir把自己读进去了？不应该readdir包含自己
            let mut entries = self.do_readdir(inode, false, Some(1_0000)).await?;
            info!(
                "the read dir result of {inode} is {:?}",
                entries.iter().map(|e| e.inode).collect::<Vec<_>>()
            );
            if entries.is_empty() {
                return Ok(());
            }
            match self
                .access(
                    inode,
                    ModeMask::WRITE.union(ModeMask::EXECUTE),
                    &Attr::default(),
                )
                .await
            {
                Ok(_) => (),
                Err(err) => {
                    if let MetaErrorEnum::NoEntryFound { parent, name } = err.inner() {
                        warn!(
                            "entry({},{}) not found when remove, maybe other thread remove it",
                            parent, name
                        );
                    } else {
                        return Err(err);
                    }
                }
            }

            // try directories first to increase parallel
            let mut dirs = 0;
            for i in 0..entries.len() {
                if entries[i].attr.typ == INodeType::Directory {
                    entries.swap(dirs, i);
                    dirs += 1;
                }
            }
            let mut join_set = JoinSet::new();
            for entry in entries {
                if entry.attr.typ == INodeType::Directory {
                    let removing_limit = max_removing.clone();
                    match removing_limit.try_acquire_owned() {
                        Ok(permit) => {
                            let mut meta = dyn_clone::clone_box(self);
                            meta.with_cancel(self.token().child_token());
                            let max_removing = max_removing.clone();
                            let count = count.clone();
                            // 这里如果多线程进入，会导致多个同时去删除，会抢占元素？
                            join_set.spawn(async move {
                                let res = meta
                                    .remove_entry(
                                        inode,
                                        &entry.name,
                                        entry.inode,
                                        skip_check_trash,
                                        count,
                                        max_removing,
                                    )
                                    .await;
                                drop(permit);
                                res
                            });
                        }
                        Err(_) => {
                            match self
                                .remove_entry(
                                    inode,
                                    &entry.name,
                                    entry.inode,
                                    skip_check_trash,
                                    count.clone(),
                                    max_removing.clone(),
                                )
                                .await
                            {
                                Ok(_) => (),
                                Err(err) => {
                                    if err.is_no_entry_found(&inode, &entry.name) {
                                        warn!(
                                            parent = %inode,
                                            name = %entry.name,
                                            "entry not found when remove, maybe other thread unlink it",
                                        )
                                    } else {
                                        self.token().cancel();
                                        return Err(err);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    count.fetch_add(1, Ordering::SeqCst);
                    match self.unlink(inode, &entry.name, skip_check_trash).await {
                        Ok(_) => (),
                        Err(err) => {
                            if err.is_no_entry_found(&inode, &entry.name) {
                                warn!(
                                    parent = %inode,
                                    name = %entry.name,
                                    "entry not found when remove, maybe other thread unlink it",
                                )
                            } else {
                                self.token().cancel();
                                return Err(err);
                            }
                        }
                    }
                }
                if self.token().is_cancelled() {
                    return InterruptedSnafu.fail()?;
                }
            }
            while let Some(join_res) = join_set.join_next().await {
                match join_res {
                    Ok(remove_res) => match remove_res {
                        Ok(_) => (),
                        Err(err) => {
                            if let MetaErrorEnum::NoEntryFound { parent, name } = err.inner() {
                                warn!(
                                    parent = %parent,
                                    name = %name,
                                    "entry not found when remove, maybe other thread unlink it",
                                );
                            } else {
                                return Err(err);
                            }
                        }
                    },
                    Err(err) => {
                        error!("remove entry coroutine failed: {}", err);
                    }
                }
            }
            // try only once for .trash
            if inode == TRASH_INODE {
                return Ok(());
            }
        }
    }

    async fn remove_slice(&self, id: u64, size: u32) {
        if let Some(ref deleting_slices) = self.as_ref().deleting_slice_ctx
            && id != 0
        {
            let _ = deleting_slices.0.send((id, size)).await.inspect_err(|e| {
                warn!(
                    "Failed to send deleting slice({}) to deleting channel: {}",
                    id, e
                );
            });
        }
    }

    // ------------------------------ attr func ------------------------------
    fn clear_sugid(&self, cur: &mut Attr, set: &mut u16) {
        // TODO: support darwin
        if cur.typ != INodeType::Directory {
            if !self.uid().is_root() || (cur.mode >> 3) & 1 != 0 {
                // clear SUID and SGID
                cur.mode &= 0o1777;
                *set &= 0o1777;
            } else {
                // keep SGID if the file is non-group-executable
                cur.mode &= 0o3777;
                *set &= 0o3777;
            }
        }
    }

    async fn update_attr_atime(&self, ino: Ino) {
        let base = self.as_ref();
        if base.conf.atime_mode == "NoAtime" || base.conf.read_only {
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        if let Some(attr) = base.open_files.attr(ino)
            && attr.full
            && !self.is_atime_need_update(&attr, now)
        {
            return;
        }

        // update persist layer atime
        match self.do_touch_atime(ino, now).await {
            Ok(attr) => {
                base.open_files.cache_attr(ino, &attr);
            }
            Err(e) => {
                warn!("Update atime of inode {} failed: {}", ino, e);
            }
        }
    }

    fn is_atime_need_update(&self, attr: &Attr, now: Duration) -> bool {
        let base = self.as_ref();
        // update atime only for > 1 second accesses
        (base.conf.atime_mode != "NoAtime" && relation_need_update(attr, now))
            || (base.conf.atime_mode == "StrictAtime"
                && now.as_nanos() - attr.atime > Duration::from_secs(1).as_nanos())
    }

    async fn merge_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        current: &Attr,
        incoming: &Attr,
        now: Duration,
        rule: &mut Option<Rule>,
    ) -> Result<Option<Attr>> {
        let mut dirty_attr = current.clone();
        let mut mode = incoming.mode;
        if set.intersects(SetAttrMask::SET_UID.union(SetAttrMask::SET_GID))
            && set.intersects(SetAttrMask::SET_MODE)
        {
            mode |= current.mode & 0o6000;
        }
        let mut changed = false;
        if current.mode & 0o6000 != 0
            && set.intersects(SetAttrMask::SET_UID.union(SetAttrMask::SET_GID))
        {
            // clear suid/sgid
            self.clear_sugid(&mut dirty_attr, &mut mode);
            changed = true;
        }
        if set.intersects(SetAttrMask::SET_GID) {
            if !self.uid().is_root() && *self.uid() != current.uid {
                return OpNotPermittedSnafu {
                    op: format!(
                        "SET_GID not permiited if not root and not inode({}) owner",
                        inode
                    ),
                }
                .fail()?;
            }
            let contains_gid = self.gids().iter().any(|gid| *gid == incoming.gid);
            if current.gid != incoming.gid {
                if self.check_permission() && !self.uid().is_root() && !contains_gid {
                    return OpNotPermittedSnafu {
                        op: format!(
                            "SET_GID not permiited if not root and not in inode({}) group",
                            incoming.gid
                        ),
                    }
                    .fail()?;
                }
                dirty_attr.gid = incoming.gid;
                changed = true;
            }
        }
        if set.intersects(SetAttrMask::SET_UID) && current.uid != incoming.uid {
            if !self.uid().is_root() {
                return OpNotPermittedSnafu {
                    op: format!(
                        "SET_UID not permiited if not root and not inode({}) owner",
                        inode
                    ),
                }
                .fail()?;
            }
            dirty_attr.uid = incoming.uid;
            changed = true;
        }
        if set.intersects(SetAttrMask::SET_MODE) {
            if !self.uid().is_root() && (mode & 0o2000) != 0 {
                if *self.gid() != current.gid {
                    mode &= 0o5777;
                }
            }
            if let Some(rule) = rule {
                rule.set_mode(mode);
                dirty_attr.mode = mode & 0o7000 | rule.get_mode();
                changed = true;
            } else if mode != current.mode {
                if !self.uid().is_root()
                    && *self.uid() != current.uid
                    && (current.mode & 0o1777 != mode & 0o1777
                        || mode & 0o2000 > current.mode & 0o2000
                        || mode & 0o4000 > current.mode & 0o4000)
                {
                    return OpNotPermittedSnafu {
                        op: format!("SET_MODE not permiited for inode({})", inode),
                    }
                    .fail()?;
                }
                dirty_attr.mode = mode;
                changed = true;
            }
        }
        if set.intersects(SetAttrMask::SET_ATIME_NOW)
            || (set.intersects(SetAttrMask::SET_ATIME) && incoming.atime == 0)
        {
            if self.access(inode, ModeMask::WRITE, current).await.is_err()
                && *self.uid() != current.uid
            {
                return PermissionDeniedSnafu { ino: inode }.fail()?;
            }
            dirty_attr.atime = now.as_nanos();
            changed = true;
        } else if set.intersects(SetAttrMask::SET_ATIME) && current.atime != incoming.atime {
            if current.uid.is_root() && !self.uid().is_root() {
                return OpNotPermittedSnafu {
                    op: format!("SET_ATIME not permiited for inode({})", inode),
                }
                .fail()?;
            }
            if self.access(inode, ModeMask::WRITE, current).await.is_err()
                && *self.uid() != current.uid
            {
                return PermissionDeniedSnafu { ino: inode }.fail()?;
            }
            dirty_attr.atime = incoming.atime;
            changed = true;
        }
        if set.intersects(SetAttrMask::SET_MTIME_NOW)
            || (set.intersects(SetAttrMask::SET_MTIME) && incoming.mtime == 0)
        {
            if self.access(inode, ModeMask::WRITE, current).await.is_err()
                && *self.uid() != current.uid
            {
                return PermissionDeniedSnafu { ino: inode }.fail()?;
            }
            dirty_attr.mtime = now.as_nanos();
            changed = true;
        } else if set.intersects(SetAttrMask::SET_MTIME) && current.mtime != incoming.mtime {
            if current.uid.is_root() && !self.uid().is_root() {
                return OpNotPermittedSnafu {
                    op: format!("SET_MTIME not permiited for inode({})", inode),
                }
                .fail()?;
            }
            if self.access(inode, ModeMask::WRITE, current).await.is_err()
                && *self.uid() != current.uid
            {
                return PermissionDeniedSnafu { ino: inode }.fail()?;
            }
            dirty_attr.mtime = incoming.mtime;
            changed = true;
        }
        if set.contains(SetAttrMask::SET_FLAG) {
            dirty_attr.flags = incoming.flags;
            changed = true;
        }
        if !changed {
            Ok(None)
        } else {
            Ok(Some(dirty_attr))
        }

        /*
        if set&SetAttrMtimeNow != 0 || (set&SetAttrMtime) != 0 && attr.Mtime < 0 {
            if st := m.Access(ctx, inode, MODE_MASK_W, cur); ctx.Uid() != cur.Uid && st != 0 {
                return nil, syscall.EACCES
            }
            dirtyAttr.Mtime = now.Unix()
            dirtyAttr.Mtimensec = uint32(now.Nanosecond())
            changed = true
        } else if set&SetAttrMtime != 0 && (cur.Mtime != attr.Mtime || cur.Mtimensec != attr.Mtimensec) {
            if cur.Uid == 0 && ctx.Uid() != 0 {
                return nil, syscall.EPERM
            }
            if st := m.Access(ctx, inode, MODE_MASK_W, cur); ctx.Uid() != cur.Uid && st != 0 {
                return nil, syscall.EACCES
            }
            dirtyAttr.Mtime = attr.Mtime
            dirtyAttr.Mtimensec = attr.Mtimensec
            changed = true
        }
        if set&SetAttrFlag != 0 {
            dirtyAttr.Flags = attr.Flags
            changed = true
        }
        if !changed {
            *attr = *cur
            return nil, 0
        }
        return &dirtyAttr, 0
             */
    }

    async fn refresh_session(&self) {
        loop {
            let base = self.as_ref();
            sleep_with_jitter(base.conf.heartbeat).await;
            {
                let unmounting = self.as_ref().ses_umounting.lock().await;
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
                Err(err) => {
                    if let MetaErrorEnum::NotInitializedError = err.inner() {
                        error!("please reload setting: {}", err);
                        process::exit(UMOUNT_EXIT_CODE);
                    } else {
                        warn!("reload setting: {}", err);
                    }
                }
            }

            match self.get_counter(USED_SPACE).await {
                Ok(v) => base.fs_stat.used_space.store(v, Ordering::SeqCst),
                Err(e) => warn!("Get counter {}: {}", USED_SPACE, e),
            }
            match self.get_counter(USED_INODES).await {
                Ok(v) => base.fs_stat.used_inodes.store(v, Ordering::SeqCst),
                Err(e) => warn!("Get counter {}: {}", USED_INODES, e),
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
                    let mut meta = dyn_clone::clone_box(self);
                    meta.with_cancel(self.token().child_token());
                    tokio::spawn(async move {
                        meta.cleanup_stale_sessions().await;
                    });
                }
                Err(e) => warn!("checking counter lastCleanupSessions: {}", e),
                _ => {}
            }
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
    E: Engine + AsRef<CommonMeta>,
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
            Ok(None) => return NotInitializedSnafu.fail()?,
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
    async fn new_session(&self, persist: bool) -> Result<()> {
        let mut meta = dyn_clone::clone_box(self);
        meta.with_cancel(self.token().child_token());
        tokio::spawn(async move {
            meta.refresh_session().await;
        });

        let base = self.as_ref();
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
            self.do_new_session(
                sid,
                &bincode::serialize(&session_info).unwrap(),
                action == "Update",
            )
            .await?;
            info!("{} session {} OK with version:", action, sid);
        }

        self.load_quotas().await;
        // flush stats
        let mut meta = dyn_clone::clone_box(self);
        meta.with_cancel(self.token().child_token());
        tokio::spawn(async move {
            meta.flush_stats().await;
        });
        // flush dir stats
        let mut meta = dyn_clone::clone_box(self);
        meta.with_cancel(self.token().child_token());
        tokio::spawn(async move {
            let period = meta.as_ref().as_ref().conf.dir_stat_flush_period;
            loop {
                sleep(period).await;
                meta.flush_dir_stat().await;
            }
        });
        // flush quota
        let mut meta = dyn_clone::clone_box(self);
        meta.with_cancel(self.token().child_token());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(3)).await;
                meta.flush_quotas().await;
            }
        });

        // ---------------------------------------- delete slice job ------------------------------------------
        if let Some(ref ctx) = base.deleting_slice_ctx {
            for _ in 0..base.conf.max_deletes_task {
                let mut meta = dyn_clone::clone_box(self);
                meta.with_cancel(self.token().child_token());
                let rx = ctx.1.clone();
                let m = dyn_clone::clone_box(self);
                tokio::spawn(async move {
                    while let Ok((id, size)) = rx.recv().await {
                        if let Err(e) = m.do_delete_slice(id, size).await {
                            error!("Delete meta entry of slice {id} ({size} bytes): {e}");
                        }
                    }
                });
            }
        }

        // ----------------------------------------- background job --------------------------------------------
        if base.conf.enable_bg_job {
            // cleanup deleted files
            let mut meta = dyn_clone::clone_box(self);
            meta.with_cancel(self.token().child_token());
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
            let mut meta = dyn_clone::clone_box(self);
            meta.with_cancel(self.token().child_token());
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
            let mut meta = dyn_clone::clone_box(self);
            meta.with_cancel(self.token().child_token());
            tokio::spawn(async move {
                loop {
                    sleep_with_jitter(Duration::from_hours(1)).await;
                    if let Err(e) = meta.do_get_attr(TRASH_INODE).await {
                        if matches!(e.inner(), MetaErrorEnum::NoEntryFound { .. }) {
                            warn!("get trash attr inode error {e}");
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
        self.do_get_session(sid, detail).await
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
        if inode.is_root() || inode == self.as_ref().root() {
            return vec!["/".to_string()];
        }
        if inode.is_trash() {
            return vec!["/.trash".to_string()];
        }

        const OUTSIDE: &str = "path not shown because it's outside of the mounted root";
        async fn get_dir_path(
            meta: &(impl Engine + AsRef<CommonMeta>),
            mut ino: Ino,
        ) -> std::result::Result<String, String> {
            let mut names = vec![];
            while !ino.is_root() && ino != meta.as_ref().root() {
                let attr = meta
                    .do_get_attr(ino)
                    .await
                    .map_err(|err| format!("getattr inode {}: {}", ino, err))?;
                if attr.typ != INodeType::Directory {
                    return Err(format!("inode {} is not a directory", ino));
                }
                let entries = meta
                    .do_readdir(attr.parent, false, None)
                    .await
                    .map_err(|err| format!("readdir inode {}: {}", ino, err))?;
                let name = None
                    .or_else(|| {
                        if attr.parent.is_root() && ino.is_trash() {
                            Some(TRASH_NAME.to_string())
                        } else {
                            None
                        }
                    })
                    .or_else(|| entries.into_iter().find(|e| e.inode == ino).map(|e| e.name))
                    .ok_or_else(|| format!("entry {} not found", ino))?;
                names.push(name);
                ino = attr.parent;
            }

            if !meta.as_ref().root().is_root() && ino == ROOT_INODE {
                return Err(OUTSIDE.to_string());
            }
            names.reverse();
            if names.is_empty() {
                Ok("/".to_string())
            } else {
                Ok(format!("/{}/", names.join("/")))
            }
        }

        let mut paths = vec![];
        // inode != RootInode, parent is the real parent inode
        for (parent, count) in self.get_parents(inode).await {
            if count <= 0 {
                continue;
            }
            let dir = match get_dir_path(self, parent).await {
                Ok(dir) => dir,
                Err(err) => {
                    if err == OUTSIDE {
                        paths.push(OUTSIDE.to_string());
                    } else {
                        warn!("Get directory path of {}: {}", parent, err);
                    }
                    continue;
                }
            };
            let entries = match self.do_readdir(parent, false, None).await {
                Ok(entries) => entries,
                Err(err) => {
                    warn!("Readdir inode {}: {}", parent, err);
                    continue;
                }
            };
            let mut c = 0;
            for e in entries {
                if e.inode == inode {
                    c += 1;
                    paths.push(format!("{}{}", dir, e.name));
                }
            }
            if c != count {
                warn!(
                    "Expect to find {} entries under parent {}, but got {}",
                    count, parent, c
                );
            }
        }
        paths
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
        op: QuotaOp,
        dpath: &str,
        strict: bool,
        repair: bool,
    ) -> Result<HashMap<String, QuotaView>> {
        // TODO: check only volumn mount point can handle quota
        let ino = if op != QuotaOp::List {
            let (ino, attr) = self.resolve(ROOT_INODE, dpath, true).await?;
            ensure!(attr.typ == INodeType::Directory, NotDir2Snafu { ino });
            if ino.is_trash() {
                whatever!("No quota for any trash directory");
            }
            Some(ino)
        } else {
            None
        };

        match op {
            QuotaOp::Set(quota) => {
                let inode = ino.unwrap();
                let format = self.get_format();
                if !format.enable_dir_stats {
                    let mut format = self.get_format().as_ref().clone();
                    format.enable_dir_stats = true;
                    self.do_init(format, false).await?;
                }
                let origin_capacity = self
                    .do_set_quota(
                        inode,
                        SetQuota::Capacity {
                            space: quota.max_space,
                            inodes: quota.max_inodes,
                        },
                    )
                    .await?;
                if origin_capacity.is_none() {
                    // new quota need to update used space and used inodes
                    let sum = self.get_summary(inode, true, strict).await.map_err(|err| {
                        error!(
                            "set quota usage for file({}), please repair it later: {}",
                            dpath, err
                        );
                        SetQuotaSnafu {
                            file: dpath.to_string(),
                        }
                        .build()
                    })?;
                    self.do_set_quota(
                        inode,
                        SetQuota::Repair {
                            space: sum.size - align_4k(0) as u64,
                            inodes: sum.dirs + sum.files - 1,
                        },
                    )
                    .await
                    .map_err(|err| {
                        error!(
                            "set quota usage for file({}), please repair it later: {}",
                            dpath, err
                        );
                        SetQuotaSnafu {
                            file: dpath.to_string(),
                        }
                        .build()
                    })?;
                }
                Ok(HashMap::new())
            }
            QuotaOp::Get => {
                let inode = ino.unwrap();
                let quota = self.do_get_quota(inode).await?;
                ensure_whatever!(quota.is_some(), "no quota for inode {inode}");
                let mut quotas = HashMap::new();
                quotas.insert(dpath.to_string(), quota.unwrap());
                Ok(quotas)
            }
            QuotaOp::Del => {
                let inode = ino.unwrap();
                self.do_del_quota(inode).await?;
                Ok(HashMap::new())
            }
            QuotaOp::List => {
                let quota_map = self.do_load_quotas().await?;
                let mut quotas = HashMap::new();
                for (ino, quota) in quota_map {
                    let p = self.get_paths(ino).await;
                    let p = if p.is_empty() {
                        format!("inode:{}", ino)
                    } else {
                        p[0].clone()
                    };
                    quotas.insert(p, quota);
                }
                Ok(quotas)
            }
            QuotaOp::Check => {
                let ino = ino.unwrap();
                let quota = self.do_get_quota(ino).await?;
                ensure_whatever!(quota.is_some(), "no quota for inode {ino}");
                let mut quotas = HashMap::new();
                let sum = self.get_summary(ino, true, strict).await?;
                let used_inodes = sum.dirs + sum.files - 1;
                let used_space = sum.size - align_4k(0) as u64;
                let mut quota = quota.unwrap();
                if quota.used_inodes == used_inodes && quota.used_space == used_space {
                    info!("quota of {} is consistent", dpath);
                    quotas.insert(dpath.to_string(), quota);
                    return Ok(quotas);
                }
                warn!(
                    "{}: quota({} {}) != summary({} {})",
                    dpath,
                    quota.used_inodes,
                    quota.used_space as u64,
                    used_inodes,
                    used_space as u64,
                );
                if repair {
                    quota.used_inodes = used_inodes;
                    quota.used_space = used_space;
                    quotas.insert(dpath.to_string(), quota.clone());
                    info!("repairing...");
                    self.do_set_quota(
                        ino,
                        SetQuota::Repair {
                            space: used_space,
                            inodes: used_inodes,
                        },
                    )
                    .await?;
                    Ok(quotas)
                } else {
                    whatever!(
                        "quota of {dpath} is inconsistent, please repair it with --repair flag"
                    );
                }
            }
        }
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
    async fn stat_fs(&self, ino: Ino) -> Result<StatFs> {
        let mut stat_fs = self.stat_root_fs().await;
        let ino = self.as_ref().check_root(ino);
        if ino.is_root() {
            return Ok(stat_fs.clone());
        }

        // Not rootfs, fallback to dir ino
        self.access(
            ino,
            ModeMask::READ.union(ModeMask::EXECUTE),
            &Attr::default(),
        )
        .await?;
        let mut inode = ino;
        let mut usage = None;
        // `space_avail` and `i_avail` is the minium of all path chain(/ROOT/.../ino)
        while inode >= ROOT_INODE {
            let attr = self.get_attr(inode).await?;
            if inode == ROOT_INODE {
                break;
            }
            if let Some(mut quota) = self.do_get_quota(inode).await? {
                quota.sanitize();
                if usage.is_none() {
                    // usage is the first avaliable quota of inode path chain
                    info!(inode = %inode, "Get quota: {quota:?} for statfs {ino}");
                    usage = Some(quota.clone());
                }
                if quota.max_space > 0 {
                    let ls = (quota.max_space - quota.used_space) as u64;
                    if ls < stat_fs.space_avail {
                        stat_fs.space_avail = ls; // minium avail space of all root chain
                    }
                }
                if quota.max_inodes > 0 {
                    let li = (quota.max_inodes - quota.used_inodes) as u64;
                    if li < stat_fs.i_avail {
                        stat_fs.i_avail = li; // minium avail inodes of all root chain
                    }
                }
            }
            inode = attr.parent;
        }
        if let Some(usage) = usage {
            stat_fs.space_total = usage.used_space + stat_fs.space_avail;
            stat_fs.i_used = usage.used_inodes;
        }
        Ok(stat_fs)
    }

    // Access checks the access permission on given inode.
    async fn access(&self, inode: Ino, mode_mask: ModeMask, attr: &Attr) -> Result<()> {
        if self.uid().is_root() || !self.check_permission() {
            return Ok(());
        }
        let attr = if !attr.full {
            self.get_attr(inode).await?
        } else {
            attr.clone()
        };
        // ref: https://github.com/torvalds/linux/blob/e5eb28f6d1afebed4bb7d740a797d0390bd3a357/fs/namei.c#L352-L357
        // dont check acl if mask is 0
        if attr.access_acl.is_valid_acl() && (attr.mode & 0o070) != 0 {
            let rule = self
                .do_get_facl(inode, AclType::Access, attr.access_acl)
                .await?;
            if rule.can_access(self.uid(), self.gids(), attr.uid, attr.gid, mode_mask) {
                return Ok(());
            }
            return PermissionDeniedSnafu { ino: inode }.fail()?;
        }

        let mode = access_mode(&attr, self.uid(), self.gids());
        if mode & mode_mask != mode_mask {
            debug!(
                "Access inode {}, attr mode {:o}, access mode {:o}, request mode {:o}",
                inode, attr.mode, mode, mode_mask
            );
            return PermissionDeniedSnafu { ino: inode }.fail()?;
        } else {
            Ok(())
        }
    }

    // Lookup returns the inode and attributes for the given entry in a directory.
    async fn lookup(&self, parent: Ino, name: &str, check_permission: bool) -> Result<(Ino, Attr)> {
        let base = self.as_ref();
        let parent = base.check_root(parent);
        if check_permission {
            let par_attr = self.get_attr(parent).await?;
            self.access(parent, ModeMask::EXECUTE, &par_attr).await?;
        }
        let name = match name {
            ".." if parent == base.root() => ".",
            other => other,
        };
        match name {
            ".." => {
                let par_attr = self.get_attr(parent).await?;
                if par_attr.typ != INodeType::Directory {
                    NotDir2Snafu { ino: parent }.fail()?
                } else {
                    Ok(((par_attr.parent), self.get_attr(par_attr.parent).await?))
                }
            }
            "." => Ok((parent, self.get_attr(parent).await?)),
            _ => {
                if parent.is_root() && name == TRASH_NAME {
                    Ok((TRASH_INODE, self.get_attr(TRASH_INODE).await?))
                } else {
                    let (inode, attr) = match self.do_lookup(parent, name).await {
                        Ok((inode, attr)) => (inode, attr),
                        Err(err) => {
                            // fallback to find case insensitive
                            if err.is_no_entry_found(&parent, name)
                                && base.conf.case_insensi
                                && let Ok(Some(entry)) =
                                    self.lookup_ignore_ascii_case(parent, name).await
                            {
                                match self.get_attr(entry.inode).await {
                                    Ok(attr) => (entry.inode, attr),
                                    Err(err) => {
                                        if err.is_no_entry_found2(&entry.inode) {
                                            warn!(
                                                inode = %entry.inode,
                                                %parent,
                                                name = %name,
                                                "No attribute find, use incomplele attr.",
                                            );
                                            (entry.inode, entry.attr)
                                        } else {
                                            return Err(err);
                                        }
                                    }
                                }
                            } else {
                                return Err(err);
                            }
                        }
                    };
                    if attr.typ == INodeType::Directory && !parent.is_trash() {
                        let mut dir_parents = base.dir_parents.lock();
                        dir_parents.insert(inode, parent);
                    }
                    Ok((inode, attr))
                }
            }
        }
    }

    async fn resolve(&self, parent: Ino, path: &str, fallback: bool) -> Result<(Ino, Attr)> {
        match self.do_resolve(parent, path).await {
            Ok((ino, attr)) => Ok((ino, attr)),
            Err(e) => {
                if e.is_op_not_supported() && fallback {
                    let level_path = path.split("/").collect::<Vec<&str>>();
                    ensure!(
                        level_path.iter().any(|p| !p.is_empty()),
                        InvalidArgSnafu {
                            arg: format!("resolve: path: {path}")
                        }
                    );
                    let mut p = parent;
                    let mut ino_attr = None;
                    for path in level_path.iter().filter(|p| !p.is_empty()) {
                        let (ino, attr) = self.lookup(p, *path, false).await?;
                        ino_attr.replace((ino, attr));
                        p = ino;
                    }
                    Ok(ino_attr.unwrap())
                } else {
                    Err(e)
                }
            }
        }
    }

    // GetAttr returns the attributes for given node.
    async fn get_attr(&self, inode: Ino) -> Result<Attr> {
        let base = self.as_ref();
        let inode = base.check_root(inode);
        if let Some(attr) = base.open_files.attr(inode) {
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
                    typ: INodeType::Directory,
                    mode: if inode.is_trash() { 0o555 } else { 0o777 },
                    nlink: 2,
                    length: 4 << 10,
                    parent: ROOT_INODE,
                    full: true,
                    ..Attr::default()
                })
            }
        } else {
            let mut attr = self.do_get_attr(inode).await?;
            if base.open_files.cache_attr(inode, &attr) {
                attr.keep_cache = true
            } else {
                if let Some(chunks) = base.open_files.chunks(inode) {
                    let mut chunks = chunks.write().await;
                    chunks.clear();
                }
            }
            if attr.typ == INodeType::Directory && inode.is_root() && !attr.parent.is_trash() {
                base.dir_parents.lock().insert(inode, attr.parent);
            }
            Ok(attr)
        }
    }

    // SetAttr updates the attributes for given node.
    async fn set_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        sggid_clear_mode: u8,
        attr: &Attr,
    ) -> Result<()> {
        let base = self.as_ref();
        let inode = base.check_root(inode);
        defer! {
            base.open_files.remove_attr(inode);
            if let Some(mut attr) = base.open_files.attr(inode)
                && set.intersects(SetAttrMask::SET_ATIME | SetAttrMask::SET_ATIME_NOW)
            {
                attr.full = false;
            }
        }
        let _ = self.do_set_attr(inode, set, sggid_clear_mode, attr).await?;
        Ok(())
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
        skip_perm_check: bool,
    ) -> Result<Attr> {
        let chunks = match self.as_ref().open_files.chunks(inode) {
            Some(chunks) => chunks,
            None => {
                return self
                    .do_exists(inode)
                    .await?
                    .then(|| BadFDSnafu { fd: inode }.fail())
                    .unwrap_or_else(|| NoEntryFound2Snafu { ino: inode }.fail())?;
            }
        };
        let mut chunks = chunks.write().await; // lock file until truncate finished
        defer! {
            chunks.clear();
        }
        let (attr, dir_stat) = self
            .do_truncate(inode, flags, attr_length, skip_perm_check)
            .await?;
        self.update_parent_stats(inode, attr.parent, dir_stat.length, dir_stat.space)
            .await;
        Ok(attr)
    }

    // Fallocate preallocate given space for given file.
    async fn fallocate(&self, inode: Ino, flag: Falloc, off: u64, size: u64) -> Result<u64> {
        if flag.intersects(Falloc::COLLAPES_RANGE) && flag != Falloc::COLLAPES_RANGE {
            return InvalidArgSnafu {
                arg: format!("fallocate: COLLAPES_RANGE has other flag"),
            }
            .fail()?;
        }
        if flag.intersects(Falloc::INSERT_RANGE) && flag != Falloc::INSERT_RANGE {
            return InvalidArgSnafu {
                arg: format!("fallocate: INSERT_RANGE has other flag"),
            }
            .fail()?;
        }
        ensure!(
            flag != Falloc::INSERT_RANGE,
            OpNotSupportedSnafu {
                op: "not support op: INSERT RANGE. "
            }
        );
        ensure!(
            flag != Falloc::COLLAPES_RANGE,
            OpNotSupportedSnafu {
                op: "not support op: INSERT RANGE. "
            }
        );
        if flag.intersects(Falloc::PUNCH_HOLE) && !flag.intersects(Falloc::KEEP_SIZE) {
            return InvalidArgSnafu {
                arg: format!("fallocate: KEEP_SIZE with size 0"),
            }
            .fail()?;
        }
        ensure!(
            size != 0,
            InvalidArgSnafu {
                arg: "fallocate: size 0"
            }
        );

        let chunks = match self.as_ref().open_files.chunks(inode) {
            Some(chunks) => chunks,
            None => {
                return self
                    .do_exists(inode)
                    .await?
                    .then(|| BadFDSnafu { fd: inode }.fail())
                    .unwrap_or_else(|| NoEntryFound2Snafu { ino: inode }.fail())?;
            }
        };
        let mut chunks = chunks.write().await;
        defer! {
            chunks.clear();
        }
        let (delta, attr) = self.do_fallocate(inode, flag, off, size).await?;
        self.update_parent_stats(inode, attr.parent, delta.length, delta.space)
            .await;
        Ok(attr.length)
    }

    // ReadLink returns the target of a symlink.
    async fn read_symlink(&self, inode: Ino) -> Result<String> {
        let base = self.as_ref();
        let no_atime = self.as_ref().conf.atime_mode == "NoAtime" || self.as_ref().conf.read_only;

        {
            let symlinks = base.symlinks.lock();
            if let Some((atime, path)) = symlinks.get(&inode) {
                if let Some(atime) = atime {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    let attr = Attr {
                        atime: *atime,
                        ..Attr::default()
                    };
                    // ctime and mtime are ignored since symlink can't be modified
                    let need_update_atime = self.is_atime_need_update(&attr, now);
                    if no_atime || !need_update_atime {
                        return Ok(path.clone());
                    }
                }
            }
        }
        let (atime, path) = self.do_read_symlink(inode, no_atime).await?;
        ensure_whatever!(!path.is_empty(), "read symlink empty path");
        base.symlinks.lock().insert(inode, (atime, path.clone()));
        Ok(path)
    }

    // Symlink creates a symlink in a directory with given name.
    async fn symlink(&self, parent: Ino, name: &str, target_path: &str) -> Result<(Ino, Attr)> {
        if target_path.is_empty() || target_path.len() > MAX_SYMLINK {
            return InvalidArgSnafu {
                arg: format!("symlink: target_path: {target_path}"),
            }
            .fail()?;
        }
        // ' ' is illegal in posix path
        if target_path.chars().any(|c| c == '\0') {
            return InvalidArgSnafu {
                arg: format!("symlink: target_path: {target_path}"),
            }
            .fail()?;
        }

        // mode of symlink is ignored in POSIX
        // do not care exists inode
        self.mknod(parent, name, INodeType::Symlink, 0o777, 0, 0, target_path)
            .await
    }

    // Mknod creates a node in a directory with given name, type and permissions.
    async fn mknod(
        &self,
        parent: Ino,
        name: &str,
        typ: INodeType,
        mode: u16,
        cumask: u16,
        rdev: u32,
        path: &str,
    ) -> Result<(Ino, Attr)> {
        if parent.is_trash() {
            return OpNotPermittedSnafu {
                op: format!("mknod: make inode in trash."),
            }
            .fail()?;
        }
        if parent == ROOT_INODE && name == TRASH_NAME {
            return OpNotPermittedSnafu {
                op: format!("mknod: make trash in root dir."),
            }
            .fail()?;
        }
        if self.as_ref().conf.read_only {
            return ReadFSSnafu.fail()?;
        }
        if name.is_empty() {
            return NoEntryFoundSnafu {
                parent,
                name: name.to_string(),
            }
            .fail()?;
        }

        let parent = self.as_ref().check_root(parent);
        let (space, inodes) = (align_4k(0), 1);
        self.check_quotas(space, inodes, [parent].into()).await?;

        let mut attr = Attr::default();
        match typ {
            INodeType::Directory => {
                attr.nlink = 2;
                attr.length = 4 << 10;
            }
            INodeType::Symlink => {
                attr.nlink = 1;
                attr.length = path.len() as u64;
            }
            _ => {
                attr.nlink = 1;
                attr.length = 0;
                attr.rdev = rdev;
            }
        }
        attr.typ = typ.clone();
        attr.uid = self.uid().clone();
        attr.gid = self.gid().clone();
        attr.parent = parent;
        attr.mode = mode;
        attr.full = true;
        let ino = self.next_inode().await?;
        match self.do_mknod(parent, &name, cumask, path, ino, attr).await {
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
        self.mknod(parent, name, INodeType::Directory, mode, cumask, 0, "")
            .await
            .inspect(|(ino, _)| {
                let base = self.as_ref();
                let mut dir_parents = base.dir_parents.lock();
                dir_parents.insert(*ino, parent);
            })
    }

    // Unlink removes a file entry from a directory.
    // The file will be deleted if it's not linked by any entries and not open by any sessions.
    async fn unlink(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<()> {
        if (parent == ROOT_INODE && name == TRASH_NAME)
            || (parent.is_trash() && !self.uid().is_root())
        {
            return OpNotPermittedSnafu {
                op: format!("unlink: illegal op about trash."),
            }
            .fail()?;
        }

        let base = self.as_ref();
        if base.conf.read_only {
            return ReadFSSnafu.fail()?;
        }

        let parent = base.check_root(parent);
        let attr = self.do_unlink(parent, name, skip_check_trash).await?;
        let diff_length = if attr.typ == INodeType::File {
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
            "." => {
                return InvalidArgSnafu {
                    arg: format!("rmdir: invalid name: {name}"),
                }
                .fail()?
            }
            ".." => {
                return DirNotEmptySnafu {
                    parent,
                    name: name.to_string(),
                }
                .fail()?
            }
            _ => {}
        }
        if parent.is_root() && name == TRASH_NAME {
            return OpNotPermittedSnafu {
                op: format!("rmdir: rm trash in root."),
            }
            .fail()?;
        }
        if parent.is_trash() && !self.uid().is_root() {
            return OpNotPermittedSnafu {
                op: format!("rmdir: rm trash without root user."),
            }
            .fail()?;
        }
        let base = self.as_ref();
        if base.conf.read_only {
            return ReadFSSnafu.fail()?;
        }

        let parent = base.check_root(parent);
        let ino = self.do_rmdir(parent, name, skip_check_trash).await?;
        if !parent.is_trash() {
            let mut dir_parents = base.dir_parents.lock();
            dir_parents.remove(&ino);
        }
        self.update_dir_stats(parent, 0, -(align_4k(0)), -1);
        self.update_quota(parent, -(align_4k(0)), -1).await;
        Ok(ino)
    }

    // Rename move an entry from a source directory to another with given name.
    // The targeted entry will be overwrited if it's a file or empty directory.
    // For Hadoop, the target should not be overwritten.
    async fn rename(
        &self,
        parent_src: Ino,
        name_src: &str,
        parent_dst: Ino,
        name_dst: &str,
        flags: RenameMask,
    ) -> Result<Option<(Ino, Attr)>> {
        let base = self.as_ref();
        if parent_src.is_root() && name_src == TRASH_NAME {
            return OpNotPermittedSnafu {
                op: format!("rename: move root/.trash from "),
            }
            .fail()?;
        }
        if parent_dst.is_root() && name_dst == TRASH_NAME {
            return OpNotPermittedSnafu {
                op: format!("rename: move root/.trash to "),
            }
            .fail()?;
        }
        if parent_src.is_trash() {
            return OpNotPermittedSnafu {
                op: format!("rename: source parent is trash."),
            }
            .fail()?;
        }
        if parent_dst.is_trash() && !self.uid().is_root() {
            return OpNotPermittedSnafu {
                op: format!("rename: dst parent is trash without root user."),
            }
            .fail()?;
        }
        if base.conf.read_only {
            return ReadFSSnafu.fail()?;
        }
        if name_dst.is_empty() {
            return NoEntryFoundSnafu {
                parent: parent_dst,
                name: name_dst.to_string(),
            }
            .fail()?;
        }

        match flags {
            flag if flag.is_empty()
                || flag == RenameMask::NOREPLACE
                || flag == RenameMask::EXCHANGE
                || flag == RenameMask::NOREPLACE.union(RenameMask::RESTORE) =>
            {
                ()
            }
            flag if flag == RenameMask::WHITEOUT
                || flag == RenameMask::NOREPLACE.union(RenameMask::WHITEOUT) =>
            {
                return OpNotSupportedSnafu {
                    op: "rename flag has WHITEOUT",
                }
                .fail()?;
            }
            _ => {
                return InvalidArgSnafu {
                    arg: "rename flags",
                }
                .fail()?
            }
        }

        let parent_src = base.check_root(parent_src);
        let parent_dst = base.check_root(parent_dst);
        let quota_src = if parent_src.is_trash() {
            None
        } else {
            self.get_quota_parent(parent_src).await
        };
        let quota_dst = if parent_src == parent_dst {
            quota_src
        } else {
            self.get_quota_parent(parent_dst).await
        };
        let (space, inodes) = if quota_src != quota_dst {
            let (src_ino, src_attr) = self.lookup(parent_src, name_src, false).await?;
            let (space, inodes) = if src_attr.typ == INodeType::Directory {
                let quota = {
                    let quotas = base.dir_quotas.read();
                    quotas.get(&src_ino).map(|quota| {
                        (
                            quota.used_space.load(Ordering::Relaxed),
                            quota.used_inodes.load(Ordering::Relaxed),
                        )
                    })
                };
                if let Some(quota) = quota {
                    (quota.0 + align_4k(0), quota.1 + 1)
                } else {
                    debug!("Start to get summary of inode {}", src_ino);
                    let sum = self
                        .get_summary(src_ino, true, false)
                        .await
                        .inspect_err(|err| {
                            warn!("Get summary of inode {}: {}", src_ino, err);
                        })?;
                    (sum.size as i64, (sum.dirs + sum.files) as i64)
                }
            } else {
                (align_4k(src_attr.length), 1)
            };
            // TODO: dst exists and is replaced or exchanged
            if quota_dst.is_some() && self.check_quota(parent_src, space, inodes).await {
                return QuotaExceededSnafu.fail()?;
            }
            (space, inodes)
        } else {
            (0, 0)
        };

        // do rename, return (renamed_before_ino, renamed_before_ino_attr, exists_entry))
        // maybe dst alread exists, t_entry is the target entry if exists
        let (ino, attr, t_entry) = match self
            .do_rename(parent_src, name_src, parent_dst, name_dst, flags)
            .await
        {
            Ok((ino, attr, t_entry)) => (ino, attr, t_entry),
            Err(err) => {
                if matches!(err.inner(), MetaErrorEnum::RenameSameInoError) {
                    info!("rename same ino({},{})", parent_src, name_src);
                    return Ok(None);
                } else {
                    return Err(err);
                }
            }
        };
        let diff_length = match attr.typ {
            INodeType::Directory => {
                let mut dir_parents = base.dir_parents.lock();
                dir_parents.insert(ino, parent_dst);
                0
            }
            INodeType::File => attr.length,
            _ => 0,
        };
        if parent_src != parent_dst {
            self.update_dir_stats(
                parent_src,
                -(diff_length as i64),
                -(align_4k(diff_length)),
                -1,
            );
            self.update_dir_stats(parent_dst, diff_length as i64, align_4k(diff_length), 1);
            if quota_src != quota_dst {
                if quota_src.is_some() {
                    self.update_quota(parent_src, -(space), -(inodes)).await;
                }
                if quota_dst.is_some() {
                    self.update_quota(parent_dst, space, inodes).await;
                }
            }
        }
        if let Some((tino, tattr)) = t_entry
            && flags != RenameMask::EXCHANGE
        {
            let diff_length = match tattr.typ {
                INodeType::Directory => {
                    let mut dir_parents = base.dir_parents.lock();
                    dir_parents.remove(&tino);
                    0
                }
                INodeType::File => tattr.length,
                _ => 0,
            };
            self.update_dir_stats(
                parent_dst,
                -(diff_length as i64),
                -(align_4k(diff_length)),
                -1,
            );
            if quota_dst.is_some() {
                self.update_quota(parent_dst, -(align_4k(diff_length)), -1)
                    .await;
            }
        }
        Ok(Some((ino, attr)))
    }

    // Link creates an entry for node.
    async fn link(&self, inode: Ino, parent: Ino, name: &str) -> Result<Attr> {
        if parent.is_trash() {
            return OpNotPermittedSnafu {
                op: format!("link: target dir is in trash."),
            }
            .fail()?;
        }
        if parent.is_root() && name == TRASH_NAME {
            return OpNotPermittedSnafu {
                op: format!("link: target path is root's trash."),
            }
            .fail()?;
        }
        if self.as_ref().conf.read_only {
            return ReadFSSnafu.fail()?;
        }
        if name.is_empty() {
            return NoEntryFoundSnafu {
                parent,
                name: name.to_string(),
            }
            .fail()?;
        }
        if name == "." || name == ".." {
            return InvalidArgSnafu {
                arg: format!("link: invalid name: {name}"),
            }
            .fail()?;
        }

        let parent = self.as_ref().check_root(parent);
        let attr = self.get_attr(inode).await?;
        if attr.typ == INodeType::Directory {
            return OpNotPermittedSnafu {
                op: format!("link: source is dir."),
            }
            .fail()?;
        }
        if self.check_quota(parent, align_4k(attr.length), 1).await {
            return QuotaExceededSnafu.fail()?;
        }
        defer! {
            self.as_ref().open_files.remove_attr(inode);
        }
        let attr = self.do_link(inode, parent, name).await?;
        self.update_dir_stats(parent, attr.length as i64, align_4k(attr.length), 1);
        self.update_quota(parent, align_4k(attr.length), 1).await;
        Ok(attr)
    }

    // Readdir returns all entries for given directory, which include attributes if plus is true.
    async fn readdir(&self, inode: Ino, wantattr: bool) -> Result<Vec<Entry>> {
        let base = self.as_ref();
        let inode = base.check_root(inode);
        let mut attr = self.get_attr(inode).await?;
        let mut mmask = ModeMask::READ;
        if wantattr {
            mmask.insert(ModeMask::EXECUTE);
        }
        self.access(inode, mmask, &attr).await?;
        if inode == base.root() {
            attr.parent = base.root();
        }
        let mut base_entries = vec![
            Entry {
                inode,
                name: ".".to_string(),
                attr: Attr {
                    typ: INodeType::Directory,
                    ..Default::default()
                }
                .clone(),
            },
            Entry {
                inode: attr.parent,
                name: "..".to_string(),
                attr: Attr {
                    typ: INodeType::Directory,
                    ..Default::default()
                },
            },
        ];
        //  TODO: why here ENOENT and trash return ok?
        base_entries.append(&mut self.do_readdir(inode, wantattr, None).await?);
        self.update_attr_atime(inode).await;
        Ok(base_entries)
    }

    // Create creates a file in a directory with given name.
    async fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: OFlag,
    ) -> Result<(Ino, Attr)> {
        let (ino, mut attr) = match self
            .mknod(parent, name, INodeType::File, mode, cumask, 0, "")
            .await
        {
            Ok((ino, attr)) => (ino, attr),
            Err(err) => {
                if let MetaErrorEnum::EntryExists {
                    parent: _,
                    name: _,
                    exist_ino,
                    exist_attr,
                } = err.inner()
                    && !flags.intersects(OFlag::O_EXCL)
                {
                    let attr = match exist_attr {
                        Some(attr) => attr.clone(),
                        None => self.get_attr(*exist_ino).await?,
                    };
                    if attr.typ == INodeType::File {
                        (*exist_ino, attr)
                    } else {
                        return Err(err);
                    }
                } else {
                    return Err(err);
                }
            }
        };
        self.as_ref().open_files.open(ino);
        if self.as_ref().open_files.cache_attr(ino, &attr) {
            attr.keep_cache = true;
        } else {
            if let Some(chunks) = self.as_ref().open_files.chunks(ino) {
                let mut chunks = chunks.write().await;
                chunks.clear();
            }
        }
        return Ok((ino, attr));
    }

    // Open checks permission on a node and track it as open.
    async fn open(&self, inode: Ino, flags: OFlag) -> Result<Attr> {
        if self.as_ref().conf.read_only
            && flags.intersects(
                OFlag::O_WRONLY
                    .union(OFlag::O_RDWR)
                    .union(OFlag::O_TRUNC)
                    .union(OFlag::O_APPEND),
            )
        {
            return ReadFSSnafu.fail()?;
        }
        let base = self.as_ref();
        let mut attr = match base.open_files.attr(inode) {
            Some(attr) => attr,
            None => {
                let attr = self.get_attr(inode).await?;
                let mode_mask = match flags
                    .intersection(OFlag::O_RDONLY.union(OFlag::O_WRONLY).union(OFlag::O_RDWR))
                {
                    OFlag::O_RDONLY => ModeMask::READ,
                    OFlag::O_WRONLY => ModeMask::WRITE,
                    OFlag::O_RDWR => ModeMask::READ.union(ModeMask::WRITE),
                    _ => ModeMask::empty(),
                };
                self.access(inode, mode_mask, &attr).await?;
                if attr.flags.intersects(Flag::IMMUTABLE) || attr.parent > TRASH_INODE {
                    if flags.intersects(OFlag::O_WRONLY.union(OFlag::O_RDWR)) {
                        return OpNotPermittedSnafu {
                            op: format!("open({inode},{flags:?}): flags conflict with attr flags."),
                        }
                        .fail()?;
                    }
                }
                if attr.flags.intersects(Flag::APPEND) {
                    if flags.intersects(OFlag::O_WRONLY.union(OFlag::O_RDWR))
                        && !flags.intersects(OFlag::O_APPEND)
                    {
                        return OpNotPermittedSnafu {
                            op: format!("open({inode},{flags:?}): flags conflict with attr flags."),
                        }
                        .fail()?;
                    }
                    if flags.intersects(OFlag::O_TRUNC) {
                        return OpNotPermittedSnafu {
                            op: format!("open({inode},{flags:?}): flags conflict with attr flags."),
                        }
                        .fail()?;
                    }
                }
                attr
            }
        };

        // Only success modify atime?
        base.open_files.open(inode);
        if base.open_files.cache_attr(inode, &attr) {
            attr.keep_cache = true;
        } else {
            if let Some(chunks) = base.open_files.chunks(inode) {
                let mut chunks = chunks.write().await;
                chunks.clear();
            }
        }
        self.update_attr_atime(inode).await;
        Ok(attr)
    }

    // Close a file.
    async fn close(&self, inode: Ino) -> Result<()> {
        let base = self.as_ref();
        base.open_files.close(inode);
        if !base.open_files.is_opened(inode) {
            // lock during close file
            let removed = {
                let mut removed_files = base.removed_files.lock();
                removed_files.remove(&inode)
            };

            let sid = {
                let sid = base.sid.read();
                sid.clone()
            };
            match sid {
                Some(sid) => {
                    if let Some(removed) = removed
                        && removed
                    {
                        self.do_delete_sustained_inode(sid, inode).await?;
                    }
                }
                None => error!("close a file happend at no session"),
            }
        }
        Ok(())
    }

    // Read returns the list of slices on the given chunk.
    async fn read(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>> {
        let chunks = match self.as_ref().open_files.chunks(inode) {
            Some(chunks) => chunks,
            None => {
                return self
                    .do_exists(inode)
                    .await?
                    .then(|| BadFDSnafu { fd: inode }.fail())
                    .unwrap_or_else(|| NoEntryFound2Snafu { ino: inode }.fail())?;
            }
        };
        let chunks = chunks.read().await;
        if let Some(slices) = chunks.get(&indx) {
            return Ok(slices.clone());
        }

        let pslices = self.do_read(inode, indx).await?;
        if pslices.is_empty() {
            let attr = self.do_get_attr(inode).await?;
            if attr.typ != INodeType::File {
                return InvalidArgSnafu {
                    arg: format!("read: inode is not file."),
                }
                .fail()?;
            }
            Ok(Vec::new())
        } else {
            let pslices_len = pslices.len();
            let slices = pslices.build_slices();
            if !self.as_ref().conf.read_only && (pslices_len >= 5 || slices.len() >= 5) {
                let mut meta = dyn_clone::clone_box(self);
                meta.with_cancel(self.token().child_token());
                tokio::spawn(async move {
                    meta.compact_chunk(inode, indx, false, false).await;
                });
            }
            Ok(slices)
        }
    }

    // NewSlice returns an id for new slice.
    async fn new_slice(&self) -> Result<u64> {
        let mut slices = self.as_ref().free_slices.lock().await;
        if slices.next >= slices.maxid {
            let v = self.incr_counter(NEXT_CHUNK, SLICE_ID_BATCH).await?;
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
        coff: u32,
        slice: Slice,
        mtime: DateTime<Utc>,
    ) -> Result<()> {
        ensure!(
            slice.id != 0
                && coff <= CHUNK_SIZE as u32
                && slice.len <= slice.size
                && coff + slice.len <= CHUNK_SIZE as u32,
            InvalidArgSnafu {
                arg: format!("invalid slice {slice:?} in coff {coff} in write func")
            }
        );
        let chunks = match self.as_ref().open_files.chunks(inode) {
            Some(chunks) => chunks,
            None => {
                return self
                    .do_exists(inode)
                    .await?
                    .then(|| BadFDSnafu { fd: inode }.fail())
                    .unwrap_or_else(|| NoEntryFound2Snafu { ino: inode }.fail())?;
            }
        };
        let mut chunks = chunks.write().await;
        defer! {
            chunks.remove(&indx);
        };
        let (num_slices, delta, attr) = self
            .do_write(inode, indx, coff, slice, mtime)
            .await?;
        self.update_parent_stats(inode, attr.parent, delta.length, delta.space)
            .await;
        if num_slices % 100 == 99 || num_slices > 350 {
            if num_slices < MAX_SLICES {
                let mut meta = dyn_clone::clone_box(self);
                meta.with_cancel(self.token().child_token());
                tokio::spawn(async move {
                    meta.compact_chunk(inode, indx, false, false).await;
                });
            } else {
                self.compact_chunk(inode, indx, false, false).await;
            }
        }
        Ok(())
    }

    // InvalidateChunkCache invalidate chunk cache
    async fn invalidate_chunk_cache(&self, inode: Ino, indx: u32) -> Result<()> {
        let chunks = match self.as_ref().open_files.chunks(inode) {
            Some(chunks) => chunks,
            None => {
                return self
                    .do_exists(inode)
                    .await?
                    .then(|| BadFDSnafu { fd: inode }.fail())
                    .unwrap_or_else(|| NoEntryFound2Snafu { ino: inode }.fail())?;
            }
        };
        let mut chunks = chunks.write().await;
        chunks.remove(&indx);
        Ok(())
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
    ) -> Result<u64> {
        let chunks = match self.as_ref().open_files.chunks(fout) {
            Some(chunks) => chunks,
            None => {
                return self
                    .do_exists(fout)
                    .await?
                    .then(|| BadFDSnafu { fd: fout }.fail())
                    .unwrap_or_else(|| NoEntryFound2Snafu { ino: fout }.fail())?;
            }
        };
        let mut chunks = chunks.write().await;
        defer! {
            chunks.clear();
        }
        let (copy_size, delta) = self
            .do_copy_file_range(fin, off_in, fout, off_out, size, flags)
            .await?;
        if let Some((delta_ino, delta_stat)) = delta {
            self.update_parent_stats(fout, delta_ino, delta_stat.length, delta_stat.space)
                .await;
        }
        Ok(copy_size)
    }

    // GetDirStat returns the space and inodes usage of a directory.
    async fn get_dir_stat(&self, inode: Ino) -> Result<DirStat> {
        match self
            .do_get_dir_stat(
                self.as_ref().check_root(inode),
                !self.as_ref().conf.read_only,
            )
            .await?
        {
            Some(stat) => Ok(stat),
            None => self.calc_dir_stat(inode).await,
        }
    }

    // GetXattr returns the value of extended attribute for given name.
    async fn get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>> {
        self.do_get_xattr(inode, name).await
    }

    // ListXattr returns all extended attributes of a node.
    async fn list_xattr(&self, inode: Ino) -> Result<Vec<u8>> {
        self.do_list_xattr(inode).await
    }

    // SetXattr update the extended attribute of a node.
    async fn set_xattr(&self, inode: Ino, name: &str, value: Vec<u8>, flag: XattrF) -> Result<()> {
        ensure!(!self.as_ref().conf.read_only, ReadFSSnafu);
        ensure!(
            !name.is_empty(),
            InvalidArgSnafu {
                arg: "set xattr empty name"
            }
        );
        self.do_set_xattr(inode, name, value, flag).await
    }

    // RemoveXattr removes the extended attribute of a node.
    async fn remove_xattr(&self, inode: Ino, name: &str) -> Result<()> {
        ensure!(!self.as_ref().conf.read_only, ReadFSSnafu);
        ensure!(
            !name.is_empty(),
            InvalidArgSnafu {
                arg: "remove xattr empty name"
            }
        );

        self.do_remove_xattr(self.as_ref().check_root(inode), name)
            .await
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
        ltype: Fcntl,
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
        self.do_list_slices(delete).await
    }

    // Remove all files and directories recursively.
    async fn remove(&self, parent: Ino, name: &str) -> Result<u64> {
        let base = self.as_ref();
        let parent = base.check_root(parent);
        self.access(
            parent,
            ModeMask::WRITE.union(ModeMask::EXECUTE),
            &Attr::default(),
        )
        .await?;
        let (inode, attr) = self.lookup(parent, name, false).await?;
        let mut count = 0;
        if attr.typ != INodeType::Directory {
            count += 1;
            self.unlink(parent, name, false).await?;
        } else {
            // max for 50 parallelism coroutine to run remove op
            let max_removing = Arc::new(Semaphore::new(50));
            let atomic_count = Arc::new(AtomicU64::new(0));
            self.remove_entry(
                parent,
                name,
                inode,
                false,
                atomic_count.clone(),
                max_removing,
            )
            .await?;
            count += atomic_count.load(Ordering::Relaxed);
        }
        Ok(count)
    }

    // Get summary of a node; for a directory it will accumulate all its child nodes
    async fn get_summary(&self, ino: Ino, recursive: bool, strict: bool) -> Result<Summary> {
        let attr = self.get_attr(ino).await?;
        if attr.typ != INodeType::Directory {
            let length = if attr.typ == INodeType::File {
                attr.length
            } else {
                0
            };
            Ok(Summary {
                dirs: 0,
                files: 1,
                size: align_4k(0) as u64,
                length: length,
            })
        } else {
            let inode = self.as_ref().check_root(ino);
            let mut summary = self.get_dir_summary(inode, recursive, strict).await?;
            summary.dirs += 1;
            summary.size += align_4k(0) as u64;
            Ok(summary)
        }
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
    async fn clone_ino(
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
    async fn chroot(&self, subdir: &str) -> Result<Ino> {
        let level_subdir = subdir.split("/").collect::<Vec<&str>>();
        let mut tmp_root = self.as_ref().root();
        for subdir in level_subdir {
            ensure!(
                !subdir.is_empty(),
                NoEntryFoundSnafu {
                    parent: tmp_root,
                    name: subdir.to_string()
                }
            );
            let (inode, attr) = match self.lookup(tmp_root, subdir, false).await {
                Ok((inode, attr)) => (inode, attr),
                Err(e) => {
                    if e.is_no_entry_found(&tmp_root, subdir) {
                        self.mkdir(tmp_root, subdir, 0o777, 0, 0).await?
                    } else {
                        return Err(e);
                    }
                }
            };
            ensure!(
                attr.typ == INodeType::Directory,
                NotDir2Snafu { ino: inode }
            );
            tmp_root = inode;
        }
        self.as_ref().chroot(tmp_root);
        Ok(tmp_root)
    }

    // GetParents returns a map of node parents (> 1 parents if hardlinked)
    async fn get_parents(&self, inode: Ino) -> HashMap<Ino, u32> {
        if inode.is_root() || inode.is_trash() {
            return [(ROOT_INODE, 1)].into();
        }
        let attr = match self.get_attr(inode).await {
            Ok(attr) => attr,
            Err(_) => {
                warn!("GetAttr inode err {}: {}", inode, "st");
                return HashMap::new();
            }
        };
        if attr.parent > 0 {
            [(attr.parent, 1)].into()
        } else {
            self.do_get_parents(inode).await
        }
    }

    fn get_base(&self) -> &CommonMeta {
        self.as_ref()
    }
}
