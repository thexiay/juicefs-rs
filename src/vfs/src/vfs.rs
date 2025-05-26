use std::{
    cmp::min,
    collections::HashMap,
    ops::Deref,
    sync::{atomic::AtomicU64, Arc},
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_async_stream::try_stream;

use juice_meta::{
    api::{
        Attr, Entry, Falloc, Fcntl, INodeType, Ino, Meta, ModeMask, OFlag, SetAttrMask, StatFs,
        MAX_FILE_LEN, MAX_FILE_NAME_LEN, O_ACCMODE, ROOT_INODE,
    }, config::Format, context::{FsContext, Gid, Uid, WithContext}
};
use juice_storage::api::CachedStore;
use juice_utils::fs::{self, task_local};
use nix::errno::Errno;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    config::Config,
    frange::Frange,
    handle::{Fh, Handle, HandleInner},
    internal::{CONTROL_INODE, LOG_INODE},
    reader::DataReader,
    writer::DataWriter,
};

#[derive(Clone)]
pub struct Vfs {
    vfs: Arc<VfsInner>,
    context: FsContext,
}

impl Deref for Vfs {
    type Target = VfsInner;

    fn deref(&self) -> &Self::Target {
        &self.vfs
    }
}

impl AsRef<FsContext> for Vfs {
    fn as_ref(&self) -> &FsContext {
        &self.context
    }
}

impl AsMut<FsContext> for Vfs {
    fn as_mut(&mut self) -> &mut FsContext {
        &mut self.context
    }
}

pub struct VfsInner {
    pub(crate) conf: Arc<Config>,
    pub(crate) meta: Arc<dyn Meta>,
    pub(crate) store: Arc<CachedStore>,
    pub(crate) reader: Arc<DataReader>,
    pub(crate) writer: Arc<DataWriter>,

    pub(crate) handles: DashMap<Ino, DashMap<Fh, Arc<Handle>>>,
    pub(crate) ino_mapping: DashMap<Fh, Ino>,
    pub(crate) next_fh: AtomicU64,
    pub(crate) internal_inos: Vec<Entry>,
    pub(crate) last_modified: RwLock<HashMap<Ino, DateTime<Utc>>>,
}

/// Here's the [`FileSystem`] function of vfs:
/// 1. Interface calls that wrap metadata and data
/// 2. Handle different inode:
///     - special control inode
///     - file inode
///     - directory inode
/// 3. Pre-check such as file length, etc.
impl Vfs {
    pub fn new(config: Config, meta: Arc<dyn Meta>, store: CachedStore) -> Self {
        let config = Arc::new(config);
        let store = Arc::new(store);
        let reader = DataReader::new(meta.clone(), store.clone(), config.clone());
        let writer = DataWriter::new(
            config.clone(),
            meta.clone(),
            store.clone(),
            reader.clone().invalid_notifier(),
        );
        Vfs {
            vfs: Arc::new(VfsInner {
                conf: config,
                meta,
                store,
                reader,
                writer,
                handles: DashMap::new(),
                ino_mapping: DashMap::new(),
                next_fh: AtomicU64::new(1),
                internal_inos: Vec::new(),
                last_modified: RwLock::new(HashMap::new()),
            }),
            context: FsContext::default(),
        }
    }

    pub fn with_cancel(&self, token: CancellationToken) -> Self {
        self.clone()
    }

    pub async fn get_attr(&self, ino: Ino) -> Result<Entry, Errno> {
        todo!()
    }

    pub async fn set_attr(
        &self,
        ino: Ino,
        mask: SetAttrMask,
        fh: Option<Fh>,
        attr: Attr,
    ) -> Result<Entry, Errno> {
        todo!()
    }

    pub async fn stat_fs(&self, ino: Ino) -> Result<StatFs, Errno> {
        let stat_fs = self.meta.stat_fs(ino).await.map_err(|e| {
            error!("stat_fs failed: {:?}", e);
            e.fs_err()
        })?;
        Ok(stat_fs)
    }

    pub async fn mknod(
        &self,
        parent: Ino,
        name: &str,
        r#type: INodeType,
        mode: u16,
        cumask: u16,
        rdev: u32,
    ) -> Result<Entry, Errno> {
        if parent == ROOT_INODE && self.is_special_node(name) {
            return Err(Errno::EEXIST);
        }

        if name.len() > MAX_FILE_NAME_LEN {
            return Err(Errno::ENAMETOOLONG);
        }

        if r#type == INodeType::Symlink {
            return Err(Errno::EINVAL);
        }

        let (ino, attr) = self
            .meta
            .mknod(parent, name, r#type, mode, cumask, rdev, "")
            .await
            .map_err(|e| {
                error!("mknod failed: {:?}", e);
                e.fs_err()
            })?;
        self.cache_dir_entry(parent, name, Some((ino, attr.clone())))
            .await;
        Ok(Entry {
            inode: ino,
            name: name.to_string(),
            attr,
        })
    }

    pub async fn truncate(&self, ino: Ino, fh: Fh, size: u64) -> Result<Attr, Errno> {
        if self.is_special_inode(ino) {
            return Err(Errno::EPERM);
        }
        if size >= MAX_FILE_LEN {
            return Err(Errno::EFBIG);
        }

        // Lock all active handles, but ops after this truncates will not be locked
        let mut hs = self.find_all_handle(ino);
        hs.sort_by_key(|h| h.fh());
        let mut locks = Vec::new();
        for h in hs.iter() {
            locks.push(h.write(true).await.ok_or(Errno::EINTR)?);
        }
        let _ = self.writer.flush(ino).await.inspect_err(|e| {
            info!("flush writer {ino} failed: {:?}, ignore.", e);
        });

        let attr = if fh == 0 {
            self.meta.truncate(ino, 0, size, false).await.map_err(|e| {
                error!("truncate failed: {:?}", e);
                e.fs_err()
            })?
        } else {
            let _ = self.find_handle(ino, fh).ok_or(Errno::EBADF)?;
            self.meta.truncate(ino, 0, size, true).await.map_err(|e| {
                error!("truncate failed: {:?}", e);
                e.fs_err()
            })?
        };

        self.writer.truncate(ino, size);
        self.reader.truncate(ino, size);
        let mut last_modified = self.last_modified.write();
        last_modified.insert(ino, Utc::now());
        Ok(attr)
    }

    pub async fn fallocate(
        &self,
        ino: Ino,
        fh: Fh,
        mode: Falloc,
        off: u64,
        size: u64,
    ) -> Result<(), Errno> {
        if self.is_special_inode(ino) {
            return Err(Errno::EPERM);
        }
        if off >= MAX_FILE_LEN || off + size >= MAX_FILE_LEN {
            return Err(Errno::EFBIG);
        }

        let h = match self.find_handle(ino, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        let mut h = h.write(true).await.ok_or(Errno::EINTR)?;
        match &mut *h {
            HandleInner::File(h) => match h.writer() {
                Some(writer) => {
                    writer.flush().await.map_err(|e| {
                        error!("flush writer failed: {:?}", e);
                        e.fs_err()
                    })?;
                    let length = self
                        .meta
                        .fallocate(ino, mode, off, size)
                        .await
                        .map_err(|e| {
                            error!("fallocate failed: {:?}", e);
                            e.fs_err()
                        })?;
                    writer.truncate(size);
                    let s = min(length - off, size);
                    if s > 0 {
                        self.reader.invalidate(ino, Frange { off, len: s });
                    }
                    let mut last_modified = self.last_modified.write();
                    last_modified.insert(ino, Utc::now());
                    Ok(())
                }
                None => Err(Errno::EBADF),
            },
            HandleInner::Dir(_) => Err(Errno::EINVAL),
            HandleInner::Control(_) => Err(Errno::EINVAL),
        }
    }

    pub async fn copy_file_range(
        &self,
        ino_in: Ino,
        fh_in: Fh,
        off_in: u64,
        ino_out: Ino,
        fh_out: Fh,
        off_out: u64,
        size: u64,
        flags: u32,
    ) -> Result<u64, Errno> {
        if self.is_special_inode(ino_in) {
            return Err(Errno::ENOTSUP);
        }
        if self.is_special_inode(ino_out) {
            return Err(Errno::EPERM);
        }

        let hi = match self.find_handle(ino_in, fh_in) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        let ho = match self.find_handle(ino_out, fh_out) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        if !hi.readable() {
            return Err(Errno::EBADF);
        }
        if !ho.writable() {
            return Err(Errno::EBADF);
        }
        if off_in >= MAX_FILE_LEN
            || off_in + size >= MAX_FILE_LEN
            || off_out >= MAX_FILE_LEN
            || off_out + size >= MAX_FILE_LEN
        {
            return Err(Errno::EFBIG);
        }
        if flags != 0 {
            return Err(Errno::EINVAL);
        }
        if ino_in == ino_out
            && (off_in <= off_out && off_out < off_in + size
                || off_out <= off_in && off_in < off_out + size)
        // overlap
        {
            return Err(Errno::EINVAL);
        }

        let ho = ho.write(true).await.ok_or(Errno::EINTR)?;
        let hi = if ino_in != ino_out {
            Some(hi.read(true).await.ok_or(Errno::EINTR)?)
        } else {
            None
        };
        self.writer.flush(ino_in).await.map_err(|e| {
            error!("flush writer {ino_in} failed: {:?}", e);
            e.fs_err()
        })?;
        self.writer.flush(ino_out).await.map_err(|e| {
            error!("flush writer {ino_out} failed: {:?}", e);
            e.fs_err()
        })?;
        if let Some((copied, length)) = self
            .meta
            .copy_file_range(ino_in, off_in, ino_out, off_out, size, flags)
            .await
            .map_err(|e| {
                error!("copy_file_range failed: {:?}", e);
                e.fs_err()
            })?
        {
            self.writer.truncate(ino_out, length);
            self.reader.invalidate(
                ino_out,
                Frange {
                    off: off_out,
                    len: copied,
                },
            );
            let mut last_modified = self.last_modified.write();
            last_modified.insert(ino_out, Utc::now());
            Ok(copied)
        } else {
            Ok(0)
        }
    }

    pub async fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
    ) -> Result<Entry, Errno> {
        if parent == ROOT_INODE && self.is_special_node(name) {
            return Err(Errno::EEXIST);
        }
        if name.len() > MAX_FILE_NAME_LEN {
            return Err(Errno::ENAMETOOLONG);
        }

        let (ino, attr) = self
            .meta
            .mkdir(parent, name, mode, cumask, 0)
            .await
            .map_err(|e| {
                error!("mkdir failed: {:?}", e);
                e.fs_err()
            })?;
        self.cache_dir_entry(parent, name, Some((ino, attr.clone())))
            .await;
        Ok(Entry {
            inode: ino,
            name: name.to_string(),
            attr,
        })
    }

    pub async fn rmdir(&self, parent: Ino, name: &str) -> Result<(), Errno> {
        if name.len() > MAX_FILE_NAME_LEN {
            return Err(Errno::ENAMETOOLONG);
        }
        self.meta.rmdir(parent, name, false).await.map_err(|e| {
            error!("rmdir failed: {:?}", e);
            e.fs_err()
        })?;
        self.cache_dir_entry(parent, name, None).await;
        Ok(())
    }

    pub async fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u16,
        cumask: u16,
        flags: OFlag,
    ) -> Result<(Fh, Entry), Errno> {
        if parent == ROOT_INODE && self.is_special_node(name) {
            return Err(Errno::EEXIST);
        }
        if name.len() > MAX_FILE_NAME_LEN {
            return Err(Errno::ENAMETOOLONG);
        }

        let (ino, mut attr) = self
            .meta
            .create(parent, name, mode & 0o7777, cumask, flags)
            .await
            .map_err(|e| {
                error!("create failed: {:?}", e);
                e.fs_err()
            })?;
        self.update_len(ino, &mut attr);
        let fh = self
            .new_handle(ino, attr.length, Some(flags))
            .await
            .map_err(|e| {
                error!("create failed: {:?}", e);
                e.fs_err()
            })?;
        self.cache_dir_entry(parent, name, Some((ino, attr.clone())))
            .await;
        Ok((
            fh,
            Entry {
                inode: ino,
                name: name.to_string(),
                attr,
            },
        ))
    }

    pub async fn open(&self, ino: Ino, flags: OFlag) -> Result<(Fh, Attr), Errno> {
        if self.is_special_inode(ino) {
            if ino != CONTROL_INODE && (flags & O_ACCMODE) != OFlag::O_RDONLY {
                return Err(Errno::EACCES);
            }
            todo!()
        }

        let mut attr = self.meta.open(ino, flags).await.map_err(|e| {
            error!("open failed: {:?}", e);
            e.fs_err()
        })?;
        self.update_len(ino, &mut attr);
        let fh = self
            .new_handle(ino, attr.length, Some(flags))
            .await
            .map_err(|e| {
                error!("open failed: {:?}", e);
                e.fs_err()
            })?;
        Ok((fh, attr))
    }

    pub async fn opendir(&self, ino: Ino, flags: OFlag) -> Result<Fh, Errno> {
        if self.check_permission() {
            let mmask = match flags & (OFlag::O_RDONLY | OFlag::O_WRONLY | OFlag::O_RDWR) {
                OFlag::O_RDONLY => ModeMask::READ,
                OFlag::O_WRONLY => ModeMask::WRITE,
                OFlag::O_RDWR => ModeMask::READ | ModeMask::WRITE,
                _ => ModeMask::empty(),
            };
            self.meta
                .access(ino, mmask, &Attr::default())
                .await
                .map_err(|_| Errno::EACCES)?;
        }
        let fh = self.new_handle(ino, 0, None).await.map_err(|e| {
            error!("opendir failed: {:?}", e);
            e.fs_err()
        })?;
        Ok(fh)
    }

    pub async fn releasedir(&self, ino: Ino, fh: Fh) -> Result<(), Errno> {
        match self.find_handle(ino, fh) {
            Some(_) => {
                self.release_handle(ino, fh); // dir has no writer and reader, so delete it directly
                Ok(())
            }
            None => Err(Errno::EBADF),
        }
    }

    // force close handle
    pub async fn release(&self, ino: Ino, fh: Fh) -> Result<(), Errno> {
        if self.is_special_inode(ino) {
            if ino == LOG_INODE {
                // close access log
            }
            self.release_handle(ino, fh);
            return Ok(());
        }

        let h = match self.find_handle(ino, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        let mut h = h.write(true).await.ok_or(Errno::EINTR)?;
        match &mut *h {
            HandleInner::File(h) => {
                if let Some(writer) = h.writer() {
                    writer.flush().await.map_err(|e| {
                        error!("flush writer failed: {:?}", e);
                        e.fs_err()
                    })?;
                }
                {
                    let mut last_modified = self.last_modified.write();
                    last_modified.insert(ino, Utc::now());
                }
                if let Some(owner) = &h.posix_lock_owner() {
                    let _ = self
                        .meta
                        .setlk(ino, *owner, false, Fcntl::F_RDLCK, 0, 0x7FFFFFFFFFFFFFFF, 0)
                        .await;
                }
            }
            HandleInner::Dir(_) => {}
            HandleInner::Control(_) => {}
        }
        let _ = self.meta.close(ino).await;
        self.release_handle(ino, fh); // drop file handle auto kill all reader and writer tasks
        Ok(())
    }

    pub async fn flush(&self, ino: Ino, fh: Fh, lock_owner: u64) -> Result<(), Errno> {
        if self.is_special_inode(ino) {
            if ino == CONTROL_INODE {
                // cancel all operations
            }
            return Ok(());
        }
        let h = match self.find_handle(ino, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };

        let mut h = loop {
            match h.write(true).await {
                Some(h) => break h,
                None => (), // cancel op
            }
        };
        match &mut *h {
            HandleInner::File(h) => {
                if let Some(writer) = h.writer() {
                    writer.flush().await.map_err(|e| {
                        error!("flush writer failed: {:?}", e);
                        e.fs_err()
                    })?;
                }
                let already_locked = h
                    .posix_lock_owner()
                    .map_or(false, |owner| owner == lock_owner);
                if already_locked {
                    h.free_posix_lock();
                }
                if let Some(owner) = &h.posix_lock_owner() {
                    let _ = self
                        .meta
                        .setlk(ino, *owner, false, Fcntl::F_UNLCK, 0, 0x7FFFFFFFFFFFFFFF, 0)
                        .await;
                }
            }
            HandleInner::Dir(_) => {}
            HandleInner::Control(_) => {}
        }
        Ok(())
    }

    pub async fn fsync(&self, ino: Ino, fh: Fh) -> Result<(), Errno> {
        if self.is_special_inode(ino) {
            return Ok(());
        }
        let h = match self.find_handle(ino, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        let mut h = loop {
            match h.write(true).await {
                Some(h) => break h,
                None => (),
            }
        };
        match &mut *h {
            HandleInner::File(h) => {
                if let Some(writer) = h.writer() {
                    writer.flush().await.map_err(|e| {
                        error!("flush writer failed: {:?}", e);
                        e.fs_err()
                    })?;
                }
            }
            HandleInner::Dir(_) => {}
            HandleInner::Control(_) => {}
        }
        Ok(())
    }

    // returns `(offset, dir entry)`
    #[try_stream(boxed, ok = Entry, error = Errno)]
    pub async fn readdir(&self, parent: Ino, fh: Fh, off: u64) {
        let h = match self.find_handle(parent, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        let mut h = loop {
            match h.write(true).await {
                Some(h) => break h,
                None => (),
            }
        };

        match &mut *h {
            HandleInner::Dir(handle) => {
                if handle.need_refresh() {
                    handle.refresh().await.map_err(|e| {
                        error!("refresh failed: {:?}", e);
                        e.fs_err()
                    })?;
                }
                handle.seek(off);
                loop {
                    match handle.read_once() {
                        Some(entry) => {
                            yield entry;
                        }
                        None => break,
                    }
                }
            }
            _ => return Err(Errno::ENOTDIR),
        }
    }

    pub async fn read(&self, ino: Ino, fh: Fh, off: u64, size: u64) -> Result<Bytes, Errno> {
        if self.is_special_inode(ino) {
            todo!()
        }
        let h = match self.find_handle(ino, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        if off >= MAX_FILE_LEN || off + size >= MAX_FILE_LEN {
            return Err(Errno::EFBIG);
        }

        let h = h.read(true).await.ok_or(Errno::EINTR)?;
        match &*h {
            HandleInner::File(h) => {
                self.writer.flush(ino).await.map_err(|e| {
                    error!("flush writer failed: {:?}", e);
                    e.fs_err()
                })?;
                let reader = h.reader();
                let mut buf = BytesMut::with_capacity(size as usize);
                let _ = reader.read(off, size, &mut buf).await.map_err(|e| {
                    error!("read failed: {:?}", e);
                    e.fs_err()
                })?;
                Ok(buf.freeze())
            }
            HandleInner::Dir(_) => Err(Errno::EISDIR),
            HandleInner::Control(_) => todo!(),
        }
    }

    pub async fn write(&self, ino: Ino, fh: Fh, off: u64, data: Bytes) -> Result<(), Errno> {
        let h = match self.find_handle(ino, fh) {
            Some(h) => h,
            None => return Err(Errno::EBADF),
        };
        if off >= MAX_FILE_LEN || off + data.len() as u64 >= MAX_FILE_LEN {
            return Err(Errno::EFBIG);
        }
        if !h.writable() {
            return Err(Errno::EBADF);
        }

        if ino == CONTROL_INODE {
            todo!()
        }

        let mut h = h.write(true).await.ok_or(Errno::EINTR)?;
        match &mut *h {
            HandleInner::File(h) => match h.writer() {
                Some(writer) => {
                    writer.write(off, data.clone()).await.map_err(|e| {
                        error!("write failed: {:?}", e);
                        e.fs_err()
                    })?;
                    self.reader.invalidate(
                        ino,
                        Frange {
                            off,
                            len: data.len() as u64,
                        },
                    );
                    let mut modified = self.last_modified.write();
                    modified.insert(ino, Utc::now());
                    Ok(())
                }
                None => return Err(Errno::EBADF),
            },
            HandleInner::Dir(_) => Err(Errno::EISDIR),
            HandleInner::Control(_) => todo!(),
        }
    }
}

/// other functions
impl Vfs {
    pub fn config(&self) -> &Config {
        &self.vfs.conf
    }
    
    pub fn meta_format(&self) -> Arc<Format> {
        self.vfs.meta.get_format()
    }

    pub fn modified_since(&self, ino: &Ino) -> bool {
        match fs::task_local::start() {
            Some(start) => match self.last_modified.read().get(ino) {
                Some(modified) => *modified > start,
                None => false,
            },
            None => return false,
        }
    }

    /// When the attr obtained from the metadata, it may not be the latest,
    /// and there is still unrefreshed data in memory.
    pub fn update_len(&self, ino: Ino, attr: &mut Attr) {
        if attr.full && attr.typ == INodeType::File {
            let length = self.writer.len(ino);
            if length > attr.length {
                attr.length = length;
            }
            self.reader.truncate(ino, attr.length);
        }
    }
}
