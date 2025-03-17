use std::{
    collections::HashMap,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::Stream;
use futures_async_stream::{stream, try_stream};

use juice_meta::{
    api::{
        Attr, Entry, Fcntl, INodeType, Ino, Meta, ModeMask, OFlag, StatFs, MAX_FILE_LEN,
        MAX_FILE_NAME_LEN, O_ACCMODE, ROOT_INODE,
    },
    context::{FsContext, Gid, Uid, WithContext},
};
use juice_storage::api::CachedStore;
use nix::errno::Errno;
use parking_lot::RwLock;
use tracing::error;

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
        todo!()
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

/// Here's the function of vfs:
/// 1. Interface calls that wrap metadata and data
/// 2. Handle different inode:
///     - special control inode
///     - file inode
///     - directory inode
/// 3. Pre-check such as file length, etc.
impl Vfs {
    pub async fn get_attr(&self, ino: Ino, opened: u8) -> Result<Entry, Errno> {
        todo!()
    }

    pub async fn set_attr(&self, ino: Ino, set: isize, fh: Fh, attr: Attr) -> Result<Entry, Errno> {
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
        path: &str,
    ) -> Result<Entry, Errno> {
        if parent == ROOT_INODE && self.is_special_node(name) {
            return Err(Errno::EEXIST);
        }

        if name.len() > MAX_FILE_NAME_LEN {
            return Err(Errno::ENAMETOOLONG);
        }

        let (ino, attr) = self
            .meta
            .mknod(parent, name, r#type, mode, cumask, rdev, path)
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

    pub async fn open(&self, ino: Ino, flags: OFlag) -> Result<(Fh, Attr), Errno> {
        if VfsInner::is_special_inode(ino) {
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
        if VfsInner::is_special_inode(ino) {
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
                let mut last_modified = self.last_modified.write();
                last_modified.insert(ino, Utc::now());
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
        if VfsInner::is_special_inode(ino) {
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

    pub async fn fsync(&self, ino: Ino, fh: Fh, datasync: bool) -> Result<(), Errno> {
        if VfsInner::is_special_inode(ino) {
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

    #[try_stream(boxed, ok = Entry, error = Errno)]
    pub async fn readdir(&self, ino: Ino, fh: Fh, off: u64) {
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
                        },
                        None => break,
                    }
                }
            }
            _ => return Err(Errno::ENOTDIR),
        }
    }

    pub async fn read(&self, ino: Ino, fh: Fh, off: u64, size: u64) -> Result<Bytes, Errno> {
        if VfsInner::is_special_inode(ino) {
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
                let _ = reader.read(off, &mut buf).await.map_err(|e| {
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

        if ino == CONTROL_INODE {
            // 拼装消息并且处理控制节点的消息
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

impl VfsInner {
    /// When the attr obtained from the metadata, it may not be the latest,
    /// and there is still unrefreshed data in memory.
    fn update_len(&self, ino: Ino, attr: &mut Attr) {
        if attr.full && attr.typ == INodeType::File {
            let length = self.writer.len(ino);
            if length > attr.length {
                attr.length = length;
            }
            self.reader.truncate(ino, attr.length);
        }
    }
}
