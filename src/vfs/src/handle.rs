use std::{
    collections::HashMap,
    path::Path,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use juice_meta::api::{Attr, Entry, Ino, InoExt, OFlag};
use tokio::sync::{RwLock as AsyncRwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    error::{InvalidOFlagSnafu, Result},
    reader::FileReaderRef,
    vfs::Vfs,
    writer::FileWriterRef,
};

pub type Fh = u64;

pub enum HandleType {
    File,
    Dir,
    Control,
}

/// Proxy of [`FileReader`] and [`FileWriter`]
/// Can handle write and read progres bar.
pub struct Handle {
    inode: Ino,
    fh: u64,
    // fast check
    handle_type: HandleType,
    readable: bool,
    writeable: bool,

    handle: AsyncRwLock<HandleInner>,
}

impl Handle {
    pub fn fh(&self) -> Fh {
        self.fh
    }

    pub fn typ(&self) -> &HandleType {
        &self.handle_type
    }

    pub fn writable(&self) -> bool {
        self.writeable
    }

    pub fn readable(&self) -> bool {
        self.readable
    }

    pub async fn write(&self, accept_interrupt: bool) -> Option<RwLockWriteGuard<'_, HandleInner>> {
        // TODO: add interrupt break for cancel token
        // 针对不同的类型
        Some(self.handle.write().await)
    }

    pub async fn read(&self, accept_interrupt: bool) -> Option<RwLockReadGuard<'_, HandleInner>> {
        // TODO: add interrupt break for cancel token
        Some(self.handle.read().await)
    }
}

pub enum HandleInner {
    File(FileHandle),
    Dir(DirHandle),
    Control(ControlHandle),
}

enum FileHandleType {
    ReadOnlyFile {
        reader: FileReaderRef,
    },
    WRFile {
        reader: FileReaderRef,
        writer: FileWriterRef,
    },
}

pub struct FileHandle {
    flags: OFlag,
    bsd_lock_owner: Option<u64>,
    ofd_lock_owner: Option<u64>,
    handle: FileHandleType,
}

impl FileHandle {
    pub fn reader(&self) -> &FileReaderRef {
        match &self.handle {
            FileHandleType::ReadOnlyFile { reader } | FileHandleType::WRFile { reader, .. } => {
                reader
            }
        }
    }

    pub fn writer(&mut self) -> Option<&FileWriterRef> {
        match &self.handle {
            FileHandleType::WRFile { writer, .. } => Some(writer),
            _ => None,
        }
    }

    pub fn bsd_lock_owner(&self) -> Option<u64> {
        self.bsd_lock_owner
    }

    pub fn posix_lock_owner(&self) -> Option<u64> {
        self.ofd_lock_owner
    }

    pub fn free_posix_lock(&mut self) {
        self.ofd_lock_owner = None;
    }
}

pub struct DirHandle {
    read_dir:
        Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Vec<Entry>>> + Send>> + Send + Sync>,
    children: Vec<Entry>,
    indexs: HashMap<String, usize>,
    read_at: DateTime<Utc>,
    read_off_idx: usize, // current readdir offset in children
}

impl DirHandle {
    pub fn cache(&mut self, name: &str, entry: Option<(Ino, Attr)>) {
        if !self.children.is_empty() && !self.indexs.is_empty() {
            if let Some((inode, attr)) = entry {
                self.indexs.insert(name.to_string(), self.children.len());
                self.children.push(Entry {
                    inode,
                    name: name.to_string(),
                    attr,
                });
            } else {
                if let Some(idx) = self.indexs.remove(name) {
                    // only remove the entry if it's not read yet
                    if idx >= self.read_off_idx {
                        let total_len = self.children.len();
                        self.children.swap(idx, total_len - 1);
                        self.indexs.insert(self.children[idx].name.clone(), idx);
                        self.children.pop();
                    }
                }
            }
        }
    }

    pub async fn refresh(&mut self) -> Result<()> {
        self.read_at = Utc::now();
        self.read_off_idx = 0;
        self.children = (&self.read_dir)().await?;
        Ok(())
    }

    pub fn need_refresh(&self) -> bool {
        self.read_off_idx == 0
    }

    pub fn seek(&mut self, offset: u64) {
        self.read_off_idx = offset as usize;
    }

    pub fn read_once(&mut self) -> Option<Entry> {
        self.children.get(self.read_off_idx).map(|entry| {
            self.read_off_idx += 1;
            entry.clone()
        })
    }
}

pub struct ControlHandle {
    // internal files
    off: u64,
    data: Bytes,
    pending: Bytes,
}

impl Vfs {
    pub fn find_handle(&self, ino: Ino, fh: Fh) -> Option<Arc<Handle>> {
        self.handles
            .get(&ino)
            .and_then(|map| map.get(&fh).map(|entry| entry.value().clone()))
    }

    pub fn find_all_handle(&self, ino: Ino) -> Vec<Arc<Handle>> {
        self.handles
            .get(&ino)
            .map(|map| map.iter().map(|entry| entry.value().clone()).collect())
            .unwrap_or_default()
    }

    pub fn release_handle(&self, ino: Ino, fh: Fh) {
        self.handles.get(&ino).map(|map| map.remove(&fh));
        self.handles.retain(|_, v| v.len() > 0);
    }

    pub async fn new_handle(&self, ino: Ino, length: u64, flags: Option<OFlag>) -> Result<Fh> {
        while self
            .ino_mapping
            .get(&self.next_fh.load(Ordering::SeqCst))
            .is_some()
        {
            self.next_fh
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let fh = self
            .next_fh
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let handle_inner = match flags {
            Some(flags) => {
                let file_type = if flags.contains(OFlag::O_WRONLY) || flags.contains(OFlag::O_RDWR)
                {
                    FileHandleType::WRFile {
                        reader: self.reader.clone().open(ino, length),
                        writer: self.writer.clone().open(ino, length),
                    }
                } else if flags.contains(OFlag::O_RDONLY) {
                    FileHandleType::ReadOnlyFile {
                        reader: self.reader.clone().open(ino, length),
                    }
                } else {
                    return Err(InvalidOFlagSnafu.build().into());
                };
                HandleInner::File(FileHandle {
                    flags,
                    bsd_lock_owner: None,
                    ofd_lock_owner: None,
                    handle: file_type,
                })
            }
            None => {
                let vfs_cloned = self.clone();
                let ino_cloned = ino.clone();
                HandleInner::Dir(DirHandle {
                    read_dir: Box::new(move || {
                        let vfs = vfs_cloned.clone();
                        let ino = ino_cloned.clone();
                        Box::pin(async move {
                            let mut entries = match vfs.meta.readdir(ino, true).await {
                                Ok(entries) => entries,
                                Err(e) => {
                                    if e.is_permission_denied() {
                                        vfs.meta.readdir(ino, false).await?
                                    } else {
                                        return Err(e.into());
                                    }
                                }
                            };
                            if ino.is_root() && !vfs.conf.hide_internal {
                                entries.extend_from_slice(&vfs.internal_inos);
                            }

                            Ok(entries)
                        })
                    }),
                    children: Vec::new(),
                    indexs: HashMap::new(),
                    read_at: Utc::now(),
                    read_off_idx: 0,
                })
            }
        };
        let typ = match &handle_inner {
            HandleInner::File(_) => HandleType::File,
            HandleInner::Dir(_) => HandleType::Dir,
            HandleInner::Control(_) => HandleType::Control,
        };
        let writeable = matches!(
            &handle_inner, 
            HandleInner::File(f) if matches!(f.handle, FileHandleType::WRFile { .. })
        );
        let readable = true;
        let handle = Handle {
            inode: ino,
            fh,
            handle_type: typ,
            readable,
            writeable,
            handle: AsyncRwLock::new(handle_inner),
        };
        self.handles
            .entry(ino)
            .or_insert_with(DashMap::new)
            .insert(fh, Arc::new(handle));
        Ok(fh)
    }

    pub async fn cache_dir_entry(&self, parent: Ino, name: &str, entry: Option<(Ino, Attr)>) {
        let handles = self.find_all_handle(parent);
        for handle in handles {
            if let Some(ref mut handle) = handle.write(false).await {
                if let HandleInner::Dir(ref mut dir_handle) = **handle {
                    dir_handle.cache(name, entry.clone());
                }
            }
        }
    }

    pub async fn dump_all_handles(&self, path: &Path) {}

    pub async fn load_all_handles(&self, path: &Path) {}
}
