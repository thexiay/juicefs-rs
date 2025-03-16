use std::{collections::HashMap, path::Path, sync::{atomic::Ordering, Arc}};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use juice_meta::{api::{Attr, Entry, Fcntl, Ino, OFlag, O_ACCMODE}, context::Uid};
use tokio::sync::RwLock as AsyncRwLock;

use crate::{internal::CONTROL_INODE, reader::FileReaderRef, vfs::{Vfs, VfsInner}, writer::FileWriterRef};

pub type Fh = u64;
/// Proxy of [`FileReader`] and [`FileWriter`]
/// Can handle write and read progres bar.
pub struct FileHandle {
    inode: Ino,
    fh: u64,

    // for dir
    pub(crate) children: Vec<Entry>,
    pub(crate) indexs: HashMap<String, usize>,
    read_at: DateTime<Utc>,
    pub(crate) read_off: usize,

    // for file
    flags: OFlag,
    is_recovered: bool,
    pub(crate) bsd_lock_owner: Option<u64>,
    pub(crate) ofd_lock_owner: Option<u64>,
    pub(crate) reader: Option<FileReaderRef>,
    pub(crate) writer: Option<FileWriterRef>,
}

impl FileHandle {
    pub fn new(ino: Ino, fh: Fh, flags: OFlag, is_recovered: bool) -> Self {
        todo!()
    }

    pub fn fh(&self) -> Fh {
        self.fh
    }
}


struct InnerFileHandle {
    // internal files
    off: u64,
    data: Bytes,
    pending: Bytes,
}


impl VfsInner {
    pub fn new_handle(&self, ino: Ino, flags: OFlag) -> Arc<AsyncRwLock<FileHandle>> {
        let low_bits = if OFlag::O_RDONLY.contains(flags) {
            // read only
            1
        } else {
            0
        };
        while self
            .ino_mapping
            .get(&self.next_fh.load(Ordering::SeqCst))
            .is_some()
            || self.next_fh.load(Ordering::SeqCst) & 1 != low_bits
        {
            self.next_fh
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst); // skip recovered fd
        }
        let fh = self
            .next_fh
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let handle = Arc::new(AsyncRwLock::new(FileHandle::new(ino, fh, flags, false)));
        self.handles
            .entry(ino)
            .or_insert_with(DashMap::new)
            .insert(fh, handle.clone());
        handle
    }

    pub fn find_handle(&self, ino: Ino, fh: Fh) -> Option<Arc<AsyncRwLock<FileHandle>>> {
        self
            .handles
            .get(&ino)
            .and_then(|map| map.get(&fh).map(|entry| entry.value().clone()))
    }

    pub fn find_all_handle(&self, ino: Ino) -> Vec<Arc<AsyncRwLock<FileHandle>>> {
        self.handles
            .get(&ino)
            .map(|map| map.iter().map(|entry| entry.value().clone()).collect())
            .unwrap_or_default()
    }

    pub fn release_handle(&self, ino: Ino, fh: Fh) {
        self.handles.get(&ino).map(|map| map.remove(&fh));
        self.handles.retain(|_, v| v.len() > 0);
    }

    pub async fn new_file_handle(&self, ino: Ino, length: u64, flags: OFlag) -> Fh {
        let h = self.new_handle(ino, flags);
        let mut h = h.write().await;
        match flags.intersection(O_ACCMODE) {
            OFlag::O_RDONLY => {
                h.reader = Some(self.reader.clone().open(ino, length));
            }
            OFlag::O_WRONLY | OFlag::O_RDWR => {
                h.reader = Some(self.reader.clone().open(ino, length));
                h.writer = Some(self.writer.clone().open(ino, length));
            }
            _ => {}
        }
        h.fh()
    }

    pub async fn cache_dir_entry(&self, parent: Ino, name: &str, entry: Option<(Ino, Attr)>) {
        let handles = self.find_all_handle(parent);
        for handle in handles {
            let mut handle = handle.write().await;
            if !handle.children.is_empty() && !handle.indexs.is_empty() {
                if let Some((ino, attr)) = &entry {
                    handle.children.push(Entry {
                        inode: *ino,
                        name: name.to_string(),
                        attr: attr.clone(),
                    });
                } else {
                    let index = handle.indexs.remove(name);
                    if let Some(index) = index {
                        handle.children[index].inode = 0; // invalid
                        if index >= handle.read_off {
                            // not read yet, remove it
                            handle.children.remove(index);
                        }
                    }
                }
            }
        }
    }

    pub async fn dump_all_handles(&self, path: &Path) {}

    pub async fn load_all_handles(&self, path: &Path) {}
}
