use std::{
    collections::HashMap, path::Path, sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use juice_meta::{
    api::{Attr, Entry, Ino, Meta, OFlag, O_ACCMODE},
    context::{FsContext, Gid, Uid, WithContext},
};
use juice_storage::api::CachedStore;
use tokio::sync::RwLock as AsyncRwLock;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    fhandle::{Fh, FileHandle},
    reader::DataReader,
    writer::DataWriter,
};

const MIN_INTERNAL_NODE: Ino = 0x7FFFFFFF00000000;
const LOG_INODE: Ino = MIN_INTERNAL_NODE + 1;
const CONTROL_INODE: Ino = MIN_INTERNAL_NODE + 2;
const STATS_INODE: Ino = MIN_INTERNAL_NODE + 3;
const CONFIG_INODE: Ino = MIN_INTERNAL_NODE + 4;
const TRASH_INODE: Ino = juice_meta::api::TRASH_INODE;

#[derive(Clone)]
pub struct SharedVfs {
    vfs: Arc<Vfs>,
    context: FsContext,
}

pub struct Vfs {
    conf: Arc<Config>,
    meta: Arc<dyn Meta>,
    store: Arc<CachedStore>,
    reader: DataReader,
    writer: DataWriter,

    handles: DashMap<Ino, DashMap<Fh, Arc<AsyncRwLock<FileHandle>>>>,
    ino_mapping: DashMap<Fh, Ino>,
    next_fh: AtomicU64,
    last_modified: HashMap<Ino, DateTime<Utc>>,
}

impl Vfs {
    fn new_handle(&self, ino: Ino, flags: OFlag) -> Arc<AsyncRwLock<FileHandle>> {
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

    fn find_handle(&self, ino: Ino, fh: Fh) -> Option<Arc<AsyncRwLock<FileHandle>>> {
        let handle = self
            .handles
            .get(&ino)
            .and_then(|map| map.get(&fh).map(|entry| entry.value().clone()));
        match handle {
            Some(handle) => Some(handle),
            None => {
                if fh & 1 == 1 && ino != CONTROL_INODE {
                    let handle =
                        Arc::new(AsyncRwLock::new(FileHandle::new(ino, fh, OFlag::O_RDWR, true)));
                    self.handles
                        .entry(ino)
                        .or_insert_with(DashMap::new)
                        .insert(fh, handle.clone());
                    self.ino_mapping.insert(fh, ino);
                    Some(handle)
                } else {
                    None
                }
            }
        }
    }

    fn find_all_handle(&self, ino: Ino) -> Vec<Arc<AsyncRwLock<FileHandle>>> {
        self.handles
            .get(&ino)
            .map(|map| map.iter().map(|entry| entry.value().clone()).collect())
            .unwrap_or_default()
    }

    fn release_handle(&self, ino: Ino, fh: Fh) {
        self.handles.get(&ino).map(|map| map.remove(&fh));
        self.handles.retain(|_, v| v.len() > 0);
    }

    async fn new_file_handle(&self, ino: Ino, length: u64, flags: OFlag) -> Fh {
        let h = self.new_handle(ino, flags);
        let mut h = h.write().await;
        match flags.intersection(O_ACCMODE) {
            OFlag::O_RDONLY => {
                h.reader = Some(self.reader.open(ino, length));
            }
            OFlag::O_WRONLY | OFlag::O_RDWR => {
                h.reader = Some(self.reader.open(ino, length));
                h.writer = Some(self.writer.open(ino, length));
            },
            _ => {}
        }
        h.fh()
    }

    async fn release_file_handle(&self, ino: Ino, fh: Fh) {
        let h = self.find_handle(ino, fh);
        if let Some(h) = h {
            self.release_handle(ino, fh);
            h.write().await.close();
        }
    }

    async fn cache_dir_handle(&self, parent: Ino, name: String, entry: Option<(Ino, Attr)>) {
        let handles = self.find_all_handle(parent);
        // 注意这里只是当前所有parent的快照
        for handle in handles {
            let mut handle = handle.write().await;
            if !handle.children.is_empty() && !handle.indexs.is_empty() {
                if let Some((ino, attr)) = &entry {
                    handle.children.push(Entry {
                        inode: *ino,
                        name: name.clone(),
                        attr: attr.clone(),
                    });
                } else {
                    let index = handle.indexs.remove(&name);
                    if let Some(index) = index {
                        handle.children[index].inode = 0;  // invalid
                        if index >= handle.read_off {
                            // not read yet, remove it
                            handle.children.remove(index);
                        }
                    }
                }
            }
        }
    }

    
    async fn dump_all_handles(&self, path: &Path) {

    }

    async fn load_all_handles(&self, path: &Path) {

    }

}

impl WithContext for SharedVfs {
    fn with_login(&mut self, uid: Uid, gids: Vec<Gid>) {
        todo!()
    }

    fn with_cancel(&mut self, token: CancellationToken) {
        todo!()
    }

    fn uid(&self) -> &Uid {
        todo!()
    }

    fn gid(&self) -> &Gid {
        todo!()
    }

    fn gids(&self) -> &Vec<juice_meta::context::Gid> {
        todo!()
    }

    fn token(&self) -> &tokio_util::sync::CancellationToken {
        todo!()
    }

    fn check_permission(&self) -> bool {
        todo!()
    }
}
