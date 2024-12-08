use std::{
    collections::HashMap, ops::Deref, sync::Arc, time::{Duration, SystemTime}
};

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};
use tracing::{debug, warn};

use crate::api::{Attr, Ino, Slice};

pub const INVALIDATE_ALL_CHUNK: u32 = 0xFFFFFFFF;
pub const INVALIDATE_ATTR_ONLY: u32 = 0xFFFFFFFF;

#[derive(Default)]
/// Two situation to hold an openfile:
/// - Open it.
/// - Modify it.
pub struct OpenFile {
    expire: Arc<Duration>,

    attr: Option<Attr>,
    refs: i32,  // refs is the number of open fileholders.
    last_check: Duration,
    first: Option<Vec<Slice>>,
    chunks: HashMap<u32, Vec<Slice>>,
}

impl OpenFile {
    pub fn put_attr(&mut self, attr: &mut Attr) {
        if self.attr.is_some() {
            debug!("put attr to a non empty cache");
        }

        if let Some(cached_attr) = &self.attr
            && cached_attr.mtime == attr.mtime
        {
            attr.keep_cache = cached_attr.keep_cache;
        } else {
            // already modify, clear cache.
            self.invalidate_chunk(INVALIDATE_ALL_CHUNK);
        }
        self.attr = Some(attr.clone());
        self.attr.as_mut().unwrap().keep_cache = true;
        self.last_check = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards");
    }

    pub fn get_attr(&self) -> Option<Attr> {
        if *self.expire == Duration::ZERO {
            return None;
        }
        if self.attr.is_some()
            && SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                - self.last_check
                < *self.expire
        {
            Some(self.attr.as_ref().unwrap().clone())
        } else {
            None
        }
    }

    pub fn open(&mut self) {
        self.refs += 1;
    }

    pub fn is_open(&self) -> bool {
        self.refs > 0
    }
    
    pub fn close(&mut self) {
        self.refs -= 1;
    }

    pub fn invalidate_chunk(&mut self, indx: u32) {
        match indx {
            INVALIDATE_ALL_CHUNK => {
                self.first = None;
                self.chunks.clear();
            },
            0 => self.first = None,
            _ => {
                self.chunks.remove(&indx);
            }
        }
        self.last_check = Duration::ZERO;
    }

    pub fn read_chunk(&self, indx: u32) -> Option<Vec<Slice>> {
        if indx == 0 {
            self.first.as_ref().map(|slices| slices.clone())
        } else {
            self.chunks.get(&indx).map(|v| v.clone())
        }
    } 
}

pub struct OpenFileChunkGuard<'a> {
    guard: MutexGuard<'a, OpenFile>,
    indx: u32,
}

impl<'a> OpenFileChunkGuard<'a> {
    pub fn new(guard: MutexGuard<'a, OpenFile>, indx: u32) -> Self {
        OpenFileChunkGuard { guard, indx }
    }
}

impl<'a> Deref for OpenFileChunkGuard<'a> {
    type Target = MutexGuard<'a, OpenFile>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> Drop for OpenFileChunkGuard<'a> {
    fn drop(&mut self) {
        self.guard.invalidate_chunk(self.indx);
    }
}

/// OpenFiles is a cache for open files.
/// It also hold a lock for every open file, once it is open, other open request will wait until the lock is released.
pub struct OpenFiles {
    expire: Arc<Duration>,
    limit: u64,
    files: Mutex<HashMap<Ino, Arc<AsyncMutex<OpenFile>>>>,
}

impl OpenFiles {
    pub fn new(expire: Duration, limit: u64) -> Arc<Self> {
        let of = Arc::new(OpenFiles {
            expire: Arc::new(expire),
            limit,
            files: Mutex::new(HashMap::new()),
        });
        let of_clone = of.clone();
        tokio::spawn(async move {
            OpenFiles::cleanup(&of_clone).await;
        });
        of
    }

    pub fn lock(&self, ino: Ino) -> Arc<AsyncMutex<OpenFile>> {
        let mut files = self.files.lock();
        files
            .entry(ino)
            .or_insert(Arc::new(AsyncMutex::new(OpenFile::default())))
            .clone()
    }

    pub async fn has_any_open(&self, ino: Ino) -> bool {
        let file = self.lock(ino);
        let of = file.lock().await;
        of.is_open()
    }

    async fn cleanup(&self) {}
}
