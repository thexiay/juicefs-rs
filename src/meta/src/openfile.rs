use std::{
    collections::HashMap,
    ops::Deref,
    sync::Arc,
    time::{Duration, SystemTime},
};

use parking_lot::Mutex;
use tokio::sync::{MutexGuard, Mutex as AsyncMutex, RwLock as AsyncRwLock};

use crate::api::{Attr, Ino, Slice};

pub const INVALIDATE_ALL_CHUNK: u32 = 0xFFFFFFFF;
pub const INVALIDATE_ATTR_ONLY: u32 = 0xFFFFFFFF;

#[derive(Default)]
/// Two situation to hold an openfile:
/// - Open it.(Open, Close)
/// - Modify it(Truncate, Fallocate, Link, e.g.).
/// - Query it.(Read)
pub struct OpenFile {
    expire: Arc<Duration>,

    attr: Option<Attr>,
    refs: i32, // refs is the number of open fileholders.
    last_attr_valid_time: Duration,
    first: Option<Vec<Slice>>,
    chunks: HashMap<u32, Vec<Slice>>,
}

impl OpenFile {
    pub fn put_attr(&mut self, attr: &mut Attr) {
        if let Some(cached_attr) = &self.attr
            && cached_attr.mtime == attr.mtime
        {
            attr.keep_cache = cached_attr.keep_cache;
        } else {
            // already modify, clear cache.
            self.invalidate_chunk_cache(INVALIDATE_ALL_CHUNK);
        }
        self.attr = Some(attr.clone());
        self.attr.as_mut().unwrap().keep_cache = true;
        self.last_attr_valid_time = SystemTime::now()
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
                - self.last_attr_valid_time
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

    // 适用于写入后，或者truncate后，让内存中的chunks无效的场景
    pub fn invalidate_chunk_cache(&mut self, indx: u32) {
        match indx {
            INVALIDATE_ALL_CHUNK => {
                self.first = None;
                self.chunks.clear();
            }
            0 => self.first = None,
            _ => {
                self.chunks.remove(&indx);
            }
        }
        self.last_attr_valid_time = Duration::ZERO;
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
        self.guard.invalidate_chunk_cache(self.indx);
    }
}

/// OpenFiles is a cache for open files.
/// It also hold a lock for every open file, once it is open, other open request will wait until the lock is released.
/// TODO: consider files free
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

    async fn cleanup(&self) {
        // 需要及时释放，否则write和truncate都会因为找到ino的openfiles，从而导致需要去锁住。
    }
}

pub struct InvalidAttrGuard {
    ofs: Arc<OpenFiles2>,
    ino: Ino,
}

impl Deref for InvalidAttrGuard {
    type Target = OpenFiles2;

    fn deref(&self) -> &Self::Target {
        &self.ofs
    }
}

impl Drop for InvalidAttrGuard {
    fn drop(&mut self) {
        let mut ofs = self.ofs.files.lock();
        if let Some(of) = ofs.get_mut(&self.ino) {
            of.attr = None;
        }
    }
}

pub struct OpenFiles2 {
    expire: Option<Duration>,
    limit: u64,
    files: Mutex<HashMap<Ino, OpenFile2>>,
}

impl OpenFiles2 {
    pub fn new(expire: Option<Duration>, limit: u64) -> Arc<Self> {
        let of = Arc::new(OpenFiles2 {
            expire,
            limit,
            files: Mutex::new(HashMap::new()),
        });
        let of_clone = of.clone();
        // TODO: release thread
        tokio::spawn(async move {});
        of
    }

    pub fn remove_attr(&self, ino: Ino) {
        let mut files = self.files.lock();
        if let Some(of) = files.get_mut(&ino) {
            of.attr = None;
        }
    }

    pub fn chunks(&self, ino: Ino) -> Option<Arc<AsyncRwLock<HashMap<u32, Vec<Slice>>>>> {
        let mut ofs = self.files.lock();
        if let Some(of) = ofs.get_mut(&ino) {
            Some(of.chunks.clone())
        } else {
            None
        }
    }

    pub fn get_attr(&self, ino: Ino) -> Option<Attr> {
        match self.expire {
            Some(expire) => {
                let mut ofs = self.files.lock();
                if let Some(of) = ofs.get_mut(&ino) {
                    if let Some(attr) = &of.attr {
                        if SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .expect("Time went backwards")
                            - of.last_attr_valid_time
                            < expire
                        {
                            return Some(attr.clone());
                        }
                    }
                }
                None
            },
            None => None,
        }
    }

    // Cache attr and return if need to keep cache
    pub fn cache_attr(&self, ino: Ino, attr: &Attr) -> bool {
        let mut ofs = self.files.lock();
        let mut keep_cache = false;
        if let Some(of) = ofs.get_mut(&ino) {
            if let Some(cached_attr) = &of.attr {
                if cached_attr.mtime == attr.mtime {
                    // assume mtime consistent, data has not been changed
                    keep_cache = true;
                }
            }

            let mut attr_cloned = attr.clone();            
            keep_cache.then(|| {
                attr_cloned.keep_cache = true;
            });
            of.attr = Some(attr_cloned);
            of.last_attr_valid_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards");
        }
        keep_cache
    }

    pub fn open(&self, ino: Ino) {
        let mut files = self.files.lock();
        files
            .entry(ino)
            .and_modify(|of| {
                of.refs += 1;
            })
            .or_insert_with(|| OpenFile2 {
                attr: None,
                refs: 1,
                last_attr_valid_time: Duration::ZERO,
                chunks: Arc::new(AsyncRwLock::new(HashMap::new())),
            });
    }

    pub fn close(&self, ino: Ino) {
        let mut files = self.files.lock();
        if let Some(of) = files.get_mut(&ino) {
            of.refs -= 1;
            if of.refs == 0 {
                files.remove(&ino);
            }
        }
    }

    pub fn is_opened(&self, ino: Ino) -> bool {
        let ofs = self.files.lock();
        ofs.get(&ino).is_some()
    }
}

pub struct OpenFile2 {
    attr: Option<Attr>,
    refs: i32, // refs is the number of open fileholders.
    last_attr_valid_time: Duration,
    chunks: Arc<AsyncRwLock<HashMap<u32, Vec<Slice>>>>,
}