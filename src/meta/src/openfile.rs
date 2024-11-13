use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};

use crate::api::{Attr, Ino, Slice};

pub const INVALIDATE_ALL_CHUNK: u32 = 0xFFFFFFFF;
pub const INVALIDATE_ATTR_ONLY: u32 = 0xFFFFFFFF;

#[derive(Default)]
pub struct OpenFile {
    attr: Attr,
    refs: i32,
    last_check: Duration,
    first: Vec<Slice>,
    chunks: HashMap<u32, Vec<Slice>>,
}

impl OpenFile {
    pub fn invalidate_chunk(&mut self) {
        self.first.clear();
        self.chunks.clear();
    }
}

/// OpenFiles is a cache for open files.
/// It also hold a lock for every open file, once it is open, other open request will wait until the lock is released.
pub struct OpenFiles {
    expire: Duration,
    limit: u64,
    files: Mutex<HashMap<Ino, Arc<AsyncMutex<OpenFile>>>>,
}

impl OpenFiles {
    pub fn new(expire: Duration, limit: u64) -> Arc<Self> {
        let of = Arc::new(OpenFiles {
            expire,
            limit,
            files: Mutex::new(HashMap::new()),
        });
        let of_clone = of.clone();
        tokio::spawn(async move {
            OpenFiles::cleanup(&of_clone).await;
        });
        of
    }

    /// Open a file. Hold a file locker.
    pub async fn open(&self, ino: Ino, attr: &mut Attr) {
        let of = {
            let mut files = self.files.lock();
            files
                .entry(ino)
                .or_insert(Arc::new(AsyncMutex::new(OpenFile::default())))
                .clone()
        };
        
        let mut of = of.lock().await;
        if attr.mtime == of.attr.mtime {
            attr.keep_cache = of.attr.keep_cache;
        } else {
            of.invalidate_chunk();
        }
        of.attr = attr.clone();
        of.attr.keep_cache = true;
        of.refs += 1;
        of.last_check = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards");
    }

    pub async fn is_open(&self, ino: Ino) -> bool {
        let file = {
            let files = self.files.lock();
            files.get(&ino).map(|file| file.clone())
        };
        match file {
            Some(file) => {
                let of = file.lock().await;
                of.refs > 0
            }
            None => false,
        }
    }

    /// Close a fileholder.
    /// Reentrant func.
    pub async fn close(&self, ino: Ino) -> bool {
        let file = {
            let files = self.files.lock();
            files.get(&ino).map(|file| file.clone())
        };
        if let Some(file) = file {
            let mut of = file.lock().await;
            of.refs -= 1;
            of.refs <= 0
        } else {
            true
        }
    }

    /// get attr in openfiles cache
    pub async fn get_attr(&self, ino: Ino) -> Option<Attr> {
        if self.expire == Duration::ZERO {
            return None;
        }
        let file = {
            let files = self.files.lock();
            files.get(&ino).map(|file| file.clone())
        };
        if let Some(of) = file {
            let of = of.lock().await;
            if SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                - of.last_check
                < self.expire
            {
                return Some(of.attr.clone());
            }
        }
        None
    }

    /// update attr in openfiles cache
    pub async fn update_attr(&self, ino: Ino, mut attr: Attr) {
        let file = {
            let files = self.files.lock();
            files.get(&ino).map(|file| file.clone())
        };
        if let Some(of) = file {
            let mut of = of.lock().await;
            if attr.mtime != of.attr.mtime {
                of.invalidate_chunk();
            } else {
                attr.keep_cache = of.attr.keep_cache;
            }
            of.attr = attr;
            of.last_check = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards");
        }
    }

    async fn cleanup(&self) {}

    pub fn find(&self, ino: Ino) -> Option<Arc<AsyncMutex<OpenFile>>> {
        let files = self.files.lock();
        files.get(&ino).map(|file| file.clone())
    }

    pub async fn invalid(&self, ino: Ino, indx: u32) {
        let file = {
            let files = self.files.lock();
            files.get(&ino).map(|file| file.clone())
        };
        if let Some(file) = file {
            let mut of = file.lock().await;
            match indx {
                INVALIDATE_ALL_CHUNK => of.invalidate_chunk(),
                0 => of.first.clear(),
                _ => {
                    of.chunks.remove(&indx);
                }
            }
            of.last_check = Duration::ZERO;
        }
    }
}
