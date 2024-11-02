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
    last_check: u64,
    first: Vec<Slice>,
    chunks: HashMap<u32, Vec<Slice>>,
}

impl OpenFile {
    pub fn invalidate_chunk(&mut self) {
        self.first.clear();
        self.chunks.clear();
    }
}

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

    pub async fn open(&self, ino: Ino, attr: &mut Attr) {
        let mut guard = self.files.lock();
        let arc_of = guard
            .entry(ino)
            .or_insert(Arc::new(AsyncMutex::new(OpenFile::default())));
        let mut of = arc_of.lock().await;
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
            .expect("Time went backwards")
            .as_secs();
    }

    pub async fn is_open(&self, ino: Ino) -> bool {
        let file = {
            let files = self.files.lock();
            files.get(&ino).map(|file| file.clone())
        };
        match file {
            Some(file)  => {
                let of = file.lock().await;
                of.refs > 0
            },
            None => false
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
            of.last_check = 0;
        }
    }
}
