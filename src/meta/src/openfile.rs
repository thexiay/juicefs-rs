use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use parking_lot::Mutex;

use crate::api::{Attr, Ino, Slice};

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
    files: Mutex<HashMap<Ino, OpenFile>>,
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

    pub fn open(&self, ino: Ino, attr: &mut Attr) {
        let mut guard = self.files.lock();
        let of = match guard.get_mut(&ino) {
            Some(of) => of,
            None => {
                guard.insert(ino, OpenFile::default());
                guard.get_mut(&ino).unwrap()
            }
        };
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

    async fn cleanup(&self) {
        
    }
}
