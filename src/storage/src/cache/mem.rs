use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;
use either::Either;
use opendal::Buffer;

use super::{CacheCntAndSize, CacheKey, CacheManager};
use crate::{buffer::FileBuffer, error::Result};

pub struct MemCacheManager {
    map: DashMap<CacheKey, Either<Buffer, FileBuffer>>,
    used_memory: AtomicI64,
}

impl MemCacheManager {
    pub fn new() -> Self {
        MemCacheManager {
            map: DashMap::new(),
            used_memory: AtomicI64::new(0),
        }
    }
}

impl CacheManager for MemCacheManager {
    async fn put(
        &self,
        key: &CacheKey,
        p: Either<Buffer, FileBuffer>,
        force: bool,
    ) -> Result<()> {
        self.used_memory
            .fetch_add(either::for_both!(&p, s => s.len()) as i64, Ordering::Relaxed);
        if let Some(v) = self.map.insert(key.clone(), p) {
            self.used_memory
                .fetch_sub(either::for_both!(&v, s => s.len()) as i64, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn remove(&self, key: &CacheKey) {
        if let Some((_, v)) = self.map.remove(key) {
            self.used_memory
                .fetch_sub(either::for_both!(&v, s => s.len()) as i64, Ordering::Relaxed);
        }
    }

    async fn get(&self, key: &CacheKey) -> Result<Option<Either<Buffer, FileBuffer>>> {
        Ok(self.map.get(key).map(|v| v.value().clone()))
    }

    fn stats(&self) -> CacheCntAndSize {
        (self.map.len() as i64, self.used_memory())
    }

    fn used_memory(&self) -> i64 {
        self.used_memory.load(Ordering::Relaxed)
    }

    fn is_invalid(&self) -> bool {
        true
    }
}
