use std::{collections::HashMap, hash::{DefaultHasher, Hasher}, sync::{atomic::{AtomicBool, Ordering}, Arc, LazyLock}, thread::sleep, time::Duration};

use ctor::{ctor, dtor};
use juice_meta::{api::{new_client, Meta}, config::Config};

mod base_test;

// TODO: init redis environment in docker container

static REDIS_DB_HOLDER: LazyLock<RedisDbOffer> =  LazyLock::new(|| {
    RedisDbOffer::new(16)
});

struct RedisDbOffer {
    redis_url: String,
    db_nums: u32,
    db_used: HashMap<u32, Arc<AtomicBool>>,
}

impl RedisDbOffer {
    fn new(num: u32) -> Self {
        let mut db_used = HashMap::new();
        for i in 0..num {
            db_used.insert(i, Arc::new(AtomicBool::new(false)));
        }
        RedisDbOffer {
            redis_url: "redis://localhost:6379".to_string(),
            db_nums: num,
            db_used,
        }
    }
    
    fn take(&self) -> (RedisDbHodler, String) {
        loop {
            for i in 0..self.db_nums {
                if self.db_used[&i].compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                    return (RedisDbHodler(self.db_used[&i].clone()), format!("{}/{}", self.redis_url, i));
                }
            }
            sleep(Duration::from_secs(10));
        }
    }
}

struct RedisDbHodler(Arc<AtomicBool>);

impl Drop for RedisDbHodler {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

fn new_redis_client() -> (Box<dyn Meta>, RedisDbHodler) {
    let db_offer = LazyLock::force(&REDIS_DB_HOLDER);
    let (db_holder, redis_url) = db_offer.take();
    (new_client(redis_url, Config::default()).unwrap(), db_holder)
}

#[tokio::test]
async fn test_meta_client() {
    let (meta, _) = new_redis_client();
    base_test::test_meta_client(meta).await;
}

async fn test_truncate_and_delete(meta: Box<dyn Meta>) {}

async fn test_trash(meta: Box<dyn Meta>) {}

async fn test_parents(meta: Box<dyn Meta>) {}

async fn test_remove(meta: Box<dyn Meta>) {
    let (meta, _) = new_redis_client();
    base_test::test_remove(meta).await;
}

async fn test_resolve(meta: Box<dyn Meta>) {

}

async fn test_sticky_bit(meta: Box<dyn Meta>) {}

async fn test_locks(meta: Box<dyn Meta>) {}

async fn test_list_locks(meta: Box<dyn Meta>) {}

async fn test_concurrent_write(meta: Box<dyn Meta>) {}

async fn test_compaction<M: Meta>(meta: M, flag: bool) {}

async fn test_copy_file_range(meta: Box<dyn Meta>) {}

async fn test_close_session(meta: Box<dyn Meta>) {}

async fn test_concurrent_dir(meta: Box<dyn Meta>) {}

async fn test_attr_flags(meta: Box<dyn Meta>) {}

async fn test_quota(meta: Box<dyn Meta>) {}

async fn test_atime(meta: Box<dyn Meta>) {}

async fn test_access(meta: Box<dyn Meta>) {}

async fn test_open_cache(meta: Box<dyn Meta>) {}

async fn test_case_incensi(meta: Box<dyn Meta>) {}

async fn test_check_and_repair(meta: Box<dyn Meta>) {}

async fn test_dir_stat(meta: Box<dyn Meta>) {}

async fn test_clone(meta: Box<dyn Meta>) {}

async fn test_acl(meta: Box<dyn Meta>) {}

async fn test_read_only(meta: Box<dyn Meta>) {}