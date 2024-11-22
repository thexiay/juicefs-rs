#![feature(let_chains)]

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, LazyLock,
    },
    time::Duration,
};

use base_test::test_format;
use ctor::{ctor, dtor};
use juice_meta::{
    api::{new_client, Meta},
    config::Config,
};
use parking_lot::RwLock;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::{fmt::{self, time::OffsetTime}, layer::SubscriberExt, util::SubscriberInitExt};

mod base_test;

// TODO: init redis environment in docker container

#[ctor]
fn before_all() {
    let mut guard = REDIS_DB_HOLDER.write();
    *guard = Some(RedisDbOffer::new(16));
    // init logger
    let default_timer = OffsetTime::local_rfc_3339().unwrap_or_else(|e| {
        println!("failed to get local time offset, falling back to UTC: {}", e);
        OffsetTime::new(
            time::UtcOffset::UTC,
            time::format_description::well_known::Rfc3339,
        )
    });
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_timer(default_timer.clone())
        .with_ansi(true)
        .with_file(true)
        .with_line_number(true);
    tracing_subscriber::registry().with(fmt_layer).init();
}

#[dtor]
fn after_all() {}

static REDIS_DB_HOLDER: RwLock<Option<RedisDbOffer>> = RwLock::new(None);

/// Because rust cargo will concurrent run test cases, so we need a clear environment to run our test cases.
/// RedisDbOffer is a wrapper of redis db, [`RedisDbOffer::take`] will find a idle clear db for test.
/// 
/// Currently, start redis is manually, we can use docker to start redis in the future.E.G.
/// ```shell
/// docker run --name myredis --network host -d redis --requirepass "mypassword"
/// ```
struct RedisDbOffer {
    redis_url: String,
    db_nums: u32,
    // db_id -> (refs, inited)
    db_used: HashMap<u32, (Arc<AtomicBool>, AtomicBool)>,
}

impl RedisDbOffer {
    fn new(num: u32) -> Self {
        let mut db_used = HashMap::new();
        for i in 0..num {
            db_used.insert(
                i,
                (Arc::new(AtomicBool::new(false)), AtomicBool::new(false)),
            );
        }
        RedisDbOffer {
            redis_url: "redis://:mypassword@127.0.0.1:6379".to_string(),
            db_nums: num,
            db_used,
        }
    }

    async fn take(&self, config: Config) -> (Box<dyn Meta>, RedisDbHodler) {
        loop {
            for i in 0..self.db_nums {
                let redis_url = format!("{}/{}", self.redis_url, i);
                let client = new_client(redis_url.clone(), config.clone()).await;
                let (refs, inited) = &self.db_used[&i];
                if inited
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // only init once, first reset it.
                    info!("Init redis db: {redis_url}");
                    client.reset().await.expect("clean up db error");
                    client.init(test_format(), true).await.unwrap();
                }

                if refs
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    info!("Use redis db: {redis_url}");
                    client.load(true).await.unwrap();
                    return (client, RedisDbHodler(refs.clone()));
                }
            }
            info!("could not find avaiable db, wait 2 seconds");
            sleep(Duration::from_secs(2)).await;
            
        }
    }
}

struct RedisDbHodler(Arc<AtomicBool>);

impl Drop for RedisDbHodler {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn test_meta_client() {
    let guard = REDIS_DB_HOLDER.read();
    let (meta, _) = guard.as_ref().unwrap().take(Config::default()).await;
    base_test::test_meta_client(meta).await;
}

#[tokio::test]
async fn test_truncate_and_delete() {
    let guard = REDIS_DB_HOLDER.read();
    let (meta, _) = guard.as_ref().unwrap().take(Config::default()).await;
    base_test::test_truncate_and_delete(meta).await;
}

async fn test_trash() {}

async fn test_parents() {}

// TODO: wrap them with macro
#[tokio::test]
async fn test_remove() {
    let guard = REDIS_DB_HOLDER.read();
    let (meta, _) = guard.as_ref().unwrap().take(Config::default()).await;
    base_test::test_remove(meta).await;
}

async fn test_resolve() {}

async fn test_sticky_bit() {}

async fn test_locks() {}

async fn test_list_locks() {}

async fn test_concurrent_write() {}

async fn test_compaction<M: Meta>(meta: M, flag: bool) {}

async fn test_copy_file_range() {}

async fn test_close_session() {}

async fn test_concurrent_dir() {}

async fn test_attr_flags() {}

async fn test_quota() {}

async fn test_atime() {}

async fn test_access() {}

async fn test_open_cache() {}

async fn test_case_incensi() {}

async fn test_check_and_repair() {}

async fn test_dir_stat() {}

async fn test_clone() {}

async fn test_acl() {}

async fn test_read_only() {}
