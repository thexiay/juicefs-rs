use std::{collections::HashMap, ops::AsyncFnOnce, sync::Arc};

use bytes::Bytes;
use opendal::Buffer;
use parking_lot::{Mutex, RwLock};
use snafu::whatever;
use tokio::sync::Notify;
use crate::{cache::CacheKey, error::Result};

struct Request {
    /// This lock to modify the val when necessary
    val: RwLock<Result<Buffer>>,
    wait_group: Notify,
}

/// SingleFilght ensure that only one request is doing the real work event if 
/// there are many requests at the same time, those work all return same result.
pub struct SingleFlight {
    /// this lock to make sure the request is unique
    requests: Mutex<HashMap<CacheKey, Arc<Request>>>,
}

impl SingleFlight {
    pub fn new() -> Self {
        SingleFlight {
            requests: Mutex::new(HashMap::new()),
        }
    }

    /// execute and get Bytes
    /// TODO: modify key and val to generic type
    pub async fn execute<F>(&self, key: &CacheKey, f: F) -> Result<Buffer>
    where
        F: AsyncFnOnce() -> Result<Buffer>,
    {
        let mut _waiter_holder = None;
        let (request, waiter) = {
            let mut lock = self.requests.lock();
            if let Some(request) = lock.get(key) {
                _waiter_holder = Some(request.clone());
                (request.clone(), _waiter_holder.as_ref().map(|r| r.wait_group.notified()))
            } else {
                let request = Arc::new(Request {
                    val: RwLock::new(Ok(Buffer::new())),
                    wait_group: Notify::new(),
                });
                lock.insert(key.clone(), request.clone());
                (request, None)
            }
        };
        
        if let Some(waiter) = waiter {
            waiter.await;
        } else {
            let result = f().await;
            {
                let mut val = request.val.write();
                *val = result;
                request.wait_group.notify_waiters();
            } 
            self.requests.lock().remove(key);
        }

        let result = request.val.read();
        match &*result {
            Ok(bytes) => Ok(bytes.clone()),
            Err(e) => whatever!("single flight error: {}", e),  // TODO: clone storage Error
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::{atomic::{AtomicU32, Ordering}, Arc}};

    use opendal::Buffer;
    use rand::Rng;
    use tokio::{sync::Barrier, task::JoinSet};
    use tracing_test::traced_test;

    use crate::{cache::CacheKey, single_flight::SingleFlight};


    #[traced_test]
    #[tokio::test]
    async fn test_single_flight() {
        // 启动100000个协程去
        let controller = Arc::new(SingleFlight::new());
        let n = Arc::new(AtomicU32::new(0));
        let routines = 100000;
        let barrier = Arc::new(Barrier::new(routines + 1));
        let mut alloced_buffers: HashMap<usize, Buffer> = HashMap::new();
        let mut tasks = JoinSet::new();
        for i in 0..routines {
            let key = CacheKey::new(i as u64 / 100, 0, 0);
            let controller = controller.clone();
            let n = n.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                let buffer = controller.execute(&key, async || {
                    // sleep 500 milliseconds to ensure other requests come in and there exist a pending
                    // request already
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    n.fetch_add(1, Ordering::SeqCst);
                    // ensure every routine has different buffer
                    let mut rng = rand::thread_rng();
                    let bytes: [u8; 10] = rng.gen();
                    Ok(Buffer::from(bytes.to_vec()))
                }).await;
                barrier.wait().await;
                (i, buffer)
            });
        }
        // wait 100000 goroutines finished
        barrier.wait().await;
        assert!(n.load(Ordering::SeqCst) == 1000, "singleflight doesn't take effect");

        while let Some(res) = tasks.join_next().await {
            assert!(res.is_ok());
            let (i, buffer) = res.unwrap();
            if alloced_buffers.contains_key(&i) {
                assert!(buffer.is_ok());
                // ensure every buffer is same as the first key return buffer
                assert!(alloced_buffers.get(&i).unwrap().to_vec() == buffer.unwrap().to_vec());
            } else {
                assert!(buffer.is_ok());
                alloced_buffers.insert(i, buffer.unwrap());
            }
        }
    }
}