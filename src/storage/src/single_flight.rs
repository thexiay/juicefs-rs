use std::{collections::HashMap, ops::AsyncFnOnce, sync::Arc};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use snafu::whatever;
use tokio::sync::Notify;
use crate::error::Result;

struct Request {
    /// This lock to modify the val when necessary
    val: RwLock<Result<Bytes>>,
    wait_group: Notify,
}

pub struct SingleFlight {
    /// this lock to make sure the request is unique
    requests: Mutex<HashMap<String, Arc<Request>>>,
}

impl SingleFlight {
    pub fn new() -> Self {
        SingleFlight {
            requests: Mutex::new(HashMap::new()),
        }
    }

    /// execute and get Bytes
    /// TODO: modify key and val to generic type
    pub async fn execute<F>(&self, key: &str, f: F) -> Result<Bytes>
    where
        F: AsyncFnOnce() -> Result<Bytes>,
    {
        let mut _waiter_holder = None;
        let (request, waiter) = {
            let mut lock = self.requests.lock();
            if let Some(request) = lock.get(key) {
                _waiter_holder = Some(request.clone());
                (request.clone(), _waiter_holder.as_ref().map(|r| r.wait_group.notified()))
            } else {
                let request = Arc::new(Request {
                    val: RwLock::new(Ok(Bytes::new())),
                    wait_group: Notify::new(),
                });
                lock.insert(key.to_string(), request.clone());
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
            Err(e) => whatever!("single flight error: {}", e),
        }
    }
}