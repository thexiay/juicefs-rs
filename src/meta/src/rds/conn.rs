use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed::{Manager, RecycleError};
use redis::{
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig}, Client, RedisError
};

pub struct RedisClient {
    client: Client,
    ping_number: AtomicUsize,
}

impl RedisClient {
    pub fn new(client: Client) -> Self {
        Self { 
            client, 
            ping_number: AtomicUsize::new(0),
        }
    }

    pub async fn get_connection(&self) -> Result<ConnectionManager, RedisError> {
        self.client.get_connection_manager_with_config(ConnectionManagerConfig::default()).await
    }
}

impl Manager for RedisClient {
    type Type = ExclusiveConnection;
    type Error = RedisError;
    
    async fn create(&self) -> Result<Self::Type, Self::Error> {
        self.get_connection().await.map(ExclusiveConnection)
    }
    
    async fn recycle(
        &self,
        conn: &mut Self::Type,
        _: &deadpool_redis::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        let ping_number = self.ping_number.fetch_add(1, Ordering::Relaxed).to_string();
        // Using pipeline to avoid roundtrip for UNWATCH
        let (n,) = redis::Pipeline::with_capacity(2)
            .cmd("UNWATCH")
            .ignore()
            .cmd("PING")
            .arg(&ping_number)
            .query_async::<(String,)>(conn)
            .await?;
        if n == ping_number {
            Ok(())
        } else {
            Err(RecycleError::message("Invalid PING response"))
        }
    }
}

/// The difference between [`ConnectionManager`] and [`ExclusiveConnection`] is that ExclusiveConnection
/// can't clone casual, it ensure it will not be abused between different threads.
pub struct ExclusiveConnection(ConnectionManager);

impl ConnectionLike for ExclusiveConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> redis::RedisFuture<'a, redis::Value> {
        self.0.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        self.0.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }
}
