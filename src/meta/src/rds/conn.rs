use r2d2::ManageConnection;
use redis::{
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig}, cmd, Client, RedisError
};
use tokio::runtime::Handle;

pub struct RedisClient {
    client: Client,
    handle: Handle,
}

impl RedisClient {
    pub fn new(client: Client) -> Self {
        Self { 
            client, 
            handle: tokio::runtime::Handle::current()
        }
    }

    pub async fn get_connection(&self) -> Result<ConnectionManager, RedisError> {
        self.client.get_connection_manager_with_config(ConnectionManagerConfig::default()).await
    }
}

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


/// [`ManageConnection::connect`] and [`ManageConnection::is_valid`] are all called in separate thread, so here we 
/// can use tokio runtime to schedule async task
impl ManageConnection for RedisClient {
    type Connection = ExclusiveConnection;
    type Error = RedisError;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.handle.block_on(async {
            let conn = self.get_connection().await?;
            Ok(ExclusiveConnection(conn))
        })
    }
    
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.handle.block_on(async move {
            cmd("PING").query_async::<String>(&mut conn.0).await?;
            Ok(())
        })
    }

    /// notice if [`ConnectionManager`] is created, it will auto reconnect to server, so it's always not broken
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        false
    }
}
