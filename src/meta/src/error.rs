use std::backtrace::Backtrace;

use deadpool::managed::PoolError;
use redis::RedisError;
use snafu::Snafu;

use crate::api::{Attr, Ino};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MyError {
    #[snafu(display("An system error occurred: {code}"))]
    SysError {
        code: i32,
    },
    #[snafu(display("An connection error occurred: {:?}", source))]
    ConnectionError {
        source: PoolError<RedisError>,
    },
    #[snafu(display("An redis error occurred: {:?}", source), context(false))]
    RedisError {
        source: redis::RedisError,
    },
    #[snafu(display("An redis error occurred: {:?}, detail: {detail}", source))]
    RedisDetailError {
        detail: String,
        source: redis::RedisError,
    },
    #[snafu(display("Txn empty key"))]
    EmptyKeyError,
    SendError,
    /// serde error
    #[snafu(context(false))]
    SerdeJsonError {
        source: serde_json::Error,
        backtrace: Backtrace
    },
    #[snafu(context(false))]
    SerdeBincodeError {
        source: bincode::Error,
        backtrace: Backtrace
    },

    // ----------------- logic error  -------------------
    #[snafu(display("database is not formatted, please run `juicefs format ...` first"))]
    NotInitializedError,
    #[snafu(display("incompatible metadata version: {version}, please upgrade the client"))]
    NotIncompatibleClientError {
        version: i32,
    },
    #[snafu(display("exist file ino: {ino}, attr: {attr:?}"))]
    FileExistError {
        ino: Ino,
        attr: Attr,
    },
    #[snafu(display("unknown driver: {driver}"))]
    DriverError {
        driver: String,
    },
    #[snafu(display("ino cann't found in db: {ino}"))]
    NotFoundInoError {
        ino: Ino,
    },
    #[snafu(display("message queue closed"))]
    SemaphoraCloseError,
    
    
}

pub type Result<T> = std::result::Result<T, MyError>;
