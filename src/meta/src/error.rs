use std::backtrace::Backtrace;

use deadpool::managed::PoolError;
use redis::RedisError;
use snafu::Snafu;
use tracing::error;

use crate::api::{Attr, Ino};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MyError {
    #[snafu(display("An system error occurred: {code}"))]
    SysError {
        code: Errno,
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
    #[snafu(display("An bincode serde error occurred {}: {:?}", loc, source), context(false))]
    SerdeBincodeError {
        source: bincode::Error,
        backtrace: Backtrace,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    // ----------------- logic error  -------------------
    #[snafu(display("database is not formatted, please run `juicefs format ...` first"))]
    NotInitializedError,
    #[snafu(display("incompatible metadata version: {version}, please upgrade the client"))]
    NotIncompatibleClientError {
        version: i32,
    },
    #[snafu(display("unknown driver: {driver}"))]
    DriverError {
        driver: String,
    },
    // not found key in db
    #[snafu(display("exist file ino: {ino}, attr: {attr:?}"))]
    FileExistError {
        ino: Ino,
        attr: Attr,
    },
    #[snafu(display("cann't found ino({ino}) in db, loc({loc})."))]
    NotFoundInoError {
        ino: Ino,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
    #[snafu(display("cann't found entry({parent} {name}) in db, loc({loc})."))]
    NotFoundEntryError {
        parent: Ino,
        name: String,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
    // other
    #[snafu(display("message queue closed"))]
    SemaphoraCloseError,
    #[snafu(display("cannot upgrade format: {detail}"))]
    UpgradeFormatError {
        detail: String,
    },
    #[snafu(display("cannot deserialize rightfully from data, detail: {detail}"))]
    IllegalDataFormatError {
        detail: String,
    }
}

pub type Result<T> = std::result::Result<T, MyError>;
pub type Errno = i32;
pub type FsResult<T> = std::result::Result<T, Errno>;

impl From<MyError> for Errno {
    fn from(e: MyError) -> Self {
        match e {
            MyError::SysError { code } => code,
            other =>  {
                error!("error: {}\nstack: {}", other, Backtrace::capture());
                libc::EIO
            },
        }
    }
}

impl From<Errno> for MyError {
    fn from(e: Errno) -> Self {
        MyError::SysError { code: e }
    }
}
