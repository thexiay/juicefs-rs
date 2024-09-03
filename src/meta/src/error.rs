use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MyError {
    #[snafu(display("An system error occurred: {code}"))]
    SysError { code: i32 },
    #[snafu(display("An connection error occurred: {:?}", source))]
    ConnectionError { source: r2d2::Error },
    #[snafu(
        display("An redis error occurred: {:?}", source),
        context(false)
    )]
    RedisError { source: redis::RedisError },
    #[snafu(display("An redis error occurred: {:?}, detail: {detail}", source),)]
    RedisDetailError { 
        detail: String,
        source: redis::RedisError 
    },
    #[snafu(display("Txn empty key"))]
    EmptyKeyError,
    SendError,
    /// serde error
    #[snafu(context(false))]
    SerdeJsonError { source: serde_json::Error },
    #[snafu(context(false))]
    SerdeBincodeError { source: bincode::Error },
}

pub type Result<T> = std::result::Result<T, MyError>;
