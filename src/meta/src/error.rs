use std::{
    fmt::{Display, Formatter},
    ops::Deref,
};

use deadpool::managed::PoolError;
use redis::RedisError;
use snafu::{FromString, GenerateImplicitData, Snafu};
use tracing::{error, Span};

use crate::{api::{Attr, Ino}, context::Uid};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), display("{source}\n{span:?}:{loc}"))]
pub struct MetaError {
    source: MetaErrorEnum,
    #[snafu(implicit)]
    loc: snafu::Location,
    #[snafu(implicit)]
    span: SpanGuard,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum MetaErrorEnum {
    // ----------------- syscall error  -------------------
    #[snafu(display("No such file or directory: {parent},{name}."))]
    NoEntryFound {
        parent: Ino,
        name: String,
    },
    #[snafu(display("No such file or directory: {ino}."))]
    NoEntryFound2 {
        ino: Ino,
    },
    #[snafu(display("Entry alreay exists: {parent},{name}, exist ino: {exist_ino}"))]
    EntryExists {
        parent: Ino,
        name: String,
        exist_ino: Ino,
        exist_attr: Option<Attr>,
    },
    #[snafu(display("Entry alreay exists: {ino}"))]
    EntryExists2 {
        ino: Ino,
        exist_attr: Option<Attr>,
    },
    #[snafu(display("Bad file descriptor: {fd}"))]
    BadFD {
        fd: Ino,
    },
    #[snafu(display("Directory not empty: {parent},{name}"))]
    DirNotEmpty {
        parent: Ino,
        name: String,
    },
    #[snafu(display("Not a directory: {parent},{name}"))]
    NotDir1 {
        parent: Ino,
        name: String,
    },
    #[snafu(display("Not a directory: {ino}"))]
    NotDir2 {
        ino: Ino,
    },
    #[snafu(display("Invalid Argument: {arg}"))]
    InvalidArg {
        arg: String,
    },
    #[snafu(display("Interrupted system call"))]
    Interrupted,
    #[snafu(display("Operation not supported: {op}"))]
    OpNotSupported {
        op: String,
    },
    #[snafu(display("Permission denied: {ino}"))]
    PermissionDenied {
        ino: Ino,
    },
    #[snafu(display("Operation not permitted: {op}"))]
    OpNotPermitted {
        op: String,
    },
    #[snafu(display("Read-only file system"))]
    ReadFS,
    #[snafu(display("Disk quota exceeded"))]
    QuotaExceeded,
    #[snafu(display("No space left on device"))]
    NoSpace,
    #[snafu(display("Broken pipe: {ino}"))]
    BrokenPipe {
        ino: Ino
    },
    #[snafu(display("No such attribute."))]
    NoSuchAttr,

    // ----------------- other error  -------------------
    #[snafu(display("An connection error occurred: {:?}", source))]
    ConnectionError {
        source: PoolError<RedisError>,
    },
    #[snafu(display("An redis error occurred: {:?}", source), context(false))]
    RedisError {
        source: redis::RedisError,
    },
    #[snafu(display("Redis err: {:?}, detail: {detail}", source))]
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
    },
    #[snafu(
        display("An bincode serde error occurred: {:?}", source),
        context(false)
    )]
    SerdeBincodeError {
        source: bincode::Error,
    },
    
    // init error
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
    #[snafu(display("cannot upgrade format: {detail}"))]
    UpgradeFormatError {
        detail: String,
    },
    // other
    #[snafu(display("Rename same error"))]
    RenameSameInoError,
    #[snafu(display("message queue closed"))]
    SemaphoraCloseError,
    #[snafu(whatever, display("{message}, cause: {source:?}"))]
    GenericError {
        message: String,

        // Having a `source` is optional, but if it is present, it must
        // have this specific attribute and type:
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl MetaError {
    pub fn inner(&self) -> &MetaErrorEnum {
        &self.source
    }

    pub fn is_no_entry_found(&self, parent: &Ino, name: &str) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::NoEntryFound { parent: parent_, name: name_ }
            if parent == parent_ && name == name_
        )
    }

    pub fn is_no_entry_found2(&self, ino: &Ino) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::NoEntryFound2 { ino: ino_ }
            if ino == ino_
        )
    }

    pub fn is_entry_exists(&self, parent: &Ino, name: &str) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::EntryExists { parent: parent_, name: name_, .. }
            if parent == parent_ && name == name_
        )
    }

    pub fn is_entry_exists2(&self, ino: &Ino) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::EntryExists2 { ino: ino_, .. }
            if ino == ino_
        )
    }

    pub fn is_dir_not_empty(&self, parent: &Ino, name: &str) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::DirNotEmpty { parent: parent_, name: name_ }
            if parent == parent_ && name == name_
        )
    }

    pub fn is_not_dir1(&self, parent: &Ino, name: &str) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::NotDir1 { parent: parent_, name: name_ }
            if parent == parent_ && name == name_
        )
    }

    pub fn is_not_dir2(&self, ino: &Ino) -> bool {
        matches!(self.inner(), MetaErrorEnum::NotDir2 { ino: ino_ } if ino == ino_)
    }

    pub fn is_invalid_arg(&self) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::InvalidArg { .. }
        )
    }

    pub fn is_op_not_supported(&self) -> bool {
        matches!(self.inner(), MetaErrorEnum::OpNotSupported { .. })
    }

    pub fn is_permission_denied(&self) -> bool {
        matches!(
            self.inner(),
            MetaErrorEnum::PermissionDenied { .. }
        )
    }

    pub fn is_op_not_permitted(&self) -> bool {
        matches!(self.inner(), MetaErrorEnum::OpNotPermitted { .. })
    }

    pub fn is_no_such_attr(&self) -> bool {
        matches!(self.inner(), MetaErrorEnum::NoSuchAttr)
    }
}

impl<E> From<E> for MetaError
where
    E: Into<MetaErrorEnum>,
{
    #[track_caller]
    fn from(error: E) -> Self {
        Self {
            source: error.into(),
            loc: GenerateImplicitData::generate(),
            span: GenerateImplicitData::generate(),
        }
    }
}

impl FromString for MetaError {
    type Source = Box<dyn std::error::Error + Send + Sync>;
    fn without_source(message: String) -> Self {
        MetaErrorEnum::GenericError { message, source: None }.into()
    }
    
    fn with_source(source: Self::Source, message: String) -> Self {
        MetaErrorEnum::GenericError { message, source: Some(source) }.into()
    }
}

pub type Result<T> = std::result::Result<T, MetaError>;

#[derive(Debug)]
pub struct SpanGuard(tracing::Span);

impl GenerateImplicitData for SpanGuard {
    fn generate() -> Self {
        SpanGuard(Span::current())
    }
}

#[derive(Debug)]
pub enum EntryKey {
    Ino(Ino),
    Name { parent: Ino, name: String },
}

impl Display for EntryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EntryKey::Ino(ino) => write!(f, "ino: {}", ino),
            EntryKey::Name { parent, name } => write!(f, "parent: {}, name: {}", parent, name),
        }
    }
}

impl PartialEq<(Ino, &String)> for EntryKey {
    fn eq(&self, other: &(Ino, &String)) -> bool {
        match self {
            EntryKey::Ino(_) => false,
            EntryKey::Name { parent, name } => *parent == other.0 && name == other.1,
        }
    }
}

impl PartialEq<Ino> for EntryKey {
    fn eq(&self, other: &Ino) -> bool {
        match self {
            EntryKey::Ino(ino) => ino == other,
            EntryKey::Name { .. } => false,
        }
    }
}

// for syscall
pub type Errno = i32;
pub type FsResult<T> = std::result::Result<T, Errno>;
impl From<MetaError> for Errno {
    fn from(e: MetaError) -> Self {
        error!("inner error: {}", e);
        match e.source {
            MetaErrorEnum::EntryExists { .. } => libc::EEXIST,
            MetaErrorEnum::NoEntryFound { .. } => libc::ENOENT,
            MetaErrorEnum::NoEntryFound2 { .. } => libc::ENOENT,
            MetaErrorEnum::BadFD { .. } => libc::EBADF,
            MetaErrorEnum::DirNotEmpty { .. } => libc::ENOTEMPTY,
            MetaErrorEnum::NotDir1 { .. } => libc::ENOTDIR,
            MetaErrorEnum::NotDir2 { .. } => libc::ENOTDIR,
            MetaErrorEnum::InvalidArg { .. } => libc::EINVAL,
            MetaErrorEnum::Interrupted => libc::EINTR,
            MetaErrorEnum::OpNotSupported { .. } => libc::ENOTSUP,
            MetaErrorEnum::PermissionDenied { .. } => libc::EACCES,
            MetaErrorEnum::OpNotPermitted { .. } => libc::EPERM,
            MetaErrorEnum::ReadFS => libc::EROFS,
            MetaErrorEnum::QuotaExceeded => libc::EDQUOT,
            MetaErrorEnum::NoSpace => libc::ENOSPC,
            _ => libc::EIO,
        }
    }
}
