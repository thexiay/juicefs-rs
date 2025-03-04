use std::sync::Arc;

use juice_meta::error::MetaError;
use juice_storage::error::StorageError;
use snafu::{FromString, GenerateImplicitData, Snafu};
use tracing::Span;

#[derive(Debug)]
pub struct SpanGuard(tracing::Span);

impl GenerateImplicitData for SpanGuard {
    fn generate() -> Self {
        SpanGuard(Span::current())
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), display("{source}\n{span:?}:{loc}"))]
pub struct VfsError {
    source: VfsErrorEnum,
    #[snafu(implicit)]
    loc: snafu::Location,
    #[snafu(implicit)]
    span: SpanGuard,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum VfsErrorEnum {
    #[snafu(display("IO error: {source}"), context(false))]
    IoError { source: std::io::Error },
    #[snafu(display("IO error: {source}"))]
    IoDetailError {
        source: std::io::Error,
        path: Option<String>,
    },
    #[snafu(display("Meta error, {source}"), context(false))]
    MetaError {
        source: MetaError,
    },
    #[snafu(display("EIO error, {source}, req: {path:?}"))]
    StorageError {
        source: StorageError,
        path: Option<String>,
    },
    #[snafu(display("Eio failed too many times"))]
    EIOFailedTooManyTimes,
    #[snafu(display("Should try again"))]
    TryAgain,
    #[snafu(transparent)]
    Shared {
        source: Arc<VfsError>,
    },
    #[snafu(whatever, display("{message}, cause: {source:?}"))]
    GenericError {
        message: String,

        // Having a `source` is optional, but if it is present, it must
        // have this specific attribute and type:
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl VfsError {
    pub fn is_io_error(&self) -> bool {
        matches!(
            self.source,
            VfsErrorEnum::IoError { .. } | VfsErrorEnum::IoDetailError { .. }
        )
    }

    pub fn is_eof(&self) -> bool {
        matches!(
            self.source,
            VfsErrorEnum::IoError {
                ref source
            } if source.kind() == std::io::ErrorKind::UnexpectedEof
        ) || matches!(
            self.source,
            VfsErrorEnum::IoDetailError {
                ref source,
                ..
            } if source.kind() == std::io::ErrorKind::UnexpectedEof
        )
    }
}

impl<E> From<E> for VfsError
where
    E: Into<VfsErrorEnum>,
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

impl FromString for VfsError {
    type Source = Box<dyn std::error::Error + Send + Sync>;
    fn without_source(message: String) -> Self {
        VfsErrorEnum::GenericError {
            message,
            source: None,
        }
        .into()
    }

    fn with_source(source: Self::Source, message: String) -> Self {
        VfsErrorEnum::GenericError {
            message,
            source: Some(source),
        }
        .into()
    }
}

pub type Result<T> = std::result::Result<T, VfsError>;
pub type SharedResult<T> = std::result::Result<T, Arc<VfsError>>;
