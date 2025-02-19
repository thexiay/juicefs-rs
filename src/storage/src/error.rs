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
pub struct StorageError {
    source: StorageErrorEnum,
    #[snafu(implicit)]
    loc: snafu::Location,
    #[snafu(implicit)]
    span: SpanGuard,
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum StorageErrorEnum {
    #[snafu(display("IO error: {}", source), context(false))]
    IoError { source: std::io::Error },
    #[snafu(display("Opendal IO error: {}", source), context(false))]
    ObjectIoError { source: opendal::Error },
    #[snafu(display("Disk is unstable, limit operation during this time"))]
    DiskUnstableError,
    #[snafu(display("Send error"))]
    SenderError,
    #[snafu(whatever, display("{message}, cause: {source:?}"))]
    GenericError {
        message: String,

        // Having a `source` is optional, but if it is present, it must
        // have this specific attribute and type:
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl StorageError {
    pub fn is_stage_concurrency(&self) -> bool {
        false
    }

    pub fn is_io_error(&self) -> bool {
        matches!(self.source, StorageErrorEnum::IoError { .. })
    }

    pub fn is_eof(&self) -> bool {
        matches!(
            self.source, 
            StorageErrorEnum::IoError {
                ref source 
            } if source.kind() == std::io::ErrorKind::UnexpectedEof
        )
    }

    pub fn is_disk_unstable_error(&self) -> bool {
        matches!(self.source, StorageErrorEnum::DiskUnstableError)
    }
}

impl<E> From<E> for StorageError
where
    E: Into<StorageErrorEnum>,
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

impl FromString for StorageError {
    type Source = Box<dyn std::error::Error + Send + Sync>;
    fn without_source(message: String) -> Self {
        StorageErrorEnum::GenericError {
            message,
            source: None,
        }
        .into()
    }

    fn with_source(source: Self::Source, message: String) -> Self {
        StorageErrorEnum::GenericError {
            message,
            source: Some(source),
        }
        .into()
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
