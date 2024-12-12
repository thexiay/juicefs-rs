use snafu::{GenerateImplicitData, Snafu};
use tracing::Span;

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

}

#[derive(Debug)]
pub struct SpanGuard(tracing::Span);

impl GenerateImplicitData for SpanGuard {
    fn generate() -> Self {
        SpanGuard(Span::current())
    }
}


pub type Result<T> = std::result::Result<T, StorageError>;
