use std::{future::Future, sync::Arc};

use either::Either;
use opendal::{Buffer, Operator};

use crate::buffer::FileBuffer;
use crate::error::Result;
use crate::compress::Compressor;

pub trait Uploader: Send + Sync + 'static {
    fn upload(&self, key: &str, block: Buffer) -> impl Future<Output = Result<Either<Buffer, FileBuffer>>> + Send;
}

#[derive(Clone)]
pub struct NormalUploader {
    storage: Arc<Operator>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
}

impl NormalUploader {
    fn new (
        storage: Arc<Operator>,
        compressor: Option<Arc<Box<dyn Compressor>>>,
    ) -> Self {
        NormalUploader {
            storage,
            compressor,
        }
    }
}

impl Uploader for NormalUploader {
    async fn upload(&self, key: &str, buffer: Buffer) -> Result<Either<Buffer, FileBuffer>> {
        let mut writer = self.storage.writer(key).await?;
        writer.write(buffer.clone()).await?;
        Ok(Either::Left(buffer))
    }
}