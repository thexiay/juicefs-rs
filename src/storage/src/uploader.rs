use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use either::Either;
use opendal::{Buffer, Operator};

use crate::buffer::FileBuffer;
use crate::error::Result;
use crate::compress::Compressor;

#[async_trait]
pub trait Uploader: Send + Sync + 'static {
    async fn upload(&self, key: &str, block: Buffer) -> Result<Either<Buffer, FileBuffer>>;
}

#[derive(Clone)]
pub struct NormalUploader {
    storage: Arc<Operator>,
    compressor: Option<Arc<Box<dyn Compressor>>>,
}

impl NormalUploader {
    pub fn new (
        storage: Arc<Operator>,
        compressor: Option<Arc<Box<dyn Compressor>>>,
    ) -> Self {
        NormalUploader {
            storage,
            compressor,
        }
    }
}

#[async_trait]
impl Uploader for NormalUploader {
    async fn upload(&self, key: &str, buffer: Buffer) -> Result<Either<Buffer, FileBuffer>> {
        let mut writer = self.storage.writer(key).await?;
        writer.write(buffer.clone()).await?;
        writer.close().await?;
        Ok(Either::Left(buffer))
    }
}