use std::sync::mpsc::Receiver;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::Result;

pub struct Description {
    is_support_multipart_upload: bool,
    is_support_upload_part_copy: bool,
    min_part_size: usize,
    max_part_size: u64,
    max_part_count: usize,
}

pub struct PutAttr {
    pub request_id: String,
    pub stroage_class: String,
}

pub struct GetAttr {
    pub request_id: String,
    pub stroage_class: String,
}

pub struct DeleteAttr {
    pub request_id: String,
}

pub struct MultipartUpload {
    pub min_part_size: isize,
    pub max_count: isize,
    pub upload_id: String,
}

pub struct Part {
    pub num: u64,
    pub size: u64,
    pub etag: String,
}

pub struct PendingPart {
    pub key: String,
    pub upload_id: String,
    pub created: i64,
}

pub trait Object {
    fn key(&self) -> String;
    fn size(&self) -> i64;
    fn mtime(&self) -> i64;
    fn is_dir(&self) -> bool;
    fn is_symlink(&self) -> bool;
    fn storage_class(&self) -> String;
}

/// ObjectStorage is the interface for object storage.
/// all of these API should be idempotent.
#[async_trait]
pub trait ObjectStore: ToString + Send + Sync + 'static {
    /// Limits description of the object storage.
    fn desc() -> Description;

    /// Create the bucket if not existed.
    async fn create() -> Result<()>;

    // Get the data for the given object specified by key.
    async fn get(&self, key: &str, off: i64, limit: i64) -> Result<(GetAttr,)>;

    /// Put data from a reader to an object specified by key.
    async fn put(&self, key: &str, reader: impl AsyncReadExt) -> Result<()>;

    /// Copy an object from src to dst.
    async fn copy(&self, dst: &str, src: &str) -> Result<()>;

    /// Delete a object
    async fn delete(&self, key: &str) -> Result<DeleteAttr>;

    /// Head returns some information about the object or an error if not found.
    async fn head(&self, key: &str) -> Result<Box<dyn Object>>;

    /// List returns a list of objects.
    async fn list(
        &self,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        limit: i64,
        follow_link: bool,
    ) -> Result<Vec<Box<dyn Object>>>;

    /// ListAll returns all the objects as an channel.
    async fn list_all(
        &self,
        prefix: &str,
        marker: &str,
        follow_link: bool,
    ) -> Result<Receiver<Box<dyn Object>>>;

    /// CreateMultipartUpload starts to upload a large object part by part.
    async fn create_multipart_upload(&self, key: &str) -> Result<MultipartUpload>;

    /// UploadPart upload a part of an object.
    async fn upload_part(&self, key: &str, upload_id: &str, num: u64, body: &[u8]) -> Result<Part>;

    /// UploadPartCopy Uploads a part by copying data from an existing object as data source.
    async fn upload_part_copy(
        &self,
        key: &str,
        upload_id: &str,
        num: u64,
        src_key: &str,
        off: u64,
        size: u64,
    ) -> Result<Part>;

    /// AbortUpload abort a multipart upload.
    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<()>;

    /// CompleteUpload finish an multipart upload.
    async fn complete_upload(&self, key: &str, upload_id: &str, parts: Vec<Part>) -> Result<()>;

    /// ListUploads lists existing multipart uploads.
    async fn list_uploads(&self, marker: &str) -> Result<(Vec<PendingPart>, String)>;
}
