use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use juice_meta::api::{Entry, Ino, OFlag};
use tokio_util::sync::CancellationToken;

use crate::{error::Result, reader::{DataReader, FileReader, FileReaderRef}, writer::{DataWriter, FileWriter, FileWriterRef}};

pub type Fh = u64;
/// Proxy of [`FileReader`] and [`FileWriter`]
/// Can handle write and read progres bar.
pub struct FileHandle {
    inode: Ino,
    fh: u64,
    
    // for dir
    pub(crate) children: Vec<Entry>,
    pub(crate) indexs: HashMap<String, usize>,
    read_at: DateTime<Utc>,
    pub(crate) read_off: usize,
    
    // for file
    flags: OFlag,
    is_recovered: bool,
    locks: u8,
    flock_owner: u64,
    ofd_owner: u64,
    pub(crate) reader: Option<FileReaderRef>,
    pub(crate) writer: Option<FileWriterRef>,

    // internal files 
    off: u64,
    data: Bytes,
    pending: Bytes,
}

impl FileHandle {
    pub fn new(ino: Ino, fh: Fh, flags: OFlag, is_recovered: bool) -> Self {
        todo!()
    }

    pub fn fh(&self) -> Fh {
        self.fh
    }

    pub async fn close(&mut self) {
        if let Some(writer) = &self.writer {
            let _ = writer.flush().await;
        }
        self.reader = None;
        self.reader = None;
    }
}
