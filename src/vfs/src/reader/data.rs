use std::{
    cmp::{max, min}, collections::HashMap, ops::Deref, sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }
};

use juice_meta::api::{Ino, Meta};
use juice_storage::api::CachedStore;
use parking_lot::Mutex;
use tokio::time::sleep;

use crate::{
    config::Config,
    frange::Frange,
};

use super::{file::{FileReader, READ_SESSION}, FileReaderRef};

pub struct DataReaderCtx {
    pub(crate) meta: Arc<dyn Meta>,
    pub(crate) store: Arc<CachedStore>,
    pub(crate) max_block_size: u64,
    /// max readahead bytes in once readahead request
    pub(crate) max_once_readahead: u64,
    /// total readahead bytes in all files
    pub(crate) max_readahead: u64,
    /// used read buffer in all file readers
    pub(crate) used_read_buffer: AtomicU64,
    /// Max read requests in one [`FileReader`]
    /// After exceeding this value, the fileReader that is completed in the cache will be cleaned.
    pub(crate) max_request: usize,
    /// Max read retries count in one [`FileReader`], requests exceeding this value will fail
    pub(crate) max_retries: u32,
}

pub struct DataReaderInvalidator(Arc<DataReader>);

impl DataReaderInvalidator {
    pub fn invalid(&self, inode: Ino, frange: Frange) {
        self.0.invalidate(inode, frange);
    }
}

pub struct DataReader {
    ctx: Arc<DataReaderCtx>,
    pub(crate) file_readers: Mutex<HashMap<Ino, Vec<FileReader>>>,
}

impl DataReader {
    pub fn new(meta: Arc<dyn Meta>, store: Arc<CachedStore>, conf: Arc<Config>) -> Arc<DataReader> {
        let total_readahead = max(conf.chunk.buffer_size * 10 / 8, 256 << 20); // default 256MB
        let max_readahead = min(
            max(8 * conf.chunk.max_block_size as u64, conf.chunk.read_ahead),
            total_readahead,
        );
        let max_retries = conf.meta.retries;
        let max_request = max_readahead as usize / conf.chunk.max_block_size * READ_SESSION + 1;
        let dr = Arc::new(DataReader {
            ctx: Arc::new(DataReaderCtx {
                meta,
                store,
                max_block_size: conf.chunk.max_block_size as u64,
                max_once_readahead: max_readahead,
                max_readahead: total_readahead,
                used_read_buffer: AtomicU64::new(0),
                max_request,
                max_retries,
            }),
            file_readers: Mutex::new(HashMap::new()),
        });
        let dr_clone = dr.clone();
        // TODO: add stop for this thread
        let _ = tokio::spawn(async move {
            loop {
                {
                    let mut file_readers = dr_clone.file_readers.lock();
                    for (_, frs) in file_readers.iter_mut() {
                        for fr in frs.iter() {
                            fr.lock().release_idle_buffer();
                        }
                    }
                }
                sleep(std::time::Duration::from_secs(10)).await;
            }
        });
        dr
    }

    pub fn open(self: Arc<Self>, inode: Ino, length: u64) -> FileReaderRef {
        let mut file_readers = self.file_readers.lock();
        let frs = file_readers.entry(inode).or_insert_with(Vec::new);
        let fr = FileReader::new(inode, length, self.ctx.clone());
        frs.insert(0, fr.clone());
        FileReaderRef::new(fr, self.clone())
    }

    /// Truncate file length
    pub fn truncate(&self, inode: Ino, length: u64) {
        self.visit(inode, |fr| {
            let mut f = fr.lock();
            if length < f.length {
                f.visit(|cr| {
                    if cr.frange.end() > length {
                        cr.clone().invalid();
                    }
                });
            }
            f.length = length;
            f.clean();
        });
    }

    /// Invalidate part of file range
    pub fn invalidate(&self, inode: Ino, frange: Frange) {
        self.visit(inode, |fr| {
            let mut f = fr.lock();
            f.visit(|cr| {
                if frange.is_overlap(&cr.frange) {
                    cr.clone().invalid();
                }
            });
            f.clean();
        });
    }

    pub fn used_read_buffer(&self) -> u64 {
        self.ctx.used_read_buffer.load(Ordering::SeqCst)
    }

    pub fn invalid_notifier(self: Arc<Self>) -> DataReaderInvalidator {
        DataReaderInvalidator(self)
    }

    fn visit(&self, inode: Ino, visitor: impl Fn(&FileReader)) {
        let file_readers = self.file_readers.lock();
        if let Some(frs) = file_readers.get(&inode) {
            for fr in frs.iter() {
                visitor(fr);
            }
        }
    }
}
