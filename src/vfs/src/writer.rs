use std::{
    cmp::{max, min},
    collections::{HashMap, LinkedList},
    mem::take,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, OnceLock,
    },
};

use async_channel::{Receiver, Sender};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use juice_meta::{
    api::{Ino, Meta, Slice, CHUNK_SIZE},
    error::MetaError,
};
use juice_storage::{
    api::{CachedStore, ChunkStore, SliceWriter as SSliceWriter, WSlice},
    error::StorageErrorEnum,
};
use opendal::Buffer;
use parking_lot::Mutex;
use snafu::{whatever, FromString, ResultExt};
use tokio::{sync::Mutex as AsyncMutex, task::JoinSet, time::sleep};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    error::{Result, StorageSnafu, VfsError, VfsErrorEnum},
    frange::Frange,
    reader::DataReaderInvalidator,
};

const MAX_WRITING_SLICES_IN_CHUNK: u32 = 3;
const MAX_WRITING_SLICES_IN_FILE: u32 = 1000;

const FLUSH_DURATION: Duration = Duration::seconds(5);
type CRange = Frange;

struct DataWriterCtx {
    meta: Arc<dyn Meta>,
    store: Arc<CachedStore>,
    ids: SliceIdPool,
    dr_invalidator: DataReaderInvalidator,
    block_size: u64,
    used_read_buffer: AtomicU64,
    max_read_buffer: u64,
    max_retries: u32,
}

struct SliceIdPool {
    meta: Arc<dyn Meta>,
    rx: Receiver<u64>,
    tx: Sender<u64>,
}

impl SliceIdPool {
    pub fn new(meta: Arc<dyn Meta>) -> Self {
        let (tx, rx) = async_channel::bounded(100);
        Self { meta, rx, tx }
    }

    pub fn reuse(&self, id: u64) {
        self.tx.try_send(id).ok();
    }

    pub async fn get(&self) -> Result<u64> {
        match self.rx.try_recv().ok() {
            Some(id) => Ok(id),
            None => Ok(self.meta.new_slice().await?),
        }
    }
}

pub struct DataWriter {
    dw_ctx: Arc<DataWriterCtx>,
    files: Mutex<HashMap<Ino, FileWriter>>,
}

impl DataWriter {
    pub fn new(
        config: Arc<Config>,
        meta: Arc<dyn Meta>,
        store: Arc<CachedStore>,
        invalidator: DataReaderInvalidator,
    ) -> Arc<Self> {
        let dw = Arc::new(Self {
            dw_ctx: Arc::new(DataWriterCtx {
                meta: meta.clone(),
                store,
                ids: SliceIdPool::new(meta.clone()),
                dr_invalidator: invalidator,
                block_size: config.chunk.max_block_size as u64,
                used_read_buffer: AtomicU64::new(0),
                max_read_buffer: config.chunk.buffer_size,
                max_retries: config.meta.retries,
            }),
            files: Mutex::new(HashMap::new()),
        });
        let dw_clone = dw.clone();
        tokio::spawn(async move {
            loop {
                {
                    let mut files = dw_clone.files.lock();
                    files.retain(|_, v| v.refs.load(Ordering::Relaxed) > 0);
                }
                sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        dw
    }

    pub fn open(self: Arc<Self>, ino: Ino, length: u64) -> FileWriterRef {
        let mut files = self.files.lock();
        let writer = files
            .entry(ino)
            .or_insert_with(|| FileWriter::new(self.dw_ctx.clone(), ino, length));
        FileWriterRef::new(writer.clone(), self.clone())
    }

    pub fn truncate(&self, ino: Ino, length: u64) {
        let writer = self.find(ino);
        if let Some(writer) = writer {
            writer.truncate(length);
        }
    }

    pub fn len(&self, ino: Ino) -> u64 {
        let writer = self.find(ino);
        if let Some(writer) = writer {
            writer.len()
        } else {
            0
        }
    }

    pub async fn flush(&self, ino: Ino) -> Result<()> {
        let writer = self.find(ino);
        if let Some(writer) = writer {
            writer.flush().await
        } else {
            Ok(())
        }
    }

    pub async fn flush_all(&self) -> Result<()> {
        let snapshot = {
            let files = self.files.lock();
            files.clone()
        };

        for (_, writer) in snapshot.iter() {
            writer.flush().await.inspect_err(|e| {
                error!("flush all error: {:?}", e);
            })?;
        }
        Ok(())
    }

    fn find(&self, ino: Ino) -> Option<FileWriter> {
        let files = self.files.lock();
        files.get(&ino).map(|f| f.clone())
    }
}

pub struct FileWriterRef {
    writer: FileWriter,
    dw: Arc<DataWriter>,
}

impl FileWriterRef {
    fn new(writer: FileWriter, dw: Arc<DataWriter>) -> Self {
        Self { writer, dw }
    }
}

impl Deref for FileWriterRef {
    type Target = FileWriter;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl Drop for FileWriterRef {
    fn drop(&mut self) {
        if self.refs.load(Ordering::Relaxed) == 0 {
            let mut files = self.dw.files.lock();
            files.remove(&self.writer.ino);
        }
    }
}

struct FileWriterCtx {
    ino: Ino,
    ferr: OnceLock<Arc<VfsError>>,
    slices_cnt: AtomicU32,
    // The length of the file(include mem cache len), it may a little longer than the real file length
    length: AtomicU64,
}

impl FileWriterCtx {
    pub fn init_err(&self, err: Arc<VfsError>) {
        let _ = self.ferr.try_insert(err);
    }

    pub fn get_result(&self) -> Result<()> {
        self.ferr
            .get()
            .map(|e| Err(e.clone().into()))
            .unwrap_or(Ok(()))
    }
}

struct FileWriterCore {
    chunks: HashMap<u32, ChunkWriter>,
}

pub struct FileWriter {
    dw_ctx: Arc<DataWriterCtx>,
    fw_ctx: Arc<FileWriterCtx>,
    ino: Ino,
    writer: Arc<AsyncMutex<FileWriterCore>>,
    refs: Arc<AtomicU32>,
}

impl Clone for FileWriter {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::Relaxed);
        Self {
            dw_ctx: self.dw_ctx.clone(),
            fw_ctx: self.fw_ctx.clone(),
            ino: self.ino.clone(),
            writer: self.writer.clone(),
            refs: self.refs.clone(),
        }
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        self.refs.fetch_sub(1, Ordering::Relaxed);
    }
}

impl FileWriter {
    fn new(dw_ctx: Arc<DataWriterCtx>, ino: Ino, length: u64) -> Self {
        let fw_ctx = Arc::new(FileWriterCtx {
            ino,
            ferr: OnceLock::new(),
            slices_cnt: AtomicU32::new(0),
            length: AtomicU64::new(length),
        });
        let writer = Arc::new(AsyncMutex::new(FileWriterCore {
            chunks: HashMap::new(),
        }));
        Self {
            dw_ctx,
            fw_ctx,
            ino,
            writer,
            refs: Arc::new(AtomicU32::new(0)),
        }
    }

    pub async fn write(&self, off: u64, mut data: Bytes) -> Result<()> {
        loop {
            if self.fw_ctx.slices_cnt.load(Ordering::Relaxed) < MAX_WRITING_SLICES_IN_FILE {
                break;
            }
            sleep(std::time::Duration::from_millis(1)).await;
        }

        if self.dw_ctx.used_read_buffer.load(Ordering::Relaxed) > self.dw_ctx.max_read_buffer {
            // slow down
            sleep(std::time::Duration::from_millis(10)).await;
            while self.dw_ctx.used_read_buffer.load(Ordering::Relaxed)
                > self.dw_ctx.max_read_buffer * 2
            {
                sleep(std::time::Duration::from_millis(100)).await;
            }
        }

        // TODO: use tokio::select to handle cancel
        let mut core = self.writer.lock().await;
        let new_length = off + data.len() as u64;
        let mut idx = (off / CHUNK_SIZE) as u32;
        let mut coff = (off % CHUNK_SIZE) as u32;
        while data.len() > 0 {
            let len = min(data.len() as u64, CHUNK_SIZE - coff as u64);
            let cw = core
                .chunks
                .entry(idx)
                .or_insert_with(|| ChunkWriter::new(idx, self.dw_ctx.clone(), self.fw_ctx.clone()));
            cw.write(coff, data.split_to(len as usize)).await?;
            idx += 1;
            coff = ((coff as u64 + len) % CHUNK_SIZE) as u32;
        }
        if new_length > self.fw_ctx.length.load(Ordering::SeqCst) {
            self.fw_ctx.length.store(new_length, Ordering::SeqCst);
        }
        self.fw_ctx.get_result()
    }

    pub async fn flush(&self) -> Result<()> {
        let mut core = self.writer.lock().await;
        let max_flush_time = max(
            std::time::Duration::from_secs(
                ((self.dw_ctx.max_retries + 2) * (self.dw_ctx.max_retries + 2) / 2) as u64,
            ),
            std::time::Duration::from_mins(5),
        );
        let mut flush_set = JoinSet::new();
        let chunks = take(&mut core.chunks);
        chunks.into_iter().for_each(|(_, cw)| {
            flush_set.spawn(async move {
                cw.finish().await;
            });
        });
        let fw_ctx = self.fw_ctx.clone();
        let check_err = tokio::spawn(async move {
            loop {
                if let Some(e) = fw_ctx.ferr.get() {
                    return e.clone();
                }
                sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        tokio::select! {
            _ = flush_set.join_all() => self.fw_ctx.get_result(),
            _ = sleep(max_flush_time) => {
                warn!(ino = %self.fw_ctx.ino, "flush timeout after waited {:?}", max_flush_time);
                // TODO: interrupt all flush task
                Err(VfsErrorEnum::EIO.into())
            },
            err = check_err => {  // any error happen in chunkWriter interrupt flush
                match err {
                    Ok(e) => Err(e.into()),
                    Err(e) => {
                        error!(ino = %self.fw_ctx.ino, "Check flush error thread error happen: {:?}", e);
                        Err(VfsErrorEnum::EIO.into())
                    },
                }
            },
        }
    }

    pub fn truncate(&self, length: u64) {
        // TODO: truncate write buffer if length < f.length
        self.fw_ctx.length.store(length, Ordering::Relaxed)
    }

    pub fn len(&self) -> u64 {
        self.fw_ctx.length.load(Ordering::Relaxed)
    }
}

struct ChunkWriter {
    idx: u32,
    dw_ctx: Arc<DataWriterCtx>,
    fw_ctx: Arc<FileWriterCtx>,
    slices: Arc<Mutex<LinkedList<SliceWriter>>>,
    commit_task: JoinSet<()>,
    finished: Arc<AtomicBool>,
}

impl ChunkWriter {
    pub fn new(idx: u32, dw_ctx: Arc<DataWriterCtx>, fw_ctx: Arc<FileWriterCtx>) -> Self {
        let slices = Arc::new(Mutex::new(LinkedList::new()));
        let commit_task = JoinSet::new();
        let finished = Arc::new(AtomicBool::new(false));
        let mut cw = Self {
            idx,
            dw_ctx,
            fw_ctx,
            slices,
            commit_task,
            finished,
        };
        info!("spawn commit task for chunk {idx}");
        cw.spawn_commit_finish();
        cw
    }

    pub async fn write(&self, coff: u32, data: Bytes) -> Result<()> {
        let id = self.dw_ctx.ids.get().await?;
        let reuse_id = {
            // info!("chunkWriter({}) op write: start to lock slices.", self.idx);
            let mut slices = self.slices.lock();
            let (reuse, slice_writer) = {
                let block_size = self.dw_ctx.block_size;
                let mut writing_slices = 0;
                // final writable slice
                let writer = slices.iter_mut().rev().find_map(|w| {
                    if !w.freezed() {
                        let flush_off = w.size / block_size as u32 * block_size as u32;
                        if (w.coff + flush_off..=w.coff + w.size).contains(&coff) {
                            return Some(Some(w));
                        }
                        writing_slices += 1;
                        if writing_slices > MAX_WRITING_SLICES_IN_CHUNK {
                            w.flush();
                        }
                    }

                    let slice_range = CRange {
                        off: w.coff as u64,
                        len: w.size as u64,
                    };
                    let write_range = CRange {
                        off: coff as u64,
                        len: data.len() as u64,
                    };
                    slice_range.is_overlap(&write_range).then_some(None)
                });
                match writer {
                    Some(Some(writer)) => (true, writer),
                    _ => {
                        slices.push_back(SliceWriter::new(
                            self.dw_ctx.clone(),
                            self.fw_ctx.clone(),
                            id,
                            self.idx,
                            coff,
                        ));
                        (false, slices.back_mut().unwrap())
                    }
                }
            };
            slice_writer.write(coff - slice_writer.coff, data)?;
            // info!("chunkWriter({}) op write: start to unlock slices.", self.idx);
            reuse.then_some(id)
        };
        if let Some(id) = reuse_id {
            self.dw_ctx.ids.reuse(id);
        }
        self.fw_ctx
            .ferr
            .get()
            .map(|e| Err(e.clone().into()))
            .unwrap_or(Ok(()))
    }

    pub async fn finish(mut self) {
        {
            // info!("chunkWriter({}) op flush: start to lock slices.", self.idx);
            let mut slices = self.slices.lock();
            for slice in slices.iter_mut() {
                slice.flush();
            }
            // info!("chunkWriter({}) op flush: start to unlock slices.", self.idx);
        }
        self.finished.store(true, Ordering::Relaxed);
        match self.commit_task.join_next().await {
            Some(Ok(_)) => (),
            Some(Err(e)) => {
                error!(ino = %self.fw_ctx.ino, chunk = %self.idx, "Commit thread error happend: {:?}", e);
                self.fw_ctx.init_err(Arc::new(VfsErrorEnum::EIO.into()));
            }
            None => {
                warn!(
                    ino = %self.fw_ctx.ino, 
                    chunk = %self.idx,
                    "Already finish, No commit task exist.",
                );
            }
        }
    }

    fn spawn_commit_finish(&mut self) {
        let slice = self.slices.clone();
        let idx = self.idx;
        let dw_ctx = self.dw_ctx.clone();
        let fw_ctx = self.fw_ctx.clone();
        let finished = self.finished.clone();
        self.commit_task.spawn(async move {
            loop {
                let sw = {
                    // info!("chunkWriter({}) op commit: start to lock slices.", idx);
                    let mut slices = slice.lock();
                    let w = slices.pop_front();
                    match w {
                        Some(w) => {
                            let is_timeout = w.start_time
                                < Utc::now() - FLUSH_DURATION.checked_mul(2).expect("overflow");
                            let flush = w.freezed() || is_timeout;
                            if flush {
                                Some(w)
                            } else {
                                slices.push_front(w);
                                None
                            }
                        }
                        None => {
                            if finished.load(Ordering::Relaxed) {
                                break;
                            }
                            None
                        }
                    }
                };
                // info!("chunkWriter({}) op commit: start to unlock slices.", idx);
                match sw {
                    Some(mut sw) => match sw.finish().await {
                        Ok(_) => {
                            info!("chunk slice {idx} commit write");
                            let _ = dw_ctx
                                .meta
                                .write(
                                    fw_ctx.ino,
                                    sw.chunk_idx,
                                    sw.coff,
                                    Slice {
                                        id: sw.slice_id,
                                        size: sw.size,
                                        off: 0,
                                        len: sw.size,
                                    },
                                    sw.last_modify,
                                )
                                .await
                                .inspect_err(|e| {
                                    error!("Commit slice error: {:?}", e);
                                })
                                .map_err(|e| fw_ctx.init_err(Arc::new(e.into())));
                        }
                        Err(e) => {
                            error!("Flush slice error: {:?}", e);
                            fw_ctx.init_err(Arc::new(e));
                        }
                    },
                    None => {
                        sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
    }
}

struct SliceWriter {
    dw_ctx: Arc<DataWriterCtx>,
    fw_ctx: Arc<FileWriterCtx>,
    chunk_idx: u32,
    coff: u32,
    size: u32,
    slice_id: u64,
    writer: Option<WSlice>,
    flush_task: JoinSet<Result<usize>>,
    last_modify: DateTime<Utc>,
    start_time: DateTime<Utc>,
}

impl SliceWriter {
    pub fn new(
        dw_ctx: Arc<DataWriterCtx>,
        fw_ctx: Arc<FileWriterCtx>,
        slice_id: u64,
        chunk_idx: u32,
        coff: u32,
    ) -> Self {
        fw_ctx.slices_cnt.fetch_add(1, Ordering::Relaxed);
        Self {
            dw_ctx: dw_ctx.clone(),
            fw_ctx,
            chunk_idx,
            coff,
            size: 0,
            slice_id,
            writer: Some(dw_ctx.store.new_writer(slice_id)),
            flush_task: JoinSet::new(),
            last_modify: Utc::now(),
            start_time: Utc::now(),
        }
    }

    pub fn write(&mut self, off: u32, data: Bytes) -> Result<()> {
        let writer = self.writer.as_mut().expect("write into freezed writer");
        let write_len = data.len() as u32;
        writer
            .write_all_at(Buffer::from(data), off as usize)
            .inspect_err(|e| {
                error!("write: chunk: {} off: {} {}", self.chunk_idx, off, e);
            })
            .context(StorageSnafu { path: None })?;
        // because all slice is new slice, soff is always 0
        if off + write_len > self.size {
            self.size = off + write_len as u32;
        }
        self.last_modify = Utc::now();
        if self.size as u64 >= self.dw_ctx.block_size {
            writer.spawn_flush_until(self.size as usize).map_err(|e| {
                error!("write: chunk: {} off: {} {}", self.slice_id, off, e);
                VfsErrorEnum::EIO
            })?;
        }
        if self.size as u64 == CHUNK_SIZE {
            self.flush();
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        if self.freezed() {
            return;
        }
        let mut writer = self.writer.take().expect("flush freezed writer");
        self.flush_task.spawn(async move {
            match writer.finish().await {
                Ok(written_size) => Ok(written_size),
                Err(e) => {
                    writer.abort().await;
                    error!("flush error: {:?}", e);
                    Err(VfsErrorEnum::StorageError {
                        source: e,
                        path: None,
                    }
                    .into())
                }
            }
        });
    }

    pub fn freezed(&self) -> bool {
        self.writer.is_none()
    }

    pub async fn finish(&mut self) -> Result<()> {
        if !self.freezed() {
            self.flush();
        }
        self.dw_ctx.meta.new_slice().await?;
        match self.flush_task.join_next().await {
            Some(Ok(Ok(written_size))) => {
                if written_size != self.size as usize {
                    whatever!("flush size non eq with write size");
                } else {
                    Ok(())
                }
            }
            Some(Ok(Err(e))) => Err(e),
            Some(Err(e)) => whatever!("Flush thread error happen {e:?}"),
            None => whatever!("No Flush Task exist!"),
        }
    }
}

impl Drop for SliceWriter {
    fn drop(&mut self) {
        self.fw_ctx.slices_cnt.fetch_sub(1, Ordering::Relaxed);
    }
}
