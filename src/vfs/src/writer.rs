use std::{
    cmp::{max, min},
    collections::{HashMap, LinkedList},
    mem::take,
    ops::Deref,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, OnceLock,
    },
};

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use juice_meta::error::Result as MetaResult;
use juice_meta::{
    api::{Ino, Meta, Slice, CHUNK_SIZE},
    error::MetaError,
};
use juice_storage::api::{CachedStore, ChunkStore, SliceWriter as SSliceWriter, WSlice};
use opendal::Buffer;
use parking_lot::Mutex;
use snafu::FromString;
use tokio::{
    sync::{
        mpsc::{channel, Receiver},
        Notify,
    },
    task::JoinSet,
    time::sleep,
};
use tracing::{error, warn};

use crate::{
    config::Config,
    error::{Result, VfsError, VfsErrorEnum},
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
    id_pool: SliceIDPool,
    dr_invalidator: DataReaderInvalidator,
    block_size: u64,
    used_read_buffer: AtomicU64,
    max_read_buffer: u64,
    max_retries: u32,
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
                id_pool: SliceIDPool::new(meta.clone()),
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
    len: u64,
    chunks: HashMap<u32, ChunkWriter>,
}

pub struct FileWriter {
    dw_ctx: Arc<DataWriterCtx>,
    fw_ctx: Arc<FileWriterCtx>,
    ino: Ino,
    writer: Arc<Mutex<FileWriterCore>>,
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
        });
        let writer = Arc::new(Mutex::new(FileWriterCore {
            len: length,
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
        let mut core = self.writer.lock();
        let new_length = off + data.len() as u64;
        let mut idx = (off / CHUNK_SIZE) as u32;
        let mut coff = (off % CHUNK_SIZE) as u32;
        while data.len() > 0 {
            let len = min(data.len() as u64, CHUNK_SIZE - coff as u64);
            let cw = core
                .chunks
                .entry(idx)
                .or_insert_with(|| ChunkWriter::new(idx, self.dw_ctx.clone(), self.fw_ctx.clone()));
            cw.write(coff, data.split_off(len as usize))?;
            idx += 1;
            coff = ((coff as u64 + len) % CHUNK_SIZE) as u32;
        }
        if new_length > core.len {
            core.len = new_length;
        }
        self.fw_ctx.get_result()
    }

    pub async fn flush(&self) -> Result<()> {
        let mut core = self.writer.lock();
        let max_flush_time = max(
            std::time::Duration::from_secs(
                ((self.dw_ctx.max_retries + 2) * (self.dw_ctx.max_retries + 2) / 2) as u64,
            ),
            std::time::Duration::from_mins(5),
        );
        let mut flush_set = JoinSet::new();
        let chunks = take(&mut core.chunks);
        chunks.into_iter().for_each(|(_, mut cw)| {
            flush_set.spawn(async move {
                cw.flush().await;
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
                warn!("flush timeout after waited {:?}", max_flush_time);
                // TODO: interrupt all flush task
                Err(VfsErrorEnum::EIO.into())
            },
            err = check_err => {
                match err {
                    Ok(e) => Err(e.into()),
                    Err(e) => {
                        error!("Check flush error thread error happen: {:?}", e);
                        Err(VfsErrorEnum::EIO.into())
                    },
                }
            },
        }
    }

    pub fn truncate(&self, length: u64) {
        let mut writer = self.writer.lock();
        // TODO: truncate write buffer if length < f.length
        writer.len = length;
    }

    pub fn len(&self) -> u64 {
        let core = self.writer.lock();
        core.len
    }
}

// TODO: Make it more reliable
struct SliceIDPool {
    meta: Arc<dyn Meta>,
    id_rx: Mutex<Receiver<MetaResult<u64>>>,
}

impl SliceIDPool {
    pub fn new(meta: Arc<dyn Meta>) -> SliceIDPool {
        let (tx, rx) = channel(1000);
        let meta_clone = meta.clone();
        tokio::spawn(async move {
            async fn get_slice_id(meta: &Arc<dyn Meta>) -> u64 {
                loop {
                    match meta.new_slice().await {
                        Ok(id) => return id,
                        Err(e) => warn!("get slice id error: {:?}", e),
                    }
                }
            }

            loop {
                let id = tokio::select! {
                    _ = sleep(std::time::Duration::from_millis(100)) =>
                        Err(MetaError::without_source("timeout to get slice id".to_string())),
                    id = get_slice_id(&meta_clone) => Ok(id),
                };
                if let Err(e) = tx.send(id).await {
                    error!("send id channel closed: {:?}", e);
                    break;
                }
            }
        });
        Self {
            meta,
            id_rx: Mutex::new(rx),
        }
    }

    pub fn get_id(&self) -> Result<u64> {
        let mut rx = self.id_rx.lock();
        match rx.blocking_recv() {
            Some(Ok(id)) => Ok(id),
            Some(Err(e)) => Err(e.into()),
            None => panic!("slice id channel closed"),
        }
    }
}

struct ChunkWriter {
    idx: u32,
    dw_ctx: Arc<DataWriterCtx>,
    fw_ctx: Arc<FileWriterCtx>,
    slices: Arc<Mutex<LinkedList<SliceWriter>>>,
    commit_task: JoinSet<()>,
}

impl ChunkWriter {
    pub fn new(idx: u32, dw_ctx: Arc<DataWriterCtx>, fw_ctx: Arc<FileWriterCtx>) -> Self {
        let slices = Arc::new(Mutex::new(LinkedList::new()));
        let commit_task = JoinSet::new();
        let mut cw = Self {
            idx,
            dw_ctx,
            fw_ctx,
            slices,
            commit_task,
        };
        cw.spawn_commit_finish();
        cw
    }

    pub fn write(&self, coff: u32, data: Bytes) -> Result<()> {
        let mut slices = self.slices.lock();
        let slice_writer = {
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
                Some(Some(writer)) => writer,
                _ => {
                    let slice_id = self.dw_ctx.id_pool.get_id()?;
                    slices.push_back(SliceWriter::new(
                        self.dw_ctx.clone(),
                        self.fw_ctx.clone(),
                        self.idx,
                        slice_id,
                        coff,
                    ));
                    slices.back_mut().unwrap()
                }
            }
        };
        slice_writer.write(coff - slice_writer.coff, data)?;
        self.fw_ctx
            .ferr
            .get()
            .map(|e| Err(e.clone().into()))
            .unwrap_or(Ok(()))
    }

    pub async fn flush(&mut self) {
        {
            let mut slices = self.slices.lock();
            for slice in slices.iter_mut() {
                slice.flush();
            }
        }
        match self.commit_task.join_next().await {
            Some(Ok(_)) => (),
            Some(Err(e)) => {
                error!("Commit thread error happend: {:?}", e);
                self.fw_ctx.init_err(Arc::new(VfsErrorEnum::EIO.into()));
            }
            None => (),
        }
    }

    fn spawn_commit_finish(&mut self) {
        let slice = self.slices.clone();
        self.commit_task.spawn(async move {
            loop {
                let flusher = {
                    let mut slices = slice.lock();
                    slices
                        .pop_front()
                        .map(|w| {
                            let is_timeout = w.start_time
                                < Utc::now() - FLUSH_DURATION.checked_mul(2).expect("overflow");
                            let flush = w.freezed() || is_timeout;
                            if flush {
                                Some(w)
                            } else {
                                slices.push_front(w);
                                None
                            }
                        })
                        .flatten()
                };

                match flusher {
                    Some(flusher) => flusher.finish().await,
                    None => sleep(std::time::Duration::from_millis(100)).await,
                }
            }
        });
    }
}

struct SliceWriter {
    dw_ctx: Arc<DataWriterCtx>,
    fw_ctx: Arc<FileWriterCtx>,
    chunk_idx: u32,
    slice_id: u64,
    coff: u32,
    size: u32,
    writer: Option<WSlice>,
    flush_task: JoinSet<()>,
    last_modify: DateTime<Utc>,
    start_time: DateTime<Utc>,
}

impl SliceWriter {
    pub fn new(
        dw_ctx: Arc<DataWriterCtx>,
        fw_ctx: Arc<FileWriterCtx>,
        chunk_idx: u32,
        slice_id: u64,
        coff: u32,
    ) -> Self {
        fw_ctx.slices_cnt.fetch_add(1, Ordering::Relaxed);
        Self {
            dw_ctx: dw_ctx.clone(),
            fw_ctx,
            chunk_idx,
            slice_id,
            coff,
            size: 0,
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
            .map_err(|e| {
                error!("write: chunk: {} off: {} {}", self.slice_id, off, e);
                VfsErrorEnum::EIO
            })?;
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
        let fw_ctx = self.fw_ctx.clone();
        let dw_ctx = self.dw_ctx.clone();
        let slice_id = self.slice_id;
        let chunk_idx = self.chunk_idx;
        let coff = self.coff;
        let size = self.size;
        let last_modify = self.last_modify;

        self.flush_task.spawn(async move {
            match writer.finish().await {
                Ok(written_size) => {
                    if written_size != size as usize {
                        error!("flush size non eq with write size");
                        fw_ctx.init_err(Arc::new(VfsErrorEnum::EIO.into()));
                    } else {
                        let slice = Slice {
                            id: slice_id,
                            size: size,
                            off: 0,
                            len: size,
                        };
                        let _ = dw_ctx
                            .meta
                            .write(fw_ctx.ino, chunk_idx, coff, slice, last_modify)
                            .await
                            .inspect_err(|e| {
                                error!("Write slice into meta node error: {:?}", e);
                            })
                            .map_err(|e| fw_ctx.init_err(Arc::new(e.into())));
                    }
                }
                Err(e) => {
                    writer.abort().await;
                    error!("flush error: {:?}", e);
                    let _ = fw_ctx.init_err(Arc::new(VfsErrorEnum::EIO.into()));
                }
            };
        });
    }

    pub fn freezed(&self) -> bool {
        self.writer.is_none()
    }

    pub async fn finish(mut self) {
        if !self.freezed() {
            self.flush();
        }
        match self.flush_task.join_next().await {
            Some(Ok(_)) => (),
            Some(Err(e)) => {
                error!("Flush thread error happend: {:?}", e);
                self.fw_ctx.init_err(Arc::new(VfsErrorEnum::EIO.into()));
            }
            None => (),
        }
    }
}

impl Drop for SliceWriter {
    fn drop(&mut self) {
        self.fw_ctx.slices_cnt.fetch_sub(1, Ordering::Relaxed);
    }
}
