use std::sync::{
    atomic::{AtomicI64, AtomicU32, Ordering},
    Arc,
};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Duration, Local, Utc};
use either::Either;
use juice_meta::api::{Meta, Slice, CHUNK_SIZE};
use juice_storage::api::{ChunkStore, SliceReader};
use opendal::Buffer;
use parking_lot::RwLock;
use snafu::FromString;
use tokio::{
    sync::{Notify, Semaphore},
    task::{AbortHandle, JoinSet},
    time::sleep,
};
use tracing::{info, warn};

use crate::{
    error::{EIOFailedTooManyTimesSnafu, Result, VfsError},
    frange::Frange,
};

use super::{data::DataReaderCtx, file::FileReaderCtx};

pub trait ReusableFRange: Clone {
    fn frange(&self) -> &Frange;

    fn valid(&self) -> bool;

    fn reuse(&self, frange: &Frange) -> Option<Self>;
}

// Shared read request based on an existing slice read
pub struct ChunkReadReq {
    // off in `ChunkReader` frange
    pub(crate) off: u64,
    pub(crate) len: u64,
    pub(crate) chunk_reader: Arc<ChunkReader>,
}

impl ReusableFRange for ChunkReadReq {
    fn frange(&self) -> &Frange {
        &self.chunk_reader.frange
    }

    fn valid(&self) -> bool {
        !self.chunk_reader.is_invalid()
    }

    fn reuse(&self, frange: &Frange) -> Option<Self> {
        self.chunk_reader
            .reuse(frange)
            .map(|(off, len)| ChunkReadReq {
                off,
                len,
                chunk_reader: self.chunk_reader.clone(),
            })
    }
}

impl Clone for ChunkReadReq {
    fn clone(&self) -> Self {
        self.chunk_reader.refs.fetch_add(1, Ordering::Relaxed);
        Self {
            off: self.off.clone(),
            len: self.len.clone(),
            chunk_reader: self.chunk_reader.clone(),
        }
    }
}

impl Drop for ChunkReadReq {
    fn drop(&mut self) {
        self.chunk_reader.refs.fetch_sub(1, Ordering::SeqCst);
    }
}

// A ChunkReader represent a read request for a slice of chunk of data
pub struct ChunkReader {
    dr_ctx: Arc<DataReaderCtx>,
    fr_ctx: Arc<FileReaderCtx>,
    // This frange is only in ONE chunk(event one block)
    pub(crate) frange: Frange,
    // reused refs, `ChunkReader` could be reused in other `ChunkReadReq`
    refs: AtomicU32,
    state: RwLock<SRState>,
    final_state_notify: Arc<Notify>,
    last_access_time: AtomicI64,
}

enum SRState {
    Init,
    Running(ChunkReadingTask),
    Ready(Bytes),
    Failed(Arc<VfsError>),
    Invalid,
}

impl SRState {
    fn new_task(slice_reader: Arc<ChunkReader>, delay: std::time::Duration) -> Self {
        let now = Local::now();
        let slice_reader_cloned = slice_reader.clone();
        let task = tokio::spawn(async move {
            sleep(delay).await;
            slice_reader_cloned.run(now).await;
        });
        SRState::Running(ChunkReadingTask {
            mtime: now,
            task: task.abort_handle(),
            nofity: slice_reader.final_state_notify.clone(),
        })
    }
}

struct ChunkReadingTask {
    mtime: DateTime<Local>,
    task: AbortHandle,
    nofity: Arc<Notify>,
}

impl ChunkReadingTask {
    fn renew_task(&mut self, slice_reader: Arc<ChunkReader>, delay: std::time::Duration) {
        let mtime = Local::now();
        self.mtime = mtime.clone();
        self.task.abort();
        self.task = tokio::spawn(async move {
            sleep(delay).await;
            slice_reader.clone().run(mtime).await;
        })
        .abort_handle();
    }
}

impl Drop for ChunkReadingTask {
    fn drop(&mut self) {
        self.task.abort();
        self.nofity.notify_waiters();
    }
}

impl ChunkReader {
    pub fn new(
        dr_ctx: Arc<DataReaderCtx>,
        fr_ctx: Arc<FileReaderCtx>,
        frange: Frange,
    ) -> Arc<Self> {
        let slice_reader = Arc::new(ChunkReader {
            dr_ctx,
            fr_ctx,
            frange,
            refs: AtomicU32::new(0),
            state: RwLock::new(SRState::Init),
            final_state_notify: Arc::new(Notify::new()),
            last_access_time: AtomicI64::new(Utc::now().timestamp()),
        });
        slice_reader.clone().refresh(std::time::Duration::ZERO);
        slice_reader
    }

    async fn run(self: Arc<Self>, task_time: DateTime<Local>) {
        let chunk_idx = (self.frange.off / CHUNK_SIZE) as u32;
        match self.dr_ctx.meta.read(self.fr_ctx.ino, chunk_idx).await {
            Ok(slices) => {
                let mut foff = self.frange.off / CHUNK_SIZE * CHUNK_SIZE;
                let mut slice_reqs = Vec::new();
                let mut flen = self.frange.len;
                for slice in slices {
                    if flen == 0 {
                        break;
                    }
                    let slice_frange = Frange {
                        off: foff,
                        len: slice.len as u64,
                    };
                    if let Some(overlap) = self.frange.overlap(&slice_frange) {
                        assert!(flen >= overlap.len, "meta return invalid slices");
                        flen -= overlap.len;
                        slice_reqs.push(Slice {
                            id: slice.id,
                            size: slice.size,
                            off: slice.off + (overlap.off - foff) as u32,
                            len: overlap.len as u32,
                        });
                    }
                    foff += slice.len as u64;
                }
                let buf = self.clone().read(slice_reqs).await;
                if buf.len() == self.frange.len as usize {
                    self.success(buf, &task_time);
                } else {
                    let _ = self
                        .dr_ctx
                        .meta
                        .invalidate_chunk_cache(self.fr_ctx.ino, chunk_idx);
                    let retry_cnt = self.fr_ctx.retries.fetch_add(1, Ordering::SeqCst);
                    if retry_cnt > self.dr_ctx.max_retries {
                        self.fail(
                            Arc::new(EIOFailedTooManyTimesSnafu.build().into()),
                            &task_time,
                        );
                    } else {
                        self.clone().refresh(Self::retry_time(retry_cnt + 1));
                    }
                }
            }
            Err(e) if e.is_no_entry_found2(&self.fr_ctx.ino) => {
                self.fail(Arc::new(e.into()), &task_time);
            }
            Err(e) => {
                info!("meta error happen: {:?}", e);
                let retry_cnt = self.fr_ctx.retries.fetch_add(1, Ordering::SeqCst);
                self.clone().refresh(Self::retry_time(retry_cnt + 1));
            }
        }
    }

    fn refresh(self: Arc<Self>, delay: std::time::Duration) {
        let mut state = self.state.write();
        let next_state = match &mut *state {
            SRState::Init => Some(SRState::new_task(self.clone(), delay)),
            SRState::Running(task) => {
                task.renew_task(self.clone(), delay);
                None
            }
            _ => None,
        };
        if let Some(next_state) = next_state {
            *state = next_state;
        }
    }

    pub fn invalid(self: Arc<Self>) {
        let mut state = self.state.write();
        let next_state = match &mut *state {
            SRState::Init => Some(SRState::new_task(self.clone(), std::time::Duration::ZERO)),
            SRState::Running(task) => {
                task.renew_task(self.clone(), std::time::Duration::ZERO);
                None
            }
            SRState::Ready(_) => {
                if self.refs.load(Ordering::SeqCst) > 0 {
                    Some(SRState::new_task(self.clone(), std::time::Duration::ZERO))
                } else {
                    Some(SRState::Invalid)
                }
            }
            _ => None,
        };
        if let Some(next_state) = next_state {
            *state = next_state;
        }
    }

    pub fn close(&self) {
        let mut state = self.state.write();
        if self.refs.load(Ordering::SeqCst) == 0 {
            *state = SRState::Invalid;
        }
    }

    fn fail(&self, err: Arc<VfsError>, task_time: &DateTime<Local>) {
        let mut state = self.state.write();
        let next_state = match &mut *state {
            SRState::Running(task) if &task.mtime == task_time => Some(SRState::Failed(err)),
            _ => None,
        };
        if let Some(next_state) = next_state {
            *state = next_state;
        }
    }

    fn success(&self, buf: Bytes, task_time: &DateTime<Local>) {
        let mut state = self.state.write();
        let next_state = match &mut *state {
            SRState::Running(task) if &task.mtime == task_time => Some(SRState::Ready(buf)),
            _ => None,
        };
        if let Some(next_state) = next_state {
            *state = next_state;
        }
    }

    async fn read(self: Arc<Self>, slice_reqs: Vec<Slice>) -> Bytes {
        let limit = Arc::new(Semaphore::new(16));
        let mut task_set = JoinSet::new();
        slice_reqs.into_iter().enumerate().for_each(|(i, req)| {
            let limit_cloned = limit.clone();
            let sr = self.clone();
            task_set.spawn(async move {
                match req.is_hole() {
                    true => Ok((i, Either::Left(req.len))),
                    false => {
                        let _premit = limit_cloned
                            .acquire()
                            .await
                            .expect("acquire semaphore failed");
                        let inode = sr.fr_ctx.ino;
                        let id = req.id;
                        let off = req.off;
                        let len = req.len;
                        sr.read_slice(req).await
                            .inspect_err(|e| {
                                warn!(ino = %inode, slice_id = %id, off = %off, len = %len, "read slice failed: {:?}", e);
                            }).map(|buf| (i, Either::Right(buf)))
                    }
                }
            });
        });
        let mut all_buf = Vec::new();
        while let Some(res) = task_set.join_next().await {
            match res {
                Ok(Ok(res)) => all_buf.push(res),
                Ok(Err(e)) => {
                    warn!(ino = %self.fr_ctx.ino, "read slice coroutine failed: {:?}", e);
                    return Bytes::new();
                }
                Err(e) => {
                    warn!(ino = %self.fr_ctx.ino, "read slice coroutine failed: {:?}", e);
                    return Bytes::new();
                }
            }
        }
        all_buf.sort_by_key(|(i, ..)| *i);
        let mut data = BytesMut::new();
        for (_, either) in all_buf {
            match either {
                Either::Left(len) => {
                    data.put_bytes(0, len as usize);
                }
                Either::Right(buf) => {
                    for slice_buf in buf.into_iter() {
                        data.put(slice_buf);
                    }
                }
            }
        }
        data.freeze()
    }

    async fn read_slice(&self, slice: Slice) -> juice_storage::error::Result<Buffer> {
        if slice.size == 0 || slice.len == 0 {
            return Ok(Buffer::new());
        }
        let reader = self.dr_ctx.store.new_reader(slice.id, slice.size as usize);
        let buf = reader
            .read_all_at(slice.off as usize, slice.len as usize)
            .await?;
        Ok(buf)
    }

    pub async fn get(&self) -> Result<Bytes> {
        loop {
            tokio::select! {
                _ = self.final_state_notify.notified() => {
                    let state = self.state.read();
                    match &*state {
                        SRState::Init | SRState::Running(_) => continue,
                        SRState::Ready(buf) => return Ok(buf.clone()),
                        SRState::Failed(err) => return Err(err.clone().into()),
                        SRState::Invalid => return Err(VfsError::without_source("invalid slice".to_string())),
                    };
                },
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            }
        }
    }

    pub fn is_invalid(&self) -> bool {
        let state = self.state.read();
        matches!(&*state, SRState::Invalid)
    }

    pub fn is_ready(&self) -> bool {
        let state = self.state.read();
        matches!(&*state, SRState::Ready(_))
    }

    pub fn is_out_dated(&self, time: Duration) -> bool {
        let now = Utc::now();
        let ts = DateTime::from_timestamp(self.last_access_time.load(Ordering::SeqCst), 0)
            .expect("invalid timestamp");
        ts + time < now
    }

    pub fn reuse(&self, frange: &Frange) -> Option<(u64, u64)> {
        let state = self.state.read();
        if !matches!(&*state, SRState::Invalid) && self.frange.is_include(frange) {
            self.refs.fetch_add(1, Ordering::SeqCst);
            self.last_access_time
                .store(Utc::now().timestamp(), Ordering::SeqCst);
            Some((frange.off - self.frange.off, frange.len))
        } else {
            None
        }
    }

    fn unuse(&self) {
        self.refs.fetch_sub(1, Ordering::SeqCst);
    }

    fn retry_time(try_cnt: u32) -> std::time::Duration {
        if try_cnt < 30 {
            std::time::Duration::from_millis((try_cnt - 1) as u64 * 300 + 1)
        } else {
            std::time::Duration::from_secs(10)
        }
    }
}
