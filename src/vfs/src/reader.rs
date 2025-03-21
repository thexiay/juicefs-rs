use std::{
    cmp::{max, min}, collections::{HashMap, LinkedList}, ops::Deref, sync::{
        atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    }
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Duration, Local, Utc};
use juice_meta::api::{Ino, Meta, CHUNK_SIZE};
use juice_storage::api::{CachedStore, ChunkStore, SliceReader};
use parking_lot::{Mutex, RwLock};
use snafu::FromString;
use tokio::{
    sync::{Notify, Semaphore},
    task::{AbortHandle, JoinSet},
    time::sleep,
};
use tracing::{info, warn};

use crate::{config::Config, error::{EIOFailedTooManyTimesSnafu, Result, VfsError}, frange::Frange};

const READ_SESSION: usize = 2;

pub struct DataReaderCtx {
    meta: Arc<dyn Meta>,
    store: Arc<CachedStore>,
    max_block_size: u64,
    /// max readahead bytes in once readahead request
    max_once_readahead: u64,
    /// total readahead bytes in all files
    max_readahead: u64,
    /// used read buffer in all file readers
    used_read_buffer: AtomicU64,
    /// Max read requests in one [`FileReader`]
    /// After exceeding this value, the fileReader that is completed in the cache will be cleaned.
    max_request: usize,
    /// Max read retries count in one [`FileReader`], requests exceeding this value will fail
    max_retries: u32,
}

pub struct DataReaderInvalidator(Arc<DataReader>);

impl DataReaderInvalidator {
    pub fn invalid(&self, inode: Ino, frange: Frange) {
        self.0.invalidate(inode, frange);
    }
}

pub struct DataReader {
    ctx: Arc<DataReaderCtx>,
    file_readers: Mutex<HashMap<Ino, Vec<FileReader>>>,
}

impl DataReader {
    pub fn new(
        meta: Arc<dyn Meta>,
        store: Arc<CachedStore>,
        conf: Arc<Config>,
    ) -> Arc<DataReader> {
        let total_readahead = max(conf.chunk.buffer_size * 10 / 8, 256 << 20);  // default 256MB
        let max_readahead = min(max(8 * conf.chunk.max_block_size as u64, conf.chunk.read_ahead), total_readahead);
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
                            fr.core.lock().release_idle_buffer();
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
            let mut f = fr.core.lock();
            if length < fr.len() {
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
            let mut f = fr.core.lock();
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

struct SessionTrace {
    last_readahead_off: u64,
    // total sequential len about this session
    seq_readlen: u64,
    last_readahead_len: u64,
    last_atime: DateTime<Utc>,
}

struct FileReaderCtx {
    inode: Ino,
    retries: AtomicU32,
}

struct FileReaderCore {
    dr_ctx: Arc<DataReaderCtx>,
    f_ctx: Arc<FileReaderCtx>,
    length: u64,
    // The link list of all chunk readers
    sessions: [SessionTrace; READ_SESSION],
    // Shared between different `FileReader`
    chunk_readers: LinkedList<Arc<ChunkReader>>,
}

impl FileReaderCore {
    fn visit(&self, mut visitor: impl FnMut(&Arc<ChunkReader>)) {
        for chunk_reader in self.chunk_readers.iter() {
            visitor(chunk_reader);
        }
    }

    fn clean(&mut self) {
        self.chunk_readers
            .retain(|cr| !cr.is_invalid());
    }

    /// Cleanup the requests that are not needed to cache anymore
    fn cleanup_requests(&mut self, frange: &Frange) {
        let mut non_overlap_remains = 0;
        let guess_nedded_range = self.maybe_readahead_range();
        self.visit(|cr| {
            let is_overlap = frange.is_overlap(&cr.frange);
            let needed = is_overlap
                || (guess_nedded_range.iter().any(|r| r.is_overlap(&cr.frange))
                    && cr.is_out_dated(Duration::seconds(30)));
            if !needed {
                cr.close();
            } else if !is_overlap {
                non_overlap_remains += 1;
            }
        });
        self.clean();
        self.visit(|cr| {
            let is_overlap = frange.is_overlap(&cr.frange);
            let needed = is_overlap || non_overlap_remains <= self.dr_ctx.max_request;
            if !needed {
                cr.close();
                non_overlap_remains -= 1;
            }
        });
        self.clean();
    }

    /// release idle chunk reader
    fn release_idle_buffer(&mut self) {
        let used = self.dr_ctx.used_read_buffer.load(Ordering::SeqCst);
        let idle = if used > self.dr_ctx.max_readahead {
            let reduce = (used / self.dr_ctx.max_readahead) as i32;
            Duration::minutes(1) / reduce
        } else {
            Duration::minutes(1)
        };
        let guess_nedded_range = self.maybe_readahead_range();
        self.visit(|cr| {
            if cr.is_out_dated(idle) || !guess_nedded_range.iter().any(|r| r.is_overlap(&cr.frange))
            {
                cr.close();
            }
        });
        self.clean();
    }

    fn readahead(&mut self, mut frange: Frange) {
        // Find the first slice that overlaps with the readahead range and crop
        // the readahead according to the range of this slice.
        self.visit(|cr| {
            if !cr.is_invalid() && cr.frange.off <= frange.off && cr.frange.end() > frange.off {
                if cr.is_ready()
                    && frange.len > self.dr_ctx.max_block_size
                    && cr.frange.off == frange.off
                    && cr.frange.off % self.dr_ctx.max_block_size == 0
                {
                    // next block is ready, reduce readahead by a block
                    frange.len -= self.dr_ctx.max_block_size / 2;
                }
                frange.len = frange.end().checked_sub(cr.frange.end()).unwrap_or(0);
                frange.off = cr.frange.end();
            }
        });
        // do readahead
        if frange.len > 0
            && frange.off < self.length
            && self.dr_ctx.used_read_buffer.load(Ordering::SeqCst) < self.dr_ctx.max_readahead
        {
            if frange.len < self.dr_ctx.max_block_size {
                // align to end of a block
                frange.len +=
                    self.dr_ctx.max_block_size - frange.end() % self.dr_ctx.max_block_size;
            }
            self.new_slice(&mut frange);
            if frange.len > 0 {
                self.readahead(frange);
            }
        }
    }

    /// Prepare frange request and split out those reqs that already exist in the slice readers
    fn prepare_reqs(&mut self, frange: &Frange) -> Vec<ChunkReadReq> {
        let mut range_points = vec![frange.off, frange.end()];
        fn contain(ranges: &[u64], p: u64) -> bool {
            ranges.iter().any(|i| *i == p)
        }
        self.visit(|cr| {
            if !cr.is_invalid() {
                if frange.is_contain(cr.frange.off) && !contain(&range_points, cr.frange.off) {
                    range_points.push(cr.frange.off);
                }
                if frange.is_contain(cr.frange.end()) && !contain(&range_points, cr.frange.end()) {
                    range_points.push(cr.frange.end());
                }
            }
        });
        range_points.sort();

        let mut reqs = Vec::new();
        for i in 0..range_points.len() - 1 {
            let mut range = Frange {
                off: range_points[i],
                len: range_points[i + 1] - range_points[i],
            };
            let matched_cr = self
                .chunk_readers
                .iter()
                .find(|cr| !cr.is_invalid() && cr.frange.is_include(&range));
            match matched_cr {
                Some(cr) => {
                    cr.reuse();
                }
                None => {
                    while range.len > 0 {
                        let cr = self.new_slice(&mut range);
                        reqs.push(ChunkReadReq {
                            off: 0,
                            len: range.len,
                            chunk_reader: cr,
                        });
                    }
                }
            }
        }
        reqs
    }

    // Check if frange could readahead
    fn check_readahead(&mut self, frange: &Frange) {
        let session = &mut self.sessions[self.guess_session(frange)];
        let used = self.dr_ctx.used_read_buffer.load(Ordering::SeqCst);
        let is_first_readahead = session.last_readahead_len == 0
            && (frange.off == 0 || session.seq_readlen > frange.len);
        let is_readahead_double = session.last_readahead_len < self.dr_ctx.max_once_readahead
            && session.seq_readlen >= session.last_readahead_len
            && self.dr_ctx.max_readahead - used > session.last_readahead_len * 4;
        let is_readahead_halve = session.last_readahead_len >= self.dr_ctx.max_block_size
            && (self.dr_ctx.max_readahead - used < session.last_readahead_len / 2
                || session.seq_readlen < session.last_readahead_len * 4);

        let readahead_len = if is_first_readahead {
            self.dr_ctx.max_block_size
        } else if is_readahead_double {
            session.last_readahead_len * 2
        } else if is_readahead_halve {
            session.last_readahead_len / 2
        } else {
            session.last_readahead_len
        };
        // only readahead len over block size can do readahead
        if readahead_len >= self.dr_ctx.max_block_size {
            let ahead = Frange {
                off: frange.end(),
                len: session.last_readahead_len,
            };
            session.last_readahead_off = frange.end();
            session.last_readahead_len = readahead_len;
            self.readahead(ahead);
        }
    }

    fn guess_session(&mut self, frange: &Frange) -> usize {
        // Find the closest session info around frange
        let matched = self
            .sessions
            .iter()
            .enumerate()
            .filter_map(|(i, session)| {
                (session.last_readahead_off <= frange.off
                    && frange.off
                        < session.last_readahead_off
                            + session.last_readahead_len
                            + self.dr_ctx.max_block_size)
                    .then_some((i, session.last_readahead_off))
            })
            .max_by_key(|(_, off)| *off)
            .or_else(|| {
                self.sessions
                    .iter()
                    .enumerate()
                    .filter_map(|(i, session)| {
                        let bt = max(session.last_readahead_len / 8, self.dr_ctx.max_block_size);
                        let min = session.last_readahead_off.checked_sub(bt).unwrap_or(0);
                        (min <= frange.off && frange.off < session.last_readahead_off)
                            .then_some((i, session.last_readahead_off))
                    })
                    .min_by_key(|(_, off)| *off)
            });
        if let Some((i, _)) = matched {
            if frange.end() > self.sessions[i].last_readahead_off {
                self.sessions[i].seq_readlen += frange.end() - self.sessions[i].last_readahead_off;
            }
            self.sessions[i].last_atime = Utc::now();
            i
        } else {
            let i = self
                .sessions
                .iter()
                .position(|s| s.seq_readlen == 0)
                .unwrap_or_else(|| {
                    self.sessions
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, s)| s.last_atime)
                        .map(|(i, _)| i)
                        .unwrap()
                });
            self.sessions[i] = SessionTrace {
                last_readahead_off: frange.off,
                seq_readlen: frange.len,
                last_readahead_len: 0,
                last_atime: Utc::now(),
            };
            i
        }
    }

    fn new_slice(&mut self, frange: &mut Frange) -> Arc<ChunkReader> {
        let next_block_end =
            (frange.off / self.dr_ctx.max_block_size + 1) * self.dr_ctx.max_block_size;
        let single_read_range = Frange {
            off: frange.off,
            len: min(frange.len, next_block_end - frange.off),
        };
        frange.off = single_read_range.end();
        frange.len -= single_read_range.len;
        self.dr_ctx
            .used_read_buffer
            .fetch_add(single_read_range.len, Ordering::SeqCst);
        let cr = ChunkReader::new(self.dr_ctx.clone(), self.f_ctx.clone(), single_read_range);
        self.chunk_readers.push_back(cr.clone());
        cr
    }

    /// Return the readahead range for the current file reader
    fn maybe_readahead_range(&self) -> Vec<Frange> {
        self.sessions
            .iter()
            .map(|s| {
                if s.seq_readlen == 0 {
                    Frange { off: 0, len: 0 }
                } else {
                    let bt = max(s.last_readahead_len / 8, self.dr_ctx.max_block_size);
                    Frange {
                        off: s.last_readahead_off.checked_sub(bt).unwrap_or(0),
                        len: s.last_readahead_len * 2 + self.dr_ctx.max_block_size * 2,
                    }
                }
            })
            .collect()
    }
}

pub struct FileReaderRef {
    reader: FileReader,
    dr: Arc<DataReader>,
}

impl FileReaderRef {
    fn new(reader: FileReader, dr: Arc<DataReader>) -> Self {
        Self { reader, dr }
    }
}

impl Deref for FileReaderRef {
    type Target = FileReader;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl Drop for FileReaderRef {
    fn drop(&mut self) {
        if self.refs.load(Ordering::Relaxed) == 0 {
            let mut files = self.dr.file_readers.lock();
            files.remove(&self.reader.f_ctx.inode);
        }
    }
}

/// A File Reader represent a reader of `File Handle`.
/// It can shared in different thread.
pub struct FileReader {
    // data reader的上下文信息
    dr_ctx: Arc<DataReaderCtx>,
    f_ctx: Arc<FileReaderCtx>,
    core: Arc<Mutex<FileReaderCore>>,
    refs: Arc<AtomicU32>,
}

impl Clone for FileReader {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::Relaxed);
        Self {
            dr_ctx: self.dr_ctx.clone(),
            f_ctx: self.f_ctx.clone(),
            core: self.core.clone(),
            refs: self.refs.clone(),
        }
    }
}

impl Drop for FileReader {
    fn drop(&mut self) {
        self.refs.fetch_sub(1, Ordering::Relaxed);
    }
}

impl FileReader {
    fn new(inode: u64, length: u64, ctx: Arc<DataReaderCtx>) -> FileReader {
        let f_ctx = Arc::new(FileReaderCtx {
            inode,
            retries: AtomicU32::new(0),
        });
        FileReader {
            dr_ctx: ctx.clone(),
            f_ctx: f_ctx.clone(),
            core: Arc::new(Mutex::new(FileReaderCore {
                dr_ctx: ctx,
                f_ctx,
                length,
                sessions: std::array::from_fn(|_| SessionTrace {
                    last_readahead_off: 0,
                    seq_readlen: 0,
                    last_readahead_len: 0,
                    last_atime: Utc::now(),
                }),
                chunk_readers: LinkedList::new(),
            })),
            refs: Arc::new(AtomicU32::new(0)),
        }
    }

    pub async fn read(&self, offset: u64, buf: &mut BytesMut) -> Result<u64> {
        if self.f_ctx.retries.load(Ordering::SeqCst) > self.dr_ctx.max_retries {
            return EIOFailedTooManyTimesSnafu.fail()?;
        }
        let reqs = {
            let mut f = self.core.lock();
            let size = buf.len();
            if offset >= f.length || size == 0 {
                return Ok(0);
            }
            let frange = Frange {
                off: offset,
                // reads that exceed the file length will be cropped
                len: min(size as u64, f.length - offset),
            };
            f.cleanup_requests(&frange);
            // small file read(< 32KB)
            let last_bs = 32 << 10;
            if frange.off + last_bs > f.length {
                let last_frange = Frange {
                    off: f.length.checked_sub(last_bs).unwrap_or(0),
                    len: min(f.length, last_bs),
                };
                f.readahead(last_frange);
            }
            // reuse before slice reader requests
            let reqs = f.prepare_reqs(&frange);
            f.check_readahead(&frange);
            reqs
        };
        // allow read to parallelized
        let io_res = self.wait_for_io(reqs, buf).await;
        {
            let mut f = self.core.lock();
            f.clean();
        }
        io_res
    }

    pub fn len(&self) -> u64 {
        let f = self.core.lock();
        f.length
    }

    async fn wait_for_io(&self, reqs: Vec<ChunkReadReq>, buf: &mut BytesMut) -> Result<u64> {
        let mut req_len = 0;
        for req in reqs.iter() {
            let cr = &req.chunk_reader;
            let bytes = cr.get().await?;

            req_len += req.len;
            buf.extend_from_slice(&bytes[req.off as usize..req.off as usize + req.len as usize]);
        }
        Ok(req_len)
    }
}

struct SliceReadReq {
    slice_id: u64,
    slice_len: u64,
    off: u32,
    buf: BytesMut,
}

// Shared read request based on an existing slice read
struct ChunkReadReq {
    // off in `ChunkReader` frange
    off: u64,
    len: u64,
    chunk_reader: Arc<ChunkReader>,
}

impl Drop for ChunkReadReq {
    fn drop(&mut self) {
        self.chunk_reader.refs.fetch_sub(1, Ordering::SeqCst);
    }
}

// A ChunkReader represent a read request for a slice of chunk of data
struct ChunkReader {
    dr_ctx: Arc<DataReaderCtx>,
    fr_ctx: Arc<FileReaderCtx>,
    // This frange is only in ONE chunk(event one block)
    frange: Frange,
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
    fn new(dr_ctx: Arc<DataReaderCtx>, fr_ctx: Arc<FileReaderCtx>, frange: Frange) -> Arc<Self> {
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
        match self.dr_ctx.meta.read(self.fr_ctx.inode, chunk_idx).await {
            Ok(slices) => {    
                let mut foff = self.frange.off / CHUNK_SIZE * CHUNK_SIZE;
                let mut buf = BytesMut::with_capacity(self.frange.len as usize);
                let mut slice_reqs = Vec::new();
                for slice in slices {
                    if buf.is_empty() {
                        break;
                    }
                    let slice_frange = Frange {
                        off: foff,
                        len: slice.len as u64,
                    };
                    if let Some(overlap) = self.frange.overlap(&slice_frange) {
                        assert!(
                            buf.len() >= overlap.len as usize,
                            "meta return invalid slices"
                        );
                        slice_reqs.push(SliceReadReq {
                            slice_id: slice.id,
                            slice_len: slice.len as u64,
                            off: slice.off + (overlap.off - foff) as u32,
                            buf: buf.split_to(overlap.len as usize),
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
                        .invalidate_chunk_cache(self.fr_ctx.inode, chunk_idx);
                    let retry_cnt = self.fr_ctx.retries.fetch_add(1, Ordering::SeqCst);
                    if retry_cnt > self.dr_ctx.max_retries {
                        self.fail(
                            Arc::new(VfsError::without_source(
                                "read chunk failed too many times".to_string(),
                            )),
                            &task_time,
                        );
                    } else {
                        self.clone().refresh(Self::retry_time(retry_cnt + 1));
                    }
                }
            }
            Err(e) if e.is_no_entry_found2(&self.fr_ctx.inode) => {
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

    fn invalid(self: Arc<Self>) {
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

    fn close(&self) {
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

    async fn read(self: Arc<Self>, slice_reqs: Vec<SliceReadReq>) -> Bytes {
        let limit = Arc::new(Semaphore::new(16));
        let mut task_set: JoinSet<Option<(usize, BytesMut)>> = JoinSet::new();
        slice_reqs.into_iter().enumerate().for_each(|(i, req)| {
            let limit_cloned = limit.clone();
            let sr = self.clone();
            task_set.spawn(async move {
                let _premit = limit_cloned
                    .acquire()
                    .await
                    .expect("acquire semaphore failed");
                let inode = sr.fr_ctx.inode;
                let id = req.slice_id;
                let off = req.off;
                let len = req.buf.len();
                sr.read_slice(req).await.inspect_err(|e| {
                    warn!(inode = %inode, id = %id, off = %off, len = %len, "read slice failed: {:?}", e);
                }).map(|buf| (i, buf)).ok()
            });
        });
        let mut all_buf = Vec::new();
        while let Some(res) = task_set.join_next().await {
            match res {
                Ok(Some(res)) => all_buf.push(res),
                Ok(None) => {
                    return Bytes::new();
                }
                Err(e) => {
                    warn!("read slice coroutine failed: {:?}", e);
                    return Bytes::new();
                }
            }
        }
        all_buf.sort_by_key(|(i, _)| *i);
        let mut buf = BytesMut::new();
        for (_, b) in all_buf {
            buf.unsplit(b);
        }
        buf.freeze()
    }

    async fn read_slice(&self, req: SliceReadReq) -> juice_storage::error::Result<BytesMut> {
        if req.slice_len == 0 {
            return Ok(req.buf);
        }
        let reader = self
            .dr_ctx
            .store
            .new_reader(req.slice_id, req.slice_len as usize);
        reader
            .read_all_at(req.off as usize, req.buf.len())
            .await?;
        Ok(req.buf)
    }

    async fn get(&self) -> Result<Bytes> {
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

    fn is_invalid(&self) -> bool {
        let state = self.state.read();
        matches!(&*state, SRState::Invalid)
    }

    fn is_ready(&self) -> bool {
        let state = self.state.read();
        matches!(&*state, SRState::Ready(_))
    }

    fn is_out_dated(&self, time: Duration) -> bool {
        let now = Utc::now();
        let ts = DateTime::from_timestamp(self.last_access_time.load(Ordering::SeqCst), 0)
            .expect("invalid timestamp");
        ts + time < now
    }

    fn reuse(&self) {
        self.refs.fetch_add(1, Ordering::SeqCst);
        self.last_access_time
            .store(Utc::now().timestamp(), Ordering::SeqCst);
    }

    fn retry_time(try_cnt: u32) -> std::time::Duration {
        if try_cnt < 30 {
            std::time::Duration::from_millis((try_cnt - 1) as u64 * 300 + 1)
        } else {
            std::time::Duration::from_secs(10)
        }
    }
}
