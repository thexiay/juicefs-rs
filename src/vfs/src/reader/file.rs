use std::{
    cmp::{max, min},
    collections::LinkedList,
    ops::Deref,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use chrono::{DateTime, Duration, Utc};
use juice_meta::api::Ino;
use parking_lot::{Mutex, MutexGuard};
use tracing::info;

use crate::{
    error::{EIOFailedTooManyTimesSnafu, Result, VfsError},
    frange::Frange,
};

use super::{
    chunk::{ChunkReadReq, ChunkReader, ReusableFRange},
    data::DataReaderCtx,
    DataReader,
};

pub const READ_SESSION: usize = 2;
struct SessionTrace {
    last_readahead_off: u64,
    // total sequential len about this session
    seq_readlen: u64,
    last_readahead_len: u64,
    last_atime: DateTime<Utc>,
}

pub struct FileReaderCtx {
    pub(crate) ino: Ino,
    pub(crate) retries: AtomicU32,
}

pub struct FileReaderCore {
    dr_ctx: Arc<DataReaderCtx>,
    f_ctx: Arc<FileReaderCtx>,
    pub(crate) length: u64,
    // The link list of all chunk readers
    sessions: [SessionTrace; READ_SESSION],
    // Shared between different `FileReader`
    chunk_readers: LinkedList<ChunkReadReq>,
}

impl FileReaderCore {
    pub fn visit(&self, mut visitor: impl FnMut(&Arc<ChunkReader>)) {
        for chunk_reader in self.chunk_readers.iter() {
            visitor(&chunk_reader.chunk_reader);
        }
    }

    pub fn clean(&mut self) {
        self.chunk_readers
            .retain(|cr| !cr.chunk_reader.is_invalid());
    }

    fn into_chunks(&mut self, offset: u64, size: u64) -> Option<Vec<ChunkReadReq>> {
        if offset >= self.length || size == 0 {
            return None;
        }
        let frange = Frange {
            off: offset,
            // reads that exceed the file length will be cropped
            len: min(size as u64, self.length - offset),
        };
        self.cleanup_requests(&frange);
        // small file read(< 32KB)
        // TODO: enable readahead later
        /*
        let last_bs = 32 << 10;
        if frange.off + last_bs > self.length {
            let last_frange = Frange {
                off: self.length.checked_sub(last_bs).unwrap_or(0),
                len: min(self.length, last_bs),
            };
            self.readahead(last_frange);
        }
         */

        // reuse before slice reader requests
        let reqs = Self::prepare_slices(&mut self.chunk_readers, &frange, &mut |mut range| {
            let mut sub_reqs = Vec::new();
            while range.len > 0 {
                sub_reqs.push(Self::new_slice(
                    self.dr_ctx.clone(),
                    self.f_ctx.clone(),
                    &mut range,
                ));
            }
            sub_reqs
        });
        // TODO: enable readahead later
        // self.check_readahead(&frange);
        Some(reqs)
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
    pub fn release_idle_buffer(&mut self) {
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
        // Find the first slice that overlaps with the readahead range and drop
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
            self.chunk_readers.push_back(Self::new_slice(
                self.dr_ctx.clone(),
                self.f_ctx.clone(),
                &mut frange,
            ));
            if frange.len > 0 {
                self.readahead(frange);
            }
        }
    }

    fn prepare_slices<C, T, F>(slices: &mut C, frange: &Frange, on_new_slice: &mut F) -> Vec<T>
    where
        for<'a> &'a C: IntoIterator<Item = &'a T>,
        C: Extend<T>,
        T: ReusableFRange,
        F: FnMut(Frange) -> Vec<T>,
    {
        if frange.len == 0 {
            return Vec::new();
        }
        let mut range_points = vec![frange.off, frange.end()];
        fn contain(ranges: &[u64], p: u64) -> bool {
            ranges.iter().any(|i| *i == p)
        }
        {
            slices.into_iter().for_each(|cr| {
                if cr.valid() {
                    let cr_frange = cr.frange();
                    if frange.is_contain(cr_frange.off) && !contain(&range_points, cr_frange.off) {
                        range_points.push(cr_frange.off);
                    }
                    if frange.is_contain(cr_frange.end())
                        && !contain(&range_points, cr_frange.end())
                    {
                        range_points.push(cr_frange.end());
                    }
                }
            });
        }
        range_points.sort();

        let mut reqs = Vec::new();
        for i in 0..range_points.len() - 1 {
            let range = Frange {
                off: range_points[i],
                len: range_points[i + 1] - range_points[i],
            };
            let reused_cr = slices.into_iter().find_map(|cr| cr.reuse(&range)).clone();
            match reused_cr {
                Some(cr) => reqs.push(cr),
                None => {
                    let sub_reqs = on_new_slice(range);
                    reqs.extend(sub_reqs.clone());
                    slices.extend(sub_reqs);
                }
            }
        }
        reqs
    }

    fn new_slice(
        dr_ctx: Arc<DataReaderCtx>,
        fr_ctx: Arc<FileReaderCtx>,
        frange: &mut Frange,
    ) -> ChunkReadReq {
        let next_block_end = (frange.off / dr_ctx.max_block_size + 1) * dr_ctx.max_block_size;
        let single_read_range = Frange {
            off: frange.off,
            len: min(frange.len, next_block_end - frange.off),
        };
        frange.off = single_read_range.end();
        frange.len -= single_read_range.len;
        dr_ctx
            .used_read_buffer
            .fetch_add(single_read_range.len, Ordering::SeqCst);
        ChunkReadReq {
            off: 0,
            len: single_read_range.len,
            chunk_reader: ChunkReader::new(dr_ctx, fr_ctx, single_read_range),
        }
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

/// A File Reader represent a reader of `File Handle`.
/// It can shared in different thread.
pub struct FileReader {
    // data reader的上下文信息
    dr_ctx: Arc<DataReaderCtx>,
    fr_ctx: Arc<FileReaderCtx>,
    core: Arc<Mutex<FileReaderCore>>,
    refs: Arc<AtomicU32>,
}

impl Clone for FileReader {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::Relaxed);
        Self {
            dr_ctx: self.dr_ctx.clone(),
            fr_ctx: self.fr_ctx.clone(),
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
    pub fn new(inode: u64, length: u64, ctx: Arc<DataReaderCtx>) -> FileReader {
        let f_ctx = Arc::new(FileReaderCtx {
            ino: inode,
            retries: AtomicU32::new(0),
        });
        FileReader {
            dr_ctx: ctx.clone(),
            fr_ctx: f_ctx.clone(),
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

    pub async fn read(&self, offset: u64, size: u64, buf: &mut BytesMut) -> Result<u64> {
        if self.fr_ctx.retries.load(Ordering::SeqCst) > self.dr_ctx.max_retries {
            return EIOFailedTooManyTimesSnafu.fail()?;
        }
        let reqs = {
            match self.core.lock().into_chunks(offset, size) {
                Some(reqs) => reqs,
                None => return Ok(0),
            }
        };
        // allow read to parallelized
        let io_res = self.wait_for_io(reqs, buf).await;
        {
            let mut f = self.core.lock();
            f.clean();
        }
        io_res
    }

    pub fn lock(&self) -> MutexGuard<'_, FileReaderCore> {
        self.core.lock()
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

pub struct FileReaderRef {
    reader: FileReader,
    dr: Arc<DataReader>,
}

impl FileReaderRef {
    pub fn new(reader: FileReader, dr: Arc<DataReader>) -> Self {
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
            files.remove(&self.reader.fr_ctx.ino);
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use juice_meta::{
        api::new_client,
        config::{Config as MetaConfig, Format as MetaFormat},
    };
    use juice_storage::{api::{new_chunk_store, Config as ChunkConfig}, CacheType};
    use juice_utils::common::meta::StorageType;
    use nix::unistd::{getpid, getppid};
    use opendal::{services::Memory, Operator};
    use tracing_test::traced_test;

    use crate::{
        config::Config,
        frange::Frange,
        reader::{chunk::ReusableFRange, file::FileReaderCore},
    };

    use super::DataReader;

    #[derive(Clone, PartialEq, Eq, Debug)]
    struct TestFrange {
        off: u64,
        len: u64,
        frange: Frange,
    }

    impl TestFrange {
        fn new(frange: Frange) -> Self {
            TestFrange {
                off: 0,
                len: frange.len,
                frange,
            }
        }
    }

    impl ReusableFRange for TestFrange {
        fn frange(&self) -> &Frange {
            &self.frange
        }

        fn valid(&self) -> bool {
            true
        }

        fn reuse(&self, frange: &Frange) -> Option<Self> {
            if self.frange.is_include(&frange) {
                Some(TestFrange {
                    off: frange.off - self.frange.off,
                    len: frange.len,
                    frange: self.frange.clone(),
                })
            } else {
                None
            }
        }
    }

    async fn mock_reader() -> Arc<DataReader> {
        // meta
        let meta_conf = MetaConfig::default();
        let meta = new_client("redis://:mypassword@127.0.0.1:6379", meta_conf.clone()).await;
        let format = MetaFormat {
            name: "test".to_string(),
            storage: StorageType::default(),
            block_size_kb: 4096,
            enable_dir_stats: true,
            ..Default::default()
        };
        meta.reset().await.expect("reset: ");
        meta.init(format.clone(), true).await.expect("setting: ");

        // chunk
        let chunk_conf = ChunkConfig {
            cache_type: CacheType::Memory,
            max_block_size: format.block_size_kb as usize * 1024,
            max_upload: 2,
            buffer_size: 30 << 20,
            cache_size: 10 << 20,
            ..Default::default()
        };

        let operator = Operator::new(Memory::default().root("/tmp"))
            .unwrap()
            .finish();
        let store = Arc::new(new_chunk_store(chunk_conf.clone(), operator).expect("store init: "));
        let config = Config {
            meta: meta_conf,
            chunk: chunk_conf,
            format,
            version: "Juicefs".to_string(),
            pid: getpid().as_raw(),
            ppid: getppid().as_raw(),
            port: None,
            attr_timeout: None,
            dir_entry_timeout: None,
            entry_timeout: None,
            backup_meta: None,
            fast_resolve: None,
            access_log: None,
            prefix_internal: false,
            hide_internal: false,
            root_squash: None,
            non_default_permission: None,
            comm_path: None,
            state_path: None,
            fuse_opts: None,
        };
        DataReader::new(meta, store, Arc::new(config))
    }

    // file reader
    #[traced_test]
    #[tokio::test]
    async fn test_file_read_decompose() {
        let mut on_slice_new = |frange: Frange| vec![TestFrange::new(frange)];
        let mut slices = vec![
            TestFrange::new((0..10).into()),
            TestFrange::new((20..30).into()),
        ];
        // case1: overlap two slices
        let reqs =
            FileReaderCore::prepare_slices(&mut slices, &(5..25).into(), &mut on_slice_new);
        assert_eq!(reqs.len(), 3);
        assert_eq!(reqs[0], TestFrange {
            off: 5,
            len: 5,
            frange: Frange { off: 0, len: 10 },
        });
        assert_eq!(reqs[1], TestFrange {
            off: 0,
            len: 10,
            frange: Frange { off: 10, len: 10 },
        });
        assert_eq!(reqs[2], TestFrange {
            off: 0,
            len: 5,
            frange: Frange { off: 20, len: 10 },
        });
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0], TestFrange::new((0..10).into()));
        assert_eq!(slices[1], TestFrange::new((20..30).into()));
        assert_eq!(slices[2], TestFrange::new((10..20).into()));

        // case2: empty reqs        
        let reqs = FileReaderCore::prepare_slices(&mut slices, &(0..0).into(), &mut on_slice_new);
        assert_eq!(reqs.len(), 0);
    }

    fn test_readahead() {}

    // chunk reader
    fn test_chunk_slice_read() {
        // 检测读取chunk中某一段slice，读取哪些slice，这个算法是否ok
    }
}
