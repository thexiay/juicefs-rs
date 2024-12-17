use std::cmp::min;
use std::io::ErrorKind;
use std::{fs::Permissions, future::Future, sync::Arc};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Duration, Utc};
use futures::future::FutureExt;
use opendal::{Buffer, Operator};
use tracing::{error, warn};

use crate::error::Result;
use crate::{
    api::SliceReader, cache::CacheManager, compress::Compressor, pre_fetcher::PreFetcher,
    single_flight::SingleFlight,
};

// Config contains options for cachedStore
pub struct Config {
    cache_dir: String,
    cache_mode: Permissions,
    cache_size: u64,
    cache_checksum: String,
    cache_eviction: String,
    cache_scan_interval: Duration,
    cache_expire: Duration,
    free_space: f32,
    auto_create: bool,
    compress: String,
    max_upload: isize,
    max_stage_write: isize,
    max_retries: isize,
    upload_limit: i64,
    download_limit: i64,
    writeback: bool,
    upload_delay: Duration,
    upload_hours: String,
    is_hash_prefix: bool,
    max_block_size: usize,
    get_timeout: Duration,
    put_timeout: Duration,
    cache_full_block: bool,
    buffer_size: u64,
    readahead: isize,
    prefetch: isize,
}

impl Config {
    pub fn should_cache(&self, size: usize) -> bool {
        self.cache_full_block || size <= self.max_block_size || !self.upload_delay.is_zero()
    }
}

pub struct PendingItem {
    key: String,
    /// full path of local file corresponding
    fpath: String,
    /// timestamp when this item is added
    ts: DateTime<Utc>,
    uploading: bool,
}

pub struct RSlice<CM: CacheManager, COMP: Compressor> {
    storage: Arc<Operator>,
    cache_manager: Option<Arc<CM>>,
    compressor: Arc<COMP>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,
    conf: Arc<Config>,

    /// slice id
    id: u64,
    /// slice length
    length: usize,
}

impl<CM: CacheManager, COMP: Compressor> RSlice<CM, COMP> {
    /// The index of the block that first contains the offset
    fn block_index(&self, off: usize) -> usize {
        off / self.conf.max_block_size
    }

    /// The size of the this block at index
    fn block_size(&self, indx: usize) -> usize {
        let bsize = self.length - indx * self.conf.max_block_size;
        if bsize > self.conf.max_block_size {
            self.conf.max_block_size
        } else {
            bsize
        }
    }

    fn key(&self, indx: usize) -> String {
        if self.conf.is_hash_prefix {
            format!(
                "chunks/{:02X}/{}/{}_{}_{}",
                self.id % 256,
                self.id / 1000 / 1000,
                self.id,
                indx,
                self.block_size(indx)
            )
        } else {
            format!(
                "chunks/{}/{}/{}_{}_{}",
                self.id / 1000 / 1000,
                self.id / 1000,
                self.id,
                indx,
                self.block_size(indx)
            )
        }
    }

    fn should_partial_read(&self, off: usize, len: usize) -> bool {
        off % self.conf.max_block_size > 0 && len <= self.conf.max_block_size / 4
    }

    /// read data from storage and cache it
    async fn read_block(&self, key: &str, cache: bool, force_cache: bool) -> Result<Bytes> {
        /*
        defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("recovered from %s", e)
		}
	}()
	needed := store.compressor.CompressBound(len(page.Data))
	compressed := needed > len(page.Data)
	// we don't know the actual size for compressed block
	if store.downLimit != nil && !compressed {
		store.downLimit.Wait(int64(len(page.Data)))
	}
	err = errors.New("Not downloaded")
	var in io.ReadCloser
	tried := 0
	start := time.Now()
	var p *Page
	if compressed {
		c := NewOffPage(needed)
		defer c.Release()
		p = c
	} else {
		p = page
	}
	p.Acquire()
	var n int
	var (
		reqID string
		sc    = object.DefaultStorageClass
	)
	err = utils.WithTimeout(func() error {
		defer p.Release()
		// it will be retried outside
		for err != nil && tried < 2 {
			time.Sleep(time.Second * time.Duration(tried*tried))
			if tried > 0 {
				logger.Warnf("GET %s: %s; retrying", key, err)
				store.objectReqErrors.Add(1)
				start = time.Now()
			}
			in, err = store.storage.Get(key, 0, -1, object.WithRequestID(&reqID), object.WithStorageClass(&sc))
			tried++
		}
		if err == nil {
			n, err = io.ReadFull(in, p.Data)
			_ = in.Close()
		}
		if compressed && err == io.ErrUnexpectedEOF {
			err = nil
		}
		return err
	}, store.conf.GetTimeout)
	used := time.Since(start)
	logRequest("GET", key, "", reqID, err, used)
	if store.downLimit != nil && compressed {
		store.downLimit.Wait(int64(n))
	}
	store.objectDataBytes.WithLabelValues("GET", sc).Add(float64(n))
	store.objectReqsHistogram.WithLabelValues("GET", sc).Observe(used.Seconds())
	if err != nil {
		store.objectReqErrors.Add(1)
		return fmt.Errorf("get %s: %s", key, err)
	}
	if compressed {
		n, err = store.compressor.Decompress(page.Data, p.Data[:n])
	}
	if err != nil || n < len(page.Data) {
		return fmt.Errorf("read %s fully: %s (%d < %d) after %s (tried %d)", key, err, n, len(page.Data),
			used, tried)
	}
	if cache {
		store.bcache.cache(key, page, forceCache)
	}
	return nil
         */
        todo!()
    }
}

impl<CM, COMP> SliceReader for RSlice<CM, COMP>
where
    CM: CacheManager,
    COMP: Compressor,
{
    fn id(&self) -> u64 {
        self.id
    }

    fn read_at(&self, off: usize, len: usize) -> impl Future<Output = Result<Buffer>> + Send {
        async move {
            if len == 0 {
                return Ok(Bytes::new().into());
            }
            if off >= self.length || off + len > self.length {
                return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
            }

            let indx = self.block_index(off);
            let boff = off % self.conf.max_block_size;
            let block_size = self.block_size(indx);
            assert!(block_size > boff);
            let block_avail_size = block_size - boff;
            // read beyond current block page
            if len > block_avail_size {
                let mut got = 0;
                let mut off = off;
                let mut bufs = vec![];
                while got < len {
                    // align to current block page
                    let block_avail_size =
                        self.block_size(self.block_index(off)) - off % self.conf.max_block_size;
                    let left_size = len - got;
                    let buffer = self
                        .read_at(off, min(left_size, block_avail_size))
                        .boxed()
                        .await?;
                    if buffer.is_empty() {
                        return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
                    }
                    got += buffer.len();
                    off += buffer.len();
                    bufs.push(buffer);
                }
                return Ok(bufs.into_iter().flatten().collect());
            }

            // read current block page
            // read data from cache if cache is enable
            let key = self.key(indx);
            if let Some(cache_manager) = &self.cache_manager {
                match cache_manager.get(&key).await {
                    Ok(p) => {
                        return Ok(p.slice(0..min(len, p.len())).into());
                    }
                    Err(e) => {
                        cache_manager.remove(&key).await.unwrap_or_else(|err| {
                            error!("remove cache failed: {}", err);
                        });
                        warn!("read partial cached block {} fail: {}", key, e);
                    }
                }
            }

            // read from object storage
            if self.should_partial_read(off, len) {
                let reader = self.storage.reader(&key).await?;
                let range = boff as u64..(boff + len) as u64;
                match reader.read(range).await {
                    Ok(buffer) => return Ok(buffer),
                    // fallback to full read
                    Err(e) => error!("read partial block {} fail: {}", key, e),
                }
            }
            let cache = self
                .group
                .execute(&key, async || {
                    self.read_block(&key, self.conf.should_cache(block_size), false)
                        .await
                })
                .await?;
            Ok(cache.slice(boff..cache.len()).into())
        }
    }
}

pub struct WSlice<CM, COMP, const N: usize>
where
    CM: CacheManager,
    COMP: Compressor,
{
    storage: Arc<Operator>,
    cache_manager: Arc<CM>,
    compressor: Arc<COMP>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,

    id: u64,
    length: isize,
    /// For every block, has a vector of pages
    block_pages: [Vec<Bytes>; N],
    uploaded: isize,
}

struct CachedStore<CM, COMP>
where
    CM: CacheManager,
    COMP: Compressor,
{
    /// concurrent limit and rate limit and retry limit
    /// all of those could put it in accesssor layer
    /// TODO: modify upload limit and download limit at runtime, add compressor at runtime
    storage: Arc<Operator>,
    cache_manager: Arc<CM>,
    compressor: Arc<COMP>,
    fetcher: Arc<PreFetcher>,
    group: Arc<SingleFlight>,
    conf: Config,
    start_hour: u32,
    end_hour: u32,
}
