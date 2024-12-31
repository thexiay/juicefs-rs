use std::{
    cmp::min,
    fs,
    io::{self, Cursor, SeekFrom},
    path::{Path, PathBuf},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use opendal::Buffer;
use snafu::whatever;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::mpsc::Sender,
};
use tracing::error;

use crate::error::Result;

const CHECKSUM_BLOCK: u64 = 32 << 10; // 32KB

#[derive(Debug, Clone, PartialEq)]
pub enum ChecksumLevel {
    /// Disable checksum verification, if local cache data is tampered, bad data will be read
    None,
    /// Perform verification when reading the full block, use this for sequential read scenarios
    Full,
    /// Perform verification on parts that's fully included within the read range, use this for random read scenarios
    Shrink,
    /// Perform verification on parts that fully include the read range, this causes read amplifications and is only
    /// used for random read scenarios demanding absolute data integrity.
    Extend,
}

impl Default for ChecksumLevel {
    fn default() -> Self {
        ChecksumLevel::Full
    }
}

// Calculate 32-bits checksum for every 32 KiB data, so 512 Bytes for 4 MiB in total
pub fn checksum(buffer: &Buffer) -> Bytes {
    let length = buffer.len();
    let mut buf = BytesMut::with_capacity(((length - 1) / CHECKSUM_BLOCK as usize + 1) * 4);
    for i in (0..length).step_by(CHECKSUM_BLOCK as usize) {
        let sum = crc32fast::hash(
            &buffer
                .slice(i..min(i + CHECKSUM_BLOCK as usize, length))
                .to_vec(),
        );
        buf.put_u32(sum);
    }
    buf.freeze()
}

/// File cache buffer
/// /*****/
/// /------------------/------------------/-----/
/// / chunk block(32k) / chunk block(32k) / ... / checksum(4 bytes) * n /
/// /*****/
#[derive(Clone)]
pub struct FileBuffer {
    file_path: PathBuf,
    /// data length(non include checksum)
    length: usize,
    checksim_level: ChecksumLevel,
    disk_err_notifier: Option<Sender<io::ErrorKind>>,
}

impl FileBuffer {
    pub fn new(
        path: &Path,
        length: usize,
        checksum_level: ChecksumLevel,
        sender: Sender<io::ErrorKind>,
    ) -> io::Result<Self> {
        let meta = fs::metadata(path)?;
        let cache_file = FileBuffer {
            file_path: path.to_path_buf(),
            length,
            checksim_level: checksum_level.clone(),
            disk_err_notifier: Some(sender),
        };
        let err = Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid file size {}, data length {}, checksum level: {:?}",
                meta.len(),
                length,
                checksum_level
            ),
        ));
        match checksum_level {
            ChecksumLevel::None => {
                if meta.len() == length as u64 {
                    Ok(cache_file)
                } else {
                    err
                }
            }
            _ => {
                if meta.len() == ((length as u64 - 1) / CHECKSUM_BLOCK + 1) * 4 {
                    Ok(cache_file)
                } else {
                    err
                }
            }
        }
    }

    pub async fn read_at(&mut self, off: usize, len: usize) -> Result<Buffer> {
        match self.read(off, len).await {
            Ok(buf) => Ok(buf),
            Err(e) => {
                if let Some(kind) = e.try_into_io_error_kind()
                    && let Some(notifier) = &self.disk_err_notifier
                {
                    notifier
                        .send(kind)
                        .await
                        .err()
                        .iter()
                        .for_each(|e| error!("failed to send disk error {e}"));
                }
                Err(e)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn as_path(&self) -> &Path {
        &self.file_path
    }

    async fn read(&mut self, off: usize, len: usize) -> Result<Buffer> {
        if off + len > self.length {
            whatever!("read out of range")
        }

        let mut file = File::open(self.as_path()).await?;
        const CS_BLOCK: usize = CHECKSUM_BLOCK as usize;
        let (read, check, slice) = match self.checksim_level {
            ChecksumLevel::None => (off..off + len, None, 0..len),
            ChecksumLevel::Full => {
                if off == 0 && len == self.length {
                    (off..off + len, Some(off..off + len), 0..len)
                } else {
                    (off..off + len, None, 0..len)
                }
            }
            ChecksumLevel::Shrink => {
                let start = (off + CS_BLOCK - 1) / CS_BLOCK * CS_BLOCK;
                let end = (off + len) / CS_BLOCK * CS_BLOCK;
                (off..off + len, Some(start..end), 0..len)
            }
            ChecksumLevel::Extend => {
                let start = off / CS_BLOCK * CS_BLOCK;
                let end = (off + len + CS_BLOCK - 1) / CS_BLOCK * CS_BLOCK;
                (start..end, Some(start..end), off - start..off + len - start)
            }
        };
        let mut data = vec![0; read.end - read.start];
        file.seek(SeekFrom::Start(read.start as u64)).await?;
        file.read_exact(&mut data).await?;
        if let Some(check) = check {
            let cs_start = check.start / CS_BLOCK * 4 + self.length;
            let cs_end = (check.end + CS_BLOCK - 1) / CS_BLOCK * 4 + self.length;
            let mut cs_buf = vec![0; cs_end - cs_start];
            file.seek(SeekFrom::Start(cs_start as u64)).await?;
            file.read_exact(&mut cs_buf).await?;

            let mut cs_data_reader =
                data[check.start - read.start..check.end - read.start].chunks(CS_BLOCK);
            let mut cs_reader = Cursor::new(cs_buf);
            while cs_reader.has_remaining() {
                let except = cs_reader.get_u32();
                let actual = crc32fast::hash(cs_data_reader.next().unwrap());
                if actual != except {
                    whatever!("checksum failed at file {:?} block ", self.file_path);
                }
            }
        }
        data.truncate(slice.end);
        Ok(Buffer::from(data.split_off(slice.start)))
    }
}

#[cfg(test)]
mod test {

    use std::io::Write;

    use bytes::BufMut;
    use rand::Rng;
    use tempfile::NamedTempFile;

    use crate::buffer::{ChecksumLevel, FileBuffer, CHECKSUM_BLOCK};

    #[tokio::test]
    async fn test_file_buffer_checksum() {
        let mut data = vec![0; 2 * CHECKSUM_BLOCK as usize + 4];
        let mut cs = vec![];
        rand::thread_rng().fill(data.as_mut_slice());
        cs.put_u32(crc32fast::hash(&data[0..CHECKSUM_BLOCK as usize]));
        cs.put_u32(crc32fast::hash(
            &data[CHECKSUM_BLOCK as usize..2 * CHECKSUM_BLOCK as usize],
        ));
        cs.put_u32(crc32fast::hash(&data[2 * CHECKSUM_BLOCK as usize..]));
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&data).unwrap();
        file.write_all(&cs).unwrap();
        file.flush().unwrap();

        // Full
        let mut full_file = FileBuffer {
            file_path: file.path().to_path_buf(),
            length: data.len(),
            checksim_level: ChecksumLevel::Full,
            disk_err_notifier: None,
        };
        let res1 = full_file.read_at(0, data.len()).await.unwrap();
        assert_eq!(&res1.to_vec(), &data);
        assert!(full_file.read_at(0, data.len() + 1).await.is_err());

        // Extended
    }
}
