use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use juice_meta::{
    api::{new_client, Falloc, Meta, OFlag, ROOT_INODE},
    config::{Config as MetaConfig, Format as MetaFormat},
};
use juice_storage::api::{new_chunk_store, Config as ChunkConfig};
use nix::unistd::{getpid, getppid};
use opendal::{services::Memory, Operator};
use parking_lot::Mutex;
use tokio::time::sleep;
use tracing::info;
use tracing_test::traced_test;

use crate::{config::Config, vfs::Vfs};

// TODO: fix it with mem object and mem meta. But at now only has redis meta.
async fn new_vfs() -> Vfs {
    let meta_conf = MetaConfig::default();
    let meta = new_client("redis://:mypassword@127.0.0.1:6379", Default::default()).await;
    let format = MetaFormat {
        name: "test".to_string(),
        storage: "redis".to_string(),
        page_size: 4096,
        enable_dir_stats: true,
        ..Default::default()
    };
    let chunk_conf = ChunkConfig {
        cache_type: "mem".to_string(),
        max_block_size: format.page_size as usize * 1024,
        max_upload: 2,
        buffer_size: 30 << 20,
        cache_size: 10 << 20,
        ..Default::default()
    };
    meta.reset().await.expect("reset: ");
    meta.init(format.clone(), true).await.expect("setting: ");
    let operator = Operator::new(Memory::default().root("/tmp"))
        .unwrap()
        .finish();
    let store = new_chunk_store(chunk_conf.clone(), operator).expect("store init: ");
    let config = Config {
        meta: Arc::new(meta_conf),
        chunk: Arc::new(chunk_conf),
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
    Vfs::new(config, meta, store)
}

async fn test_vfs_meta() {}

#[traced_test]
#[tokio::test]
async fn test_vfs_io() {
    let vfs = new_vfs().await;
	
    let (fh, entry) = vfs
        .create(ROOT_INODE, "file", 0o755, 0, OFlag::O_RDWR)
        .await
        .expect("create file:");
    vfs.fallocate(entry.inode, Falloc::NONE, 0, 64 << 10, fh)
        .await
        .expect("fallocate:");
	
    vfs.write(entry.inode, fh, 0, Bytes::from_static(b"hello"))
        .await
        .expect("write file:");
	
    vfs.fsync(entry.inode, fh).await.expect("fsync file:");
    vfs.write(entry.inode, fh, 100 << 20, Bytes::from_static(b"world"))
        .await
        .expect("write file:");

    let _ = vfs
        .truncate(entry.inode, fh, (100 << 20) + 2)
        .await
        .expect("truncate file:");
    let n = vfs
        .copy_file_range(entry.inode, fh, 0, entry.inode, fh, 10 << 20, 10, 0)
        .await
        .expect("copy file range");
    assert_eq!(n, 10);

	let buf = vfs.read(entry.inode, fh, 0, 128 << 10).await.expect("read file err");
	assert_eq!(buf.len(), 128 << 10);  // head write
	assert_eq!(&(buf.slice(0..5))[..], b"hello");
    
    let buf = vfs.read(entry.inode, fh, 10 << 20, 6).await.expect("read file end");
    assert_eq!(buf.len(), 6); // copyed range
    assert_eq!(&buf[..], b"hello\x00");

    let buf = vfs.read(entry.inode, fh, 100 << 20, 128 << 10).await.expect("read file end");
    assert_eq!(buf.len(), 2);  // truncated
 	assert_eq!(&buf[..], b"wo");
    vfs.flush(entry.inode, fh, 0).await.expect("flush file:");
    
    /*
    // edge cases
    _, fh2, _ := v.Open(ctx, fe.Inode, syscall.O_RDONLY)
    _, fh3, _ := v.Open(ctx, fe.Inode, syscall.O_WRONLY)
    wHandle := v.findHandle(fe.Inode, fh3)
    if wHandle == nil {
        t.Fatalf("failed to find O_WRONLY handle")
    }
    wHandle.reader = nil
    // read
    if _, e = v.Read(ctx, fe.Inode, nil, 0, 0); e != syscall.EBADF {
        t.Fatalf("read bad fd: %s", e)
    }
    if _, e = v.Read(ctx, fe.Inode, make([]byte, 1024), 0, fh3); e != syscall.EBADF {
        t.Fatalf("read write-only fd: %s", e)
    }
    if _, e = v.Read(ctx, fe.Inode, nil, 1<<60, fh2); e != syscall.EFBIG {
        t.Fatalf("read off too big: %s", e)
    }
    // write
    if e = v.Write(ctx, fe.Inode, nil, 0, 0); e != syscall.EBADF {
        t.Fatalf("write bad fd: %s", e)
    }
    if e = v.Write(ctx, fe.Inode, nil, 1<<60, fh2); e != syscall.EFBIG {
        t.Fatalf("write off too big: %s", e)
    }
    if e = v.Write(ctx, fe.Inode, make([]byte, 1024), 0, fh2); e != syscall.EBADF {
        t.Fatalf("write read-only fd: %s", e)
    }
    // truncate
    if e = v.Truncate(ctx, fe.Inode, -1, 0, &meta.Attr{}); e != syscall.EINVAL {
        t.Fatalf("truncate invalid off,length: %s", e)
    }
    if e = v.Truncate(ctx, fe.Inode, 1<<60, 0, &meta.Attr{}); e != syscall.EFBIG {
        t.Fatalf("truncate too large: %s", e)
    }
    // fallocate
    if e = v.Fallocate(ctx, fe.Inode, 0, -1, -1, fh); e != syscall.EINVAL {
        t.Fatalf("fallocate invalid off,length: %s", e)
    }
    if e = v.Fallocate(ctx, statsInode, 0, 0, 1, fh); e != syscall.EPERM {
        t.Fatalf("fallocate invalid off,length: %s", e)
    }
    if e = v.Fallocate(ctx, fe.Inode, 0, 0, 100, 0); e != syscall.EBADF {
        t.Fatalf("fallocate invalid off,length: %s", e)
    }
    if e = v.Fallocate(ctx, fe.Inode, 0, 1<<60, 1<<60, fh); e != syscall.EFBIG {
        t.Fatalf("fallocate invalid off,length: %s", e)
    }
    if e = v.Fallocate(ctx, fe.Inode, 0, 1<<10, 1<<20, fh2); e != syscall.EBADF {
        t.Fatalf("fallocate read-only fd: %s", e)
    }

    // copy file range
    if n, e := v.CopyFileRange(ctx, statsInode, fh, 0, fe.Inode, fh, 10<<20, 10, 0); e != syscall.ENOTSUP {
        t.Fatalf("copyfilerange internal file: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, fh, 0, statsInode, fh, 10<<20, 10, 0); e != syscall.EPERM {
        t.Fatalf("copyfilerange internal file: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, 0, 0, fe.Inode, fh, 10<<20, 10, 0); e != syscall.EBADF {
        t.Fatalf("copyfilerange invalid fh: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, fh, 0, fe.Inode, 0, 10<<20, 10, 0); e != syscall.EBADF {
        t.Fatalf("copyfilerange invalid fh: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, fh, 0, fe.Inode, fh, 10<<20, 10, 1); e != syscall.EINVAL {
        t.Fatalf("copyfilerange invalid flag: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, fh, 0, fe.Inode, fh, 10<<20, 1<<50, 0); e != syscall.EINVAL {
        t.Fatalf("copyfilerange overlap: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, fh, 0, fe.Inode, fh, 1<<63, 1<<63, 0); e != syscall.EFBIG {
        t.Fatalf("copyfilerange too big file: %s %d", e, n)
    }
    if n, e := v.CopyFileRange(ctx, fe.Inode, fh, 0, fe.Inode, fh2, 1<<20, 1<<10, 0); e != syscall.EACCES {
        t.Fatalf("copyfilerange too big file: %s %d", e, n)
    }

    // sequntial write/read
    for i := uint64(0); i < 1001; i++ {
        if e := v.Write(ctx, fe.Inode, make([]byte, 128<<10), i*(128<<10), fh); e != 0 {
            t.Fatalf("write big file: %s", e)
        }
    }
    buf = make([]byte, 128<<10)
    for i := uint64(0); i < 1000; i++ {
        if n, e := v.Read(ctx, fe.Inode, buf, i*(128<<10), fh); e != 0 || n != (128<<10) {
            t.Fatalf("read big file: %s", e)
        } else {
            for j := 0; j < 128<<10; j++ {
                if buf[j] != 0 {
                    t.Fatalf("read big file: %d %d", j, buf[j])
                }
            }
        }
    }
    // many small write
    buf = make([]byte, 5<<10)
    for j := range buf {
        buf[j] = 1
    }
    for i := int64(32 - 1); i >= 0; i-- {
        if e := v.Write(ctx, fe.Inode, buf, uint64(i)*(4<<10), fh); e != 0 {
            t.Fatalf("write big file: %s", e)
        }
    }
    time.Sleep(time.Millisecond * 1500) // wait for it to be flushed
    buf = make([]byte, 128<<10)
    if n, e := v.Read(ctx, fe.Inode, buf, 0, fh); e != 0 || n != (128<<10) {
        t.Fatalf("read big file: %s", e)
    } else {
        for j := range buf {
            if buf[j] != 1 {
                t.Fatalf("read big file: %d %d", j, buf[j])
            }
        }
    }

    v.Release(ctx, fe.Inode, fh)
     */
}

async fn test_vfs_xattr() {}

async fn test_vfs_access() {}

async fn test_vfs_attr() {}

async fn test_vfs_locks() {}

async fn test_vfs_internal_files() {}

async fn test_vfs_readdir_cache() {}
