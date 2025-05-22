use std::{ops::Index, sync::Arc, time::Duration};

use bytes::Bytes;
use juice_meta::{
    api::{new_client, Falloc, Meta, OFlag, ROOT_INODE},
    config::{Config as MetaConfig, Format as MetaFormat},
};
use juice_storage::{api::{new_chunk_store, Config as ChunkConfig}, CacheType};
use juice_utils::common::meta::StorageType;
use nix::{
    errno::Errno,
    unistd::{getpid, getppid},
};
use opendal::{services::Memory, Operator};
use tokio::time::sleep;
use tracing::info;
use tracing_test::traced_test;

use crate::{config::Config, internal::STATS_INODE, vfs::Vfs};

// TODO: fix it with mem object and mem meta. But at now only has redis meta.
async fn new_vfs() -> Vfs {
    let meta_conf = MetaConfig::default();
    let meta = new_client("redis://:mypassword@127.0.0.1:6379", Default::default()).await;
    let format = MetaFormat {
        name: "test".to_string(),
        storage: StorageType::Mem,
        block_size_kb: 4096,
        enable_dir_stats: true,
        ..Default::default()
    };
    let chunk_conf = ChunkConfig {
        cache_type: CacheType::Memory,
        max_block_size: format.block_size_kb as usize * 1024,
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
    Vfs::new(config, meta, store)
}

async fn test_vfs_meta() {}

#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_vfs_io() {
    info!("1");
    let vfs = new_vfs().await;

    let (fh, entry) = vfs
        .create(ROOT_INODE, "file", 0o755, 0, OFlag::O_RDWR)
        .await
        .expect("create file:");
    vfs.fallocate(entry.inode, fh, Falloc::NONE, 0, 64 << 10)
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

    let buf = vfs
        .read(entry.inode, fh, 0, 128 << 10)
        .await
        .expect("read file err");
    assert_eq!(buf.len(), 128 << 10); // head write
    assert_eq!(&(buf.slice(0..5))[..], b"hello");

    let buf = vfs
        .read(entry.inode, fh, 10 << 20, 6)
        .await
        .expect("read file end");
    assert_eq!(buf.len(), 6); // copyed range
    assert_eq!(&buf[..], b"hello\x00");

    let buf = vfs
        .read(entry.inode, fh, 100 << 20, 128 << 10)
        .await
        .expect("read file end");
    assert_eq!(buf.len(), 2); // truncated
    assert_eq!(&buf[..], b"wo");
    vfs.flush(entry.inode, fh, 0).await.expect("flush file:");

    // edge cases
    let (fh2, _) = vfs
        .open(entry.inode, OFlag::O_RDONLY)
        .await
        .expect("open file:");
    let (fh3, _) = vfs
        .open(entry.inode, OFlag::O_WRONLY)
        .await
        .expect("open file:");
    let w_handle = vfs.find_handle(entry.inode, fh3);
    if w_handle.is_none() {
        panic!("failed to find O_WRONLY handle");
    }
    // read
    assert_eq!(
        vfs.read(entry.inode, 0, 0, 10).await.err(),
        Some(Errno::EBADF),
        "should 'invalid fh'"
    );
    assert_eq!(
        vfs.read(entry.inode, fh2, 1 << 60, 10).await.err(),
        Some(Errno::EFBIG),
        "should 'overflow file len'"
    );
    // write
    assert_eq!(
        vfs.write(entry.inode, 0, 0, Bytes::new()).await.err(),
        Some(Errno::EBADF),
        "should 'invalid fh'"
    );
    assert_eq!(
        vfs.write(entry.inode, fh2, 1 << 60, Bytes::new())
            .await
            .err(),
        Some(Errno::EFBIG),
        "should 'overflow file len'"
    );
    assert_eq!(
        vfs.write(entry.inode, fh2, 0, Bytes::new()).await.err(),
        Some(Errno::EBADF),
        "should 'writes read-only fd'"
    );
    // truncate
    assert_eq!(
        vfs.truncate(entry.inode, fh2, 1 << 60).await.err(),
        Some(Errno::EFBIG),
        "should 'truncate overflow file len'"
    );
    // fallocate
    assert_eq!(
        vfs.fallocate(STATS_INODE, fh, Falloc::NONE, 0, 1)
            .await
            .err(),
        Some(Errno::EPERM),
        "should 'fallocate invalid off'"
    );
    assert_eq!(
        vfs.fallocate(entry.inode, 0, Falloc::NONE, 0, 100)
            .await
            .err(),
        Some(Errno::EBADF),
        "should 'fallocate invalid fn'"
    );
    assert_eq!(
        vfs.fallocate(entry.inode, fh, Falloc::NONE, 1 << 60, 1 << 60)
            .await
            .err(),
        Some(Errno::EFBIG),
        "should 'fallocate overflow file len'"
    );
    assert_eq!(
        vfs.fallocate(entry.inode, fh2, Falloc::NONE, 1 << 10, 1 << 20)
            .await
            .err(),
        Some(Errno::EBADF),
        "should 'fallocate read-only fd'"
    );
    // copy file range 
    assert_eq!(
        vfs.copy_file_range(STATS_INODE, fh, 0, entry.inode, fh, 10 << 20, 10, 0)
            .await
            .err(),
        Some(Errno::ENOTSUP),
        "should 'copy file range from internal file is illigal'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, fh, 0, STATS_INODE, fh, 10 << 20, 10, 0)
            .await
            .err(),
        Some(Errno::EPERM),
        "should 'copy file range to internal file is illigal'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, 0, 0, entry.inode, fh, 10 << 20, 10, 0)
            .await
            .err(),
        Some(Errno::EBADF),
        "should 'copy file range from invalid fh'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, fh, 0, entry.inode, 0, 10 << 20, 10, 0)
            .await
            .err(),
        Some(Errno::EBADF),
        "should 'copy file range to invalid fh'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, fh, 0, entry.inode, fh, 10 << 20, 10, 1)
            .await
            .err(),
        Some(Errno::EINVAL),
        "should 'copy file range invalid flag'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, fh, 0, entry.inode, fh, 1 << 20, 1 << 50, 0)
            .await
            .err(),
        Some(Errno::EINVAL),
        "should 'copy file range overlap'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, fh, 0, entry.inode, fh, 1 << 63, 1 << 63, 0)
            .await
            .err(),
        Some(Errno::EFBIG),
        "should 'copy file range too big file'"
    );
    assert_eq!(
        vfs.copy_file_range(entry.inode, fh, 0, entry.inode, fh2, 1 << 20, 1 << 10, 0)
            .await
            .err(),
        Some(Errno::EBADF),
        "should 'copy file range to read-only fd'"
    );
    info!("2");
    // sequntial write/read
    for i in 0..1001 {
        if vfs
            .write(entry.inode, fh, i * (128 << 10), Bytes::from(vec![1; 128 << 10]))
            .await
            .is_err()
        {
            panic!("write big file");
        }
    }
    for i in 0..1000 {
        let buf = vfs
            .read(entry.inode, fh, i * (128 << 10), 128 << 10)
            .await.expect("read big file");
        for j in 0..(128 << 10) {
            if buf[j] != 1 {
                panic!("read big file: {} {}", j, buf[j]);
            }
        }
    }
    info!("3");
    
    // many small write
    for i in (0..=31).rev() {
        if vfs
            .write(entry.inode, fh, i * (4 << 10), Bytes::from(vec![2; 5 << 10]))
            .await
            .is_err()
        {
            panic!("write big file");
        }
    }
    sleep(Duration::from_millis(1500)).await; // wait for it to be flushed
    let buf = vfs
        .read(entry.inode, fh, 0, 128 << 10)
        .await
        .expect("read big file");
    assert_eq!(buf.len(), 128 << 10);
    for j in 0..(128 << 10) {
        if buf[j] != 2 {
            panic!("read big file: {} {}", j, buf[j]);
        }
    }
    info!("4");
    vfs.release(entry.inode, fh).await.expect("release file:");
}

async fn test_vfs_xattr() {}

async fn test_vfs_access() {}

async fn test_vfs_attr() {}

async fn test_vfs_locks() {}

async fn test_vfs_internal_files() {}

async fn test_vfs_readdir_cache() {}
