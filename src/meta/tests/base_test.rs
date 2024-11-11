#![feature(let_chains)]

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use juice_meta::{
    api::{Attr, Ino, Meta, Slice},
    config::Format,
};
use tokio::time;

pub fn test_format() -> Format {
    Format {
        name: "test".to_string(),
        enable_dir_stats: true,
        ..Format::default()
    }
}

#[cfg(test)]
pub async fn test_meta_client(m: Box<Arc<dyn Meta>>) {
    match m.get_attr(1).await {
        Ok(attr) if attr.mode != 0777 => panic!("getattr root mode err"),
        Err(err) => panic!("getattr root err {}", err),
        _ => {}
    }
    m.init(test_format(), true)
        .await
        .expect("initialize failed");
    m.init(
        Format {
            name: "test2".to_string(),
            ..Default::default()
        },
        false,
    )
    .await
    .expect_err("change name without --force is not allowed");
    let format = m
        .load(true)
        .await
        .expect("load failed after initalization: ");
    if format.name != "test" {
        panic!("load got volume name {} != test", format.name);
    }
    // begin session
    m.clone().new_session(true).await.expect("new session: ");
    let sessions = m.list_sessions().await.expect("list sessions: ");
    if sessions.len() != 1 {
        panic!("list sessions cnt should be 1");
    }
    let base = m.get_base();
    let base_sid = { base.sid.read().clone() };
    match base_sid {
        Some(sid) if sid == sessions[0].sid => (),
        _ => panic!(
            "my sid {:?} != registered sid {}",
            base_sid, sessions[0].sid
        ),
    }
    let meta = m.clone();
    tokio::spawn(async move {
        meta.cleanup_stale_sessions().await;
    });

    // test mkdir rmdir
    let (parent, attr) = m.mkdir(1, "d", 0o640, 0o22, 0).await.expect("mkdir d: ");

    /*

    var parent, inode, dummyInode Ino
    if st := m.Mkdir(ctx, 1, "d", 0640, 022, 0, &parent, attr); st != 0 {
        t.Fatalf("mkdir d: %s", st)
    }
    defer m.Rmdir(ctx, 1, "d")
    if st := m.Unlink(ctx, 1, "d"); st != syscall.EPERM {
        t.Fatalf("unlink d: %s", st)
    }
    if st := m.Rmdir(ctx, parent, "."); st != syscall.EINVAL {
        t.Fatalf("unlink d.: %s", st)
    }
    if st := m.Rmdir(ctx, parent, ".."); st != syscall.ENOTEMPTY {
        t.Fatalf("unlink d..: %s", st)
    }
    if st := m.Lookup(ctx, 1, "d", &parent, attr, true); st != 0 {
        t.Fatalf("lookup d: %s", st)
    }
    if st := m.Lookup(ctx, 1, "d", &parent, nil, true); st != syscall.EINVAL {
        t.Fatalf("lookup d: %s", st)
    }
    if st := m.Lookup(ctx, 1, "..", &inode, attr, true); st != 0 || inode != 1 {
        t.Fatalf("lookup ..: %s", st)
    }
     */
}

pub async fn test_truncate_and_delete(m: Box<Arc<dyn Meta>>) {
    let mut format = m.load(false).await.unwrap().as_ref().clone();
    format.capacity = 0;
    m.init(format, false).await.unwrap();

    m.clone().unlink(1, "f", false).await.expect("unlink f: ");
    let (inode, attr) = m
        .create(1, "f", 0650, 022, 0)
        .await
        .expect("create file: {}");
    let slice_id = m.new_slice().await.expect("new chunk: ");
    m.write(
        inode,
        0,
        100,
        Slice {
            id: slice_id,
            size: 100,
            off: 0,
            len: 100,
        },
        SystemTime::now(),
    )
    .await
    .expect("write file: ");
    m.clone()
        .truncate(inode, 0, 200 << 20, false)
        .await
        .expect("truncate file: ");
    m.clone()
        .truncate(inode, 0, (10 << 40) + 10, false)
        .await
        .expect("truncate file: ");
    let attr = m
        .clone()
        .truncate(inode, 0, (300 << 20) + 10, false)
        .await
        .expect("truncate file: ");

    let slices = m
        .list_slices(false, Box::new(|| {}))
        .await
        .expect("list slices: ");
    let total_slices = slices.values().map(|s| s.len()).sum::<usize>();
    if total_slices != 1 {
        panic!("number of slices: {} != 1, {:?}", total_slices, slices);
    }
    m.close(inode).await.unwrap();
    m.clone().unlink(1, "f", false).await.expect("unlink f: ");
    time::sleep(Duration::from_millis(100)).await;

    // unlink and list slices again
    let slices = m
        .list_slices(false, Box::new(|| {}))
        .await
        .expect("list slices: ");
    let total_slices = slices.values().map(|s| s.len()).sum::<usize>();
    // the last chunk could be found and deleted
    if total_slices > 1 {
        panic!("number of slices: {} > 0, {:?}", total_slices, slices);
    }

    m.clone().unlink(1, "f", false).await.expect("unlink f:")
}

pub async fn test_trash(m: Box<Arc<dyn Meta>>) {}

pub async fn test_parents(m: Box<Arc<dyn Meta>>) {}

pub async fn test_remove(m: Box<Arc<dyn Meta>>) {
    let (_, _) = m
        .create(1, "f", 0644, 0, 0)
        .await
        .expect("create f: ");

    m.remove(1, "f", &mut 0).await.expect("rmr f: ");
    let (parent, _) = m
        .mkdir(1, "d", 0755, 0, 0)
        .await
        .expect("mkdir d: ");
    let (_, _) = m
        .mkdir(parent, "d2", 0755, 0, 0)
        .await
        .expect("create d/d2: ");
    let (inode, attr) = m
        .create(parent, "f", 0644, 0, 0)
        .await
        .expect("create d/f: ");

    let ps = m.get_paths(parent).await;
    if ps.is_empty() || ps[0] != "/d" {
        panic!("get path /d: {:?}", ps);
    }
    let ps = m.get_paths(inode).await;
    if ps.is_empty() || ps[0] != "/d/f" {
        panic!("get path /d/f: {:?}", ps);
    }
    for i in 0..4096 {
        let name = format!("f{}", i);
        m.create(1, &name, 0644, 0, 0)
            .await
            .expect(&format!("create {name}"));
    }

    let mut entries = Vec::new();
    m.readdir(1, 1, &mut entries).await.expect("readdir: ");
    if entries.len() != 4099 {
        panic!("entries: {}", entries.len());
    }
    m.remove(1, "d", &mut 0).await.expect("rmr d: ");
}

async fn test_resolve(m: Box<Arc<dyn Meta>>) {}

async fn test_sticky_bit(m: Box<Arc<dyn Meta>>) {}

async fn test_locks(m: Box<Arc<dyn Meta>>) {}

async fn test_list_locks(m: Box<Arc<dyn Meta>>) {}

async fn test_concurrent_write(m: Box<Arc<dyn Meta>>) {}

async fn test_compaction<M: Meta>(m: M, flag: bool) {}

async fn test_copy_file_range(m: Box<Arc<dyn Meta>>) {}

async fn test_close_session(m: Box<Arc<dyn Meta>>) {}

async fn test_concurrent_dir(m: Box<Arc<dyn Meta>>) {}

async fn test_attr_flags(m: Box<Arc<dyn Meta>>) {}

async fn test_quota(m: Box<Arc<dyn Meta>>) {}

async fn test_atime(m: Box<Arc<dyn Meta>>) {}

async fn test_access(m: Box<Arc<dyn Meta>>) {}

async fn test_open_cache(m: Box<Arc<dyn Meta>>) {}

async fn test_case_incensi(m: Box<Arc<dyn Meta>>) {}

async fn test_check_and_repair(m: Box<Arc<dyn Meta>>) {}

async fn test_dir_stat(m: Box<Arc<dyn Meta>>) {}

async fn test_clone(m: Box<Arc<dyn Meta>>) {}

async fn test_acl(m: Box<Arc<dyn Meta>>) {}

async fn test_read_only(m: Box<Arc<dyn Meta>>) {}
