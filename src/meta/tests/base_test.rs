#![feature(let_chains)]

use std::time::{Duration, SystemTime};

use juice_meta::{
    api::{Attr, Ino, Meta, OFlag, Slice},
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
pub async fn test_meta_client(mut m: Box<dyn Meta>) {
    use std::time::UNIX_EPOCH;

    use juice_meta::{
        api::{INodeType, ModeMask, OFlag, RenameMask, SetAttrMask, ROOT_INODE},
        error::MetaErrorEnum,
    };
    use tracing::info;

    let attr = m.get_attr(ROOT_INODE).await.unwrap();
    assert_eq!(attr.mode, 0o777);

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
    assert!(format.name == "test", "got volume name {}", format.name);
    // test session lifetime
    m.new_session(true).await.expect("new session: ");
    let sessions = m.list_sessions().await.expect("list sessions: ");
    assert!(sessions.len() == 1, "list sessions cnt should be 1");
    let base = m.get_base();
    let base_sid = { base.sid.read().clone() };
    assert!(
        base_sid.unwrap() == sessions[0].sid,
        "base sid: {:?} != registered sid {}",
        base_sid,
        sessions[0].sid
    );

    let meta = m.clone();
    tokio::spawn(async move {
        meta.cleanup_stale_sessions().await;
    });

    // test mkdir rmdir check
    let (parent, _) = m
        .mkdir(ROOT_INODE, "d", 0o640, 0o22, 0)
        .await
        .expect("mkdir d: ");
    let rs = m.unlink(ROOT_INODE, "d", false).await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_permitted(),
        "got {:?}",
        rs
    );
    let rs = m.rmdir(parent, ".", false).await;
    assert!(rs.as_ref().unwrap_err().is_invalid_arg(), "got {:?}", rs);
    let rs = m.rmdir(parent, "..", false).await;
    assert!(
        rs.as_ref().unwrap_err().is_dir_not_empty(&parent, ".."),
        "got {:?}",
        rs
    );
    // test lookup
    let (parent, _) = m.lookup(ROOT_INODE, "d", true).await.expect("lookup d: ");
    let (inode, _) = m.lookup(ROOT_INODE, "..", true).await.expect("lookup ..: ");
    assert_eq!(inode, ROOT_INODE);
    let (inode, _) = m.lookup(parent, ".", true).await.expect("lookup ..: ");
    assert_eq!(inode, parent);
    let (inode, attr) = m.lookup(parent, "..", true).await.expect("lookup ..: ");
    assert_eq!(inode, ROOT_INODE);
    assert_eq!(attr.nlink, 3);
    m.access(parent, ModeMask::READ, &attr)
        .await
        .expect("access d: ");
    let (inode, _) = m
        .create(parent, "f", 0o650, 0o22, OFlag::empty())
        .await
        .expect("create f: ");
    let _ = m.close(inode).await;
    let (_, _) = m.lookup(inode, ".", true).await.expect("lookup /d/f: ");
    let rs = m.lookup(inode, "..", true).await;
    assert!(rs.as_ref().unwrap_err().is_not_dir2(&inode), "got {:?}", rs);
    let rs = m.rmdir(parent, "f", false).await;
    assert!(
        rs.as_ref().unwrap_err().is_not_dir1(&parent, "f"),
        "got {:?}",
        rs
    );
    let rs = m.rmdir(ROOT_INODE, "d", false).await;
    assert!(
        rs.as_ref().unwrap_err().is_dir_not_empty(&ROOT_INODE, "d"),
        "got {:?}",
        rs
    );
    let rs = m
        .mknod(inode, "df", INodeType::Directory, 0o650, 0o22, 0, "")
        .await;
    assert!(rs.as_ref().unwrap_err().is_not_dir2(&inode), "got {:?}", rs);
    let rs = m
        .mknod(parent, "f", INodeType::File, 0o650, 0o22, 0, "")
        .await;
    assert!(
        rs.as_ref().unwrap_err().is_entry_exists(&parent, "f"),
        "got {:?}",
        rs
    );
    let (_, _) = m.lookup(parent, "f", true).await.expect("lookup f: ");

    // test resolve
    let rs = m.resolve(ROOT_INODE, "d/f").await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_supported(),
        "got {:?}",
        rs
    );
    let rs = m.resolve(parent, "/f").await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_supported(),
        "got {:?}",
        rs
    );

    // test resolve with different user
    let ctx = 0;
    let ctx2 = 1;
    m.with_login(ctx2, vec![ctx2]);
    let err = m.resolve(parent, "/f").await.unwrap_err();
    assert!(
        err.is_op_not_supported() || err.is_permission_denied(),
        "got {:?}",
        rs
    );
    let err = m.resolve(parent, "/f/c").await.unwrap_err();
    assert!(
        err.is_not_dir1(&parent, "/f/c") || err.is_op_not_supported(),
        "got {:?}",
        rs
    );
    let err = m.resolve(parent, "/f2").await.unwrap_err();
    assert!(
        err.is_no_entry_found(&parent, "/f2") || err.is_op_not_supported(),
        "got {:?}",
        rs
    );

    // check owner permission
    let (p1, mut attr) = m
        .mkdir(ROOT_INODE, "d1", 0o2777, 0, 0)
        .await
        .expect("mkdir d1: ");
    m.with_login(ctx, vec![ctx]); // login user root
    attr.gid = 1;
    m.set_attr(p1, SetAttrMask::empty(), 0, &attr)
        .await
        .expect("setattr d1: ");
    assert!(attr.mode & 0o2000 != 0, "SGID is lost");
    let ctx3 = 2;
    m.with_login(ctx3, vec![ctx3]);
    let (_, attr) = m.mkdir(p1, "d2", 0o777, 0o22, 0).await.expect("mkdir d2: ");
    assert_eq!(attr.gid, ctx2);
    if cfg!(target_os = "linux") {
        m.with_login(ctx2, vec![ctx2]);
        if attr.mode & 0o2000 == 0 {
            panic!("not inherit sgid");
        }
        let (_, attr) = m
            .mknod(p1, "f1", INodeType::File, 0o2777, 0o22, 0, "")
            .await
            .expect("create f1: ");
        assert!(attr.mode & 0o2010 == 0o2010, "sgid should not be cleared");

        m.with_login(ctx3, vec![ctx3]);
        let (_, attr) = m
            .mknod(p1, "f2", INodeType::File, 0o2777, 0o22, 0, "")
            .await
            .expect("create f2: ");
        assert!(attr.mode & 0o2010 == 0o0010, "sgid should not be cleared")
    }
    m.with_login(ctx2, vec![ctx2]);
    let err = m.resolve(ROOT_INODE, "/d1/d2").await.unwrap_err();
    assert!(err.is_op_not_supported(), "got {:?}", err);
    m.with_login(ctx, vec![ctx]);
    m.remove(ROOT_INODE, "d1").await.expect("Remove d1: ");
    let attr = &Attr {
        atime: 2,
        mtime: 2,
        uid: 1,
        gid: 1,
        mode: 0o640,
        ..Default::default()
    };
    m.set_attr(
        inode,
        SetAttrMask::SET_ATIME
            .union(SetAttrMask::SET_MTIME)
            .union(SetAttrMask::SET_UID)
            .union(SetAttrMask::SET_GID)
            .union(SetAttrMask::SET_MODE),
        0,
        attr,
    )
    .await
    .expect("setattr f: ");
    m.set_attr(inode, SetAttrMask::empty(), 0, attr) // change nothing
        .await
        .expect("setattr f: ");
    let attr = m.get_attr(inode).await.expect("getattr f: ");
    assert_eq!(attr.atime, 2);
    assert_eq!(attr.mtime, 2);
    assert_eq!(attr.uid, 1);
    assert_eq!(attr.gid, 1);
    assert_eq!(attr.mode, 0o640);
    m.set_attr(
        inode,
        SetAttrMask::SET_ATIME_NOW.union(SetAttrMask::SET_MTIME_NOW),
        0,
        &attr,
    )
    .await
    .expect("setattr f: ");
    m.with_login(2, vec![2, 1]); // fake_ctx
    let err = m
        .access(parent, ModeMask::WRITE, &Attr::default())
        .await
        .unwrap_err();
    assert!(err.is_permission_denied(), "got {:?}", rs);
    m.access(inode, ModeMask::READ, &Attr::default())
        .await
        .expect("access f: ");

    // test readdir result, mut have at least 2 entries
    m.with_login(ctx, vec![ctx]);
    let entries = m.readdir(parent, false).await.expect("readdir: ");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].name, ".");
    assert_eq!(entries[1].name, "..");
    assert_eq!(entries[2].name, "f");
    // -------------------------------- test rename --------------------------------

    let res = m
        .rename(parent, "f", ROOT_INODE, "f2", RenameMask::WHITEOUT)
        .await
        .unwrap_err();
    assert!(res.is_op_not_supported(), "got {:?}", res);
    m.rename(parent, "f", ROOT_INODE, "f2", RenameMask::empty())
        .await
        .expect("rename d/f -> f2: ");
    m.rename(ROOT_INODE, "f2", ROOT_INODE, "f2", RenameMask::empty())
        .await
        .expect("rename f2 -> f2: ");
    let rs = m
        .rename(ROOT_INODE, "f2", ROOT_INODE, "f", RenameMask::EXCHANGE)
        .await;
    assert!(rs.unwrap_err().is_no_entry_found(&ROOT_INODE, "f"));
    m.create(ROOT_INODE, "f", 0o644, 0o22, OFlag::empty())
        .await
        .expect("create f: ");
    m.close(inode).await.expect("close f: ");
    let rs = m
        .rename(ROOT_INODE, "f2", ROOT_INODE, "f", RenameMask::NOREPLACE)
        .await;
    assert!(rs.unwrap_err().is_entry_exists(&ROOT_INODE, "f"));
    m.rename(ROOT_INODE, "f2", ROOT_INODE, "f", RenameMask::empty())
        .await
        .expect("rename f2 -> f: ");
    m.rename(ROOT_INODE, "f", ROOT_INODE, "d", RenameMask::EXCHANGE)
        .await
        .expect("rename f -> d: ");
    m.rename(ROOT_INODE, "d", ROOT_INODE, "f", RenameMask::empty())
        .await
        .expect("rename d -> f: ");
    let attr = m.get_attr(ROOT_INODE).await.expect("getattr root: ");
    assert_eq!(attr.nlink, 2);
    let (parent, _) = m
        .mkdir(ROOT_INODE, "d", 0o640, 0o22, 0)
        .await
        .expect("mkdir d: ");
    // Test rename with parent change
    let (parent2, _) = m
        .mkdir(ROOT_INODE, "d4", 0o777, 0, 0)
        .await
        .expect("mkdir d4: ");
    m.mkdir(parent2, "d5", 0o777, 0, 0)
        .await
        .expect("mkdir d5: ");
    let res = m
        .rename(parent2, "d5", ROOT_INODE, "d5", RenameMask::NOREPLACE)
        .await
        .expect("rename d4/d5 -> d5: ");
    assert_eq!(res.unwrap().1.parent, ROOT_INODE);
    m.mknod(parent2, "f6", INodeType::File, 0o650, 0o22, 0, "")
        .await
        .expect("create file d4/f6: ");
    let res = m
        .rename(ROOT_INODE, "d5", parent2, "f6", RenameMask::EXCHANGE)
        .await
        .expect("rename d5 <-> d4/f6: ");
    match res {
        None => panic!("after exchange d5 <-> d4/f6 expect some"),
        Some((_, attr)) => {
            if attr.parent != parent2 {
                panic!(
                    "after exchange d5 <-> d4/f6 parent {} expect {}",
                    attr.parent, parent2
                );
            } else if attr.typ != INodeType::Directory {
                panic!(
                    "after exchange d5 <-> d4/f6 type {:?} expect {:?}",
                    attr.typ,
                    INodeType::Directory
                );
            }
        }
    }
    let (_, attr) = m.lookup(ROOT_INODE, "d5", true).await.expect("lookip d5: ");
    if attr.parent != ROOT_INODE {
        panic!(
            "lookup d5 after exchange parent {} expect {}",
            attr.parent, ROOT_INODE
        );
    } else if attr.typ != INodeType::File {
        panic!(
            "lookup d5 after exchange type {:?} expect {:?}",
            attr.typ,
            INodeType::Directory
        );
    }
    m.rmdir(parent2, "f6", false).await.expect("rmdir d4/f6: ");
    m.rmdir(ROOT_INODE, "d4", false).await.expect("rmdir d4: ");
    m.unlink(ROOT_INODE, "d5", false)
        .await
        .expect("unlink d5: ");
    // test hardlink
    let (inode, _) = m.lookup(ROOT_INODE, "f", true).await.expect("lookup f: ");
    m.link(inode, ROOT_INODE, "f3")
        .await
        .expect("link f3 -> f: ");
    m.link(inode, ROOT_INODE, "F3")
        .await
        .expect("link F3 -> f: ");
    let err = m.link(parent, ROOT_INODE, "d2").await.unwrap_err();
    assert!(err.is_op_not_permitted(), "got {:?}", err);
    // test softlink
    let (inode, attr) = m
        .symlink(ROOT_INODE, "s", "/f")
        .await
        .expect("symlink s -> /f: ");
    assert!(attr.mode & 0o777 == 0o777);
    let target1 = m.read_symlink(inode).await.expect("readlink s: ");
    let target2 = m.read_symlink(inode).await.expect("readlink s: "); // cached
    assert!(target1 == target2 && target1.as_bytes() == "/f".as_bytes());
    let err = m.read_symlink(parent).await.unwrap_err();
    assert!(err.is_invalid_arg(), "got {:?}", err);
    let (inode, _) = m.lookup(ROOT_INODE, "f", true).await.expect("lookup f: ");

    // data test
    // try to open a file that does not exist
    let err = m.open(99999, OFlag::O_RDWR).await.unwrap_err();
    assert!(err.is_no_entry_found2(&99999), "got {:?}", err);

    let _ = m.open(inode, OFlag::O_RDWR).await.expect("open f: ");
    m.close(inode).await.expect("close f: ");
    let slice_id = m.new_slice().await.expect("new slice: ");
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
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards"),
    )
    .await
    .expect("write f: ");
}

pub async fn test_truncate_and_delete(mut m: Box<dyn Meta>) {
    let mut format = m.load(false).await.unwrap().as_ref().clone();
    format.capacity = 0;
    m.init(format, false).await.unwrap();

    m.unlink(1, "f", false).await.expect("unlink f: ");
    let (inode, attr) = m
        .create(1, "f", 0650, 022, OFlag::empty())
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
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards"),
    )
    .await
    .expect("write file: ");
    m.truncate(inode, 0, 200 << 20, false)
        .await
        .expect("truncate file: ");
    m.truncate(inode, 0, (10 << 40) + 10, false)
        .await
        .expect("truncate file: ");
    let attr = m
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
    m.unlink(1, "f", false).await.expect("unlink f: ");
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

    m.unlink(1, "f", false).await.expect("unlink f:")
}

pub async fn test_trash(mut m: Box<dyn Meta>) {}

pub async fn test_parents(mut m: Box<dyn Meta>) {}

pub async fn test_remove(mut m: Box<dyn Meta>) {
    let (_, _) = m
        .create(1, "f", 0644, 0, OFlag::empty())
        .await
        .expect("create f: ");

    let _ = m.remove(1, "f").await.expect("rmr f: ");
    let (parent, _) = m.mkdir(1, "d", 0755, 0, 0).await.expect("mkdir d: ");
    let (_, _) = m
        .mkdir(parent, "d2", 0755, 0, 0)
        .await
        .expect("create d/d2: ");
    let (inode, attr) = m
        .create(parent, "f", 0644, 0, OFlag::empty())
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
        m.create(1, &name, 0644, 0, OFlag::empty())
            .await
            .expect(&format!("create {name}"));
    }

    let entries = m.readdir(1, true).await.expect("readdir: ");
    if entries.len() != 4099 {
        panic!("entries: {}", entries.len());
    }
    let _ = m.remove(1, "d").await.expect("rmr d: ");
}

async fn test_resolve(mut m: Box<dyn Meta>) {}

async fn test_sticky_bit(mut m: Box<dyn Meta>) {}

async fn test_locks(mut m: Box<dyn Meta>) {}

async fn test_list_locks(mut m: Box<dyn Meta>) {}

async fn test_concurrent_write(mut m: Box<dyn Meta>) {}

async fn test_compaction<M: Meta>(m: M, flag: bool) {}

async fn test_copy_file_range(mut m: Box<dyn Meta>) {}

async fn test_close_session(mut m: Box<dyn Meta>) {}

async fn test_concurrent_dir(mut m: Box<dyn Meta>) {}

async fn test_attr_flags(mut m: Box<dyn Meta>) {}

async fn test_quota(mut m: Box<dyn Meta>) {}

async fn test_atime(mut m: Box<dyn Meta>) {}

async fn test_access(mut m: Box<dyn Meta>) {}

async fn test_open_cache(mut m: Box<dyn Meta>) {}

async fn test_case_incensi(mut m: Box<dyn Meta>) {}

async fn test_check_and_repair(mut m: Box<dyn Meta>) {}

async fn test_dir_stat(mut m: Box<dyn Meta>) {}

async fn test_clone(mut m: Box<dyn Meta>) {}

async fn test_acl(mut m: Box<dyn Meta>) {}

async fn test_read_only(mut m: Box<dyn Meta>) {}
