use std::time::Duration;

use crate::{
    acl::{Entry, Rule}, align_4k, api::{
        Attr, Falloc, INodeType, Meta, ModeMask, OFlag, QuotaOp, RenameMask, SetAttrMask, Slice,
        Summary, XattrF, CHUNK_SIZE, ROOT_INODE,
    }, base::{CommonMeta, Engine}, config::Format, quota::{MetaQuota, QuotaView}
};
use chrono::Utc;
use tokio::{
    task::JoinSet,
    time::{self, sleep},
};
use tracing::info;

pub fn test_format() -> Format {
    Format {
        name: "test".to_string(),
        enable_dir_stats: true,
        ..Format::default()
    }
}

pub async fn test_meta_client(m: &mut (impl Engine + AsRef<CommonMeta>)) {
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
    let base_sid = { m.as_ref().sid.read().clone() };
    assert!(
        base_sid.unwrap() == sessions[0].sid,
        "base sid: {:?} != registered sid {}",
        base_sid,
        sessions[0].sid
    );

    let meta = dyn_clone::clone_box(m);
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
    // ----------------------------------------- test lookup -----------------------------------------
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

    // ----------------------------------------- test resolve -----------------------------------------
    let rs = m.resolve(ROOT_INODE, "d/f", false).await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_supported(),
        "got {:?}",
        rs
    );
    let rs = m.resolve(parent, "/f", false).await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_supported(),
        "got {:?}",
        rs
    );

    // test resolve with different user
    let ctx = 0;
    let ctx2 = 1;
    m.with_login(ctx2, vec![ctx2]);
    let err = m.resolve(parent, "/f", false).await.unwrap_err();
    assert!(
        err.is_op_not_supported() || err.is_permission_denied(),
        "got {:?}",
        rs
    );
    let err = m.resolve(parent, "/f/c", false).await.unwrap_err();
    assert!(
        err.is_not_dir1(&parent, "/f/c") || err.is_op_not_supported(),
        "got {:?}",
        rs
    );
    let err = m.resolve(parent, "/f2", false).await.unwrap_err();
    assert!(
        err.is_no_entry_found(&parent, "/f2") || err.is_op_not_supported(),
        "got {:?}",
        rs
    );

    // ----------------------------------------- test access -----------------------------------------
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
    let err = m.resolve(ROOT_INODE, "/d1/d2", false).await.unwrap_err();
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
    let (inode, _) = m
        .create(ROOT_INODE, "f", 0o644, 0o22, OFlag::empty())
        .await
        .expect("create f: ");
    m.close(inode).await.expect("close f: ");
    // test rename with noreplace
    let rs = m
        .rename(ROOT_INODE, "f2", ROOT_INODE, "f", RenameMask::NOREPLACE)
        .await;
    assert!(rs.unwrap_err().is_entry_exists(&ROOT_INODE, "f"));
    // test rename with replace
    let before_stat_fs = m.stat_fs(ROOT_INODE).await.unwrap();
    let renamed_entry = m
        .rename(ROOT_INODE, "f2", ROOT_INODE, "f", RenameMask::empty())
        .await
        .expect("rename f2 -> f: ");
    assert!(renamed_entry.is_some());
    let after_stat_fs = m.stat_fs(ROOT_INODE).await.unwrap();
    assert_eq!(before_stat_fs.i_used - after_stat_fs.i_used, 1); // used inode decr 1
    assert_eq!(
        // avaiable space incr attr length
        (after_stat_fs.space_avail - before_stat_fs.space_avail) as i64,
        align_4k(renamed_entry.unwrap().1.length)
    );
    // test rename with exchange
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
    let d_parent = parent;
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

    // ----------------------------------------- test data write read -----------------------------------------
    // try to open a file that does not exist
    let err = m.open(99999, OFlag::O_RDWR).await.unwrap_err();
    assert!(err.is_no_entry_found2(&99999), "got {:?}", err);

    let _ = m.open(inode, OFlag::O_RDWR).await.expect("open f: ");
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
        Utc::now(),
    )
    .await
    .expect("write f: ");
    let slices = m.read(inode, 0).await.expect("read chunk: ");
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].id, 0);
    assert_eq!(slices[0].size, 100);
    assert_eq!(slices[1].id, slice_id);
    assert_eq!(slices[1].size, 100);

    m.fallocate(inode, Falloc::PUNCH_HOLE.union(Falloc::KEEP_SIZE), 100, 50)
        .await
        .expect("fallocate: "); // success
    let rs = m
        .fallocate(
            inode,
            Falloc::PUNCH_HOLE.union(Falloc::COLLAPES_RANGE),
            100,
            50,
        )
        .await;
    assert!(rs.as_ref().unwrap_err().is_invalid_arg(), "got {:?}", rs);
    let rs = m
        .fallocate(
            inode,
            Falloc::PUNCH_HOLE.union(Falloc::INSERT_RANGE),
            100,
            50,
        )
        .await;
    assert!(rs.as_ref().unwrap_err().is_invalid_arg(), "got {:?}", rs);
    let rs = m.fallocate(inode, Falloc::COLLAPES_RANGE, 100, 50).await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_supported(),
        "got {:?}",
        rs
    );
    let rs = m.fallocate(inode, Falloc::PUNCH_HOLE, 100, 50).await;
    assert!(rs.as_ref().unwrap_err().is_invalid_arg(), "got {:?}", rs);
    let rs = m
        .fallocate(inode, Falloc::PUNCH_HOLE.union(Falloc::KEEP_SIZE), 0, 0)
        .await;
    assert!(rs.as_ref().unwrap_err().is_invalid_arg(), "got {:?}", rs);
    let _ = m.open(parent, OFlag::O_RDWR).await.expect("open f: ");
    let rs = m
        .fallocate(parent, Falloc::PUNCH_HOLE.union(Falloc::KEEP_SIZE), 100, 50)
        .await;
    assert!(
        rs.as_ref().unwrap_err().is_op_not_permitted(),
        "got {:?}",
        rs
    );
    m.close(parent).await.expect("close f: ");

    let slices = m.read(inode, 0).await.expect("read chunk: ");
    assert_eq!(slices.len(), 3);
    assert_eq!(slices[1].id, 0);
    assert_eq!(slices[1].len, 50);
    assert_eq!(slices[2].id, slice_id);
    assert_eq!(slices[2].len, 50);
    m.close(inode).await.expect("close f: ");

    // xattr
    m.set_xattr(
        inode,
        "a",
        "v".as_bytes().to_owned(),
        XattrF::CREATE_OR_REPLACE,
    )
    .await
    .expect("setxattr: ");
    m.set_xattr(
        inode,
        "a",
        "v2".as_bytes().to_owned(),
        XattrF::CREATE_OR_REPLACE,
    )
    .await
    .expect("setxattr: ");
    let value = m.get_xattr(inode, "a").await.expect("getxattr: ");
    assert_eq!(value, "v2".as_bytes());
    let value = m.list_xattr(inode).await.expect("listxattr: ");
    assert_eq!(value, "a\0".as_bytes());
    m.unlink(ROOT_INODE, "F3", false)
        .await
        .expect("unlink F3: ");
    let value = m.get_xattr(inode, "a").await.expect("getxattr: ");
    assert_eq!(value, "v2".as_bytes());
    m.remove_xattr(inode, "a").await.expect("removexattr: ");
    let rs = m
        .set_xattr(inode, "a", "v".as_bytes().to_owned(), XattrF::REPLACE)
        .await;
    assert!(rs.as_ref().unwrap_err().is_no_such_attr(), "got {:?}", rs);
    m.set_xattr(inode, "a", "v3".as_bytes().to_owned(), XattrF::CREATE)
        .await
        .expect("setxattr: ");
    let rs = m
        .set_xattr(inode, "a", "v3".as_bytes().to_owned(), XattrF::CREATE)
        .await;
    assert!(
        rs.as_ref().unwrap_err().is_entry_exists2(&inode),
        "got {:?}",
        rs
    );
    m.set_xattr(inode, "a", "v3".as_bytes().to_owned(), XattrF::REPLACE)
        .await
        .expect("setxattr: ");
    m.set_xattr(inode, "a", "v4".as_bytes().to_owned(), XattrF::REPLACE)
        .await
        .expect("setxattr: ");

    // ----------------------------------------- test quota -----------------------------------------
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 50);
    assert_eq!(stat_fs.i_avail, 10 << 20);
    let mut new_format = format.as_ref().clone();
    new_format.capacity = 1 << 20;
    new_format.inodes = 100;
    m.init(new_format, false).await.expect("set quota failed");
    // test async flush quota task
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    if stat_fs.space_total != 1 << 20 || stat_fs.i_avail != 97 {
        // only tree inodes are used
        time::sleep(Duration::from_millis(100)).await;
        let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
        assert_eq!(stat_fs.space_total, 1 << 20);
        assert_eq!(stat_fs.i_avail, 97);
    }
    // test StatFS with subdir and quota
    let (sub_ino, sub_attr) = m
        .mkdir(ROOT_INODE, "subdir", 0o755, 0, 0)
        .await
        .expect("mkdir subdir: ");
    info!("sub_no {sub_ino}");
    m.chroot("subdir").await.expect("chroot: ");
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 20);
    assert_eq!(stat_fs.i_avail, 96); // used 4 inodes

    m.handle_quota(QuotaOp::Set(QuotaView::default()), ".", false, false)
        .await
        .expect("set quota: ");
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, (1 << 20) - 4 * align_4k(0) as u64);
    assert_eq!(stat_fs.i_avail, 96);

    m.handle_quota(
        QuotaOp::Set(QuotaView {
            max_space: 1 << 10,
            max_inodes: 0,
            ..Default::default()
        }),
        ".",
        false,
        false,
    )
    .await
    .expect("set quota: ");
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 10);
    assert_eq!(stat_fs.i_avail, 96);

    m.handle_quota(
        QuotaOp::Set(QuotaView {
            max_space: 0,
            max_inodes: 10,
            ..Default::default()
        }),
        ".",
        false,
        false,
    )
    .await
    .expect("set quota: ");
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(
        stat_fs.space_total,
        (1 << 20) as u64 - 4 * align_4k(0) as u64
    );
    assert_eq!(stat_fs.i_avail, 10);

    m.handle_quota(
        QuotaOp::Set(QuotaView {
            max_space: 1 << 10,
            max_inodes: 10,
            ..Default::default()
        }),
        ".",
        false,
        false,
    )
    .await
    .expect("set quota: ");
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 10);
    assert_eq!(stat_fs.i_avail, 10);

    m.get_base().chroot(ROOT_INODE);
    let stat_fs = m.stat_fs(ROOT_INODE).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 20);
    assert_eq!(stat_fs.i_avail, 96);
    // statfs subdir directly
    let stat_fs = m.stat_fs(sub_ino).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 10);
    assert_eq!(stat_fs.i_avail, 10);

    // mock flush quota cache into persist layer
    m.load_quotas().await;
    {
        let mut dir_quotas = m.as_ref().dir_quotas.write();
        dir_quotas
            .entry(sub_ino)
            .and_modify(|quota| quota.update(4 << 10, 15)); // test used space > total space
    }
    m.flush_quotas().await;
    let stat_fs = m.stat_fs(sub_ino).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 4 << 10);
    assert_eq!(stat_fs.space_avail, 0);
    assert_eq!(stat_fs.i_used, 15);
    assert_eq!(stat_fs.i_avail, 0);
    {
        let mut dir_quotas = m.as_ref().dir_quotas.write();
        dir_quotas
            .entry(sub_ino)
            .and_modify(|quota| quota.update(-8 << 10, -20)); // invalid used space
    }
    m.flush_quotas().await;
    let stat_fs = m.stat_fs(sub_ino).await.expect("statfs: ");
    assert_eq!(stat_fs.space_total, 1 << 10);
    assert_eq!(stat_fs.space_avail, 1 << 10);
    assert_eq!(stat_fs.i_used, 0);
    assert_eq!(stat_fs.i_avail, 10);

    m.rmdir(ROOT_INODE, "subdir", false)
        .await
        .expect("rmdir subdir: ");
    let summary = m
        .get_summary(d_parent, false, true)
        .await
        .expect("get summary: ");
    assert_eq!(
        summary,
        Summary {
            length: 0,
            size: 4096,
            files: 0,
            dirs: 1
        }
    );
    let symmary = m
        .get_summary(ROOT_INODE, true, true)
        .await
        .expect("get summary: ");
    assert_eq!(
        symmary,
        Summary {
            length: 400,
            size: 20480,
            files: 3,
            dirs: 2
        }
    );
    let summary = m
        .get_summary(inode, true, true)
        .await
        .expect("get summary: ");
    assert_eq!(
        summary,
        Summary {
            length: 200,
            size: 4096,
            files: 1,
            dirs: 0
        }
    );
    m.unlink(ROOT_INODE, "f", false).await.expect("unlink f3: ");
    m.unlink(ROOT_INODE, "f3", false)
        .await
        .expect("unlink f3: ");

    sleep(Duration::from_millis(100)).await; // wait for delete

    // 这里unlink后ROOT/f理论上不应该存在这个了，但是读取到缓存数据了，从而导致错误的信息
    let rs = m.read(inode, 0).await;
    assert!(
        rs.as_ref().unwrap_err().is_no_entry_found2(&inode),
        "got {:?}",
        rs
    );

    // release resource
    m.rmdir(ROOT_INODE, "d", false).await.expect("rmdir d: ");
}

pub async fn test_truncate_and_delete(m: &mut (impl Engine + AsRef<CommonMeta>)) {
    let mut format = test_format();
    format.capacity = 0;
    m.init(format, false).await.unwrap();

    let _ = m.open(ROOT_INODE, OFlag::O_RDONLY).await.expect("open /: ");
    let res = m.truncate(ROOT_INODE, 0, 4 << 10, false).await;
    m.close(ROOT_INODE).await.expect("close /: ");
    assert!(
        res.as_ref().unwrap_err().is_op_not_permitted(),
        "got {:?}",
        res
    );
    let (inode, _) = m
        .create(ROOT_INODE, "f", 0650, 022, OFlag::empty())
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
        Utc::now(),
    )
    .await
    .expect("write file: ");
    m.truncate(inode, 0, 200 << 20, false)
        .await
        .expect("truncate file: ");
    m.truncate(inode, 0, (10 << 40) + 10, false)
        .await
        .expect("truncate file: ");
    let _attr = m
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
}

pub async fn test_trash(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

pub async fn test_parents(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

pub async fn test_remove(m: &mut (impl Engine + AsRef<CommonMeta>)) {
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
    let (inode, _) = m
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

async fn test_resolve(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_sticky_bit(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_locks(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_list_locks(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

pub async fn test_concurrent_write(m: &mut (impl Engine + AsRef<CommonMeta> + Clone)) {
    let (inode, _) = m
        .create(ROOT_INODE, "f", 0o644, 0o22, OFlag::empty())
        .await
        .expect("create file");
    let mut task_set = JoinSet::new();
    for indx in 0..=10 {
        let m = m.clone();
        let _ = task_set.spawn(async move {
            for _ in 0..100 {
                let slice_id = m.new_slice().await.expect("new slice");
                m.write(
                    inode,
                    indx,
                    0,
                    Slice {
                        id: slice_id,
                        size: 100,
                        off: 0,
                        len: 100,
                    },
                    Utc::now(),
                )
                .await
                .expect("write file");
            }
        });
    }
    while let Some(res) = task_set.join_next().await {
        res.expect("write file");
    }

    for indx in 0..=10 {
        let m = m.clone();
        let _ = task_set.spawn(async move {
            for j in 0..1000 {
                let slice_id = m.new_slice().await.expect("new slice");
                m.write(
                    inode,
                    indx,
                    200 * j,
                    Slice {
                        id: slice_id,
                        size: 100,
                        off: 0,
                        len: 100,
                    },
                    Utc::now(),
                )
                .await
                .expect("write file");
            }
        });
    }
    while let Some(res) = task_set.join_next().await {
        res.expect("write file");
    }
}

async fn test_compaction<M: Meta>(m: M, flag: bool) {}

pub async fn test_copy_file_range(m: &mut (impl Engine + AsRef<CommonMeta>)) {
    let (src_ino, _) = m
        .create(ROOT_INODE, "fin", 0o644, 0o22, OFlag::empty())
        .await
        .expect("create file");
    let (dst_ino, _) = m
        .create(ROOT_INODE, "fout", 0o644, 0o22, OFlag::empty())
        .await
        .expect("create file");
    m.write(
        src_ino,
        0,
        100,
        Slice {
            id: 10,
            size: 200,
            off: 0,
            len: 100,
        },
        Utc::now(),
    )
    .await
    .expect("write file");
    m.write(
        src_ino,
        1,
        100 << 10,
        Slice {
            id: 11,
            size: 40 << 20,
            off: 0,
            len: 40 << 20,
        },
        Utc::now(),
    )
    .await
    .expect("write file");
    m.write(
        src_ino,
        3,
        0,
        Slice {
            id: 12,
            size: 63 << 20,
            off: 10 << 20,
            len: 30 << 20,
        },
        Utc::now(),
    )
    .await
    .expect("write file");
    m.write(
        dst_ino,
        2,
        10 << 20,
        Slice {
            id: 13,
            size: 50 << 20,
            off: 10 << 20,
            len: 30 << 20,
        },
        Utc::now(),
    )
    .await
    .expect("write file");
    // cross three chunks
    let (copied, _) = m
        .copy_file_range(src_ino, 150, dst_ino, 30 << 20, 200 << 20, 0)
        .await
        .expect("copy file range")
        .expect("copy happend");
    assert_eq!(copied, 200 << 20);

    let expected_slices = vec![
        vec![
            (0, 30 << 20, 0, 30 << 20).into(),
            (10, 200, 50, 50).into(),
            (0, 0, 200, CHUNK_SIZE as u32 - (30 << 20) - 50).into(),
        ],
        vec![
            (
                0,
                0,
                150 + (CHUNK_SIZE as u32 - (30 << 20)),
                (30 << 20) - 150,
            )
                .into(),
            (0, 0, 0, 100 << 10).into(),
            (11, 40 << 20, 0, (34 << 20) + 150 - (100 << 10)).into(),
        ],
        vec![
            (
                11,
                40 << 20,
                (34 << 20) + 150 - (100 << 10),
                (6 << 20) - 150 + (100 << 10),
            )
                .into(),
            (
                0,
                0,
                (40 << 20) + (100 << 10),
                CHUNK_SIZE as u32 - (40 << 20) - (100 << 10),
            )
                .into(),
            (0, 0, 0, 150 + (CHUNK_SIZE as u32 - (30 << 20))).into(),
        ],
        vec![
            (
                0,
                0,
                150 + (CHUNK_SIZE as u32 - (30 << 20)),
                (30 << 20) - 150,
            )
                .into(),
            (12, 63 << 20, 10 << 20, (8 << 20) + 150).into(),
        ],
    ];
    for i in 0..4_usize {
        let slices = m.read(dst_ino, i as u32).await.expect("read chunk");
        println!("slices: {:?}", slices);
        assert_eq!(
            slices.len(),
            expected_slices[i].len(),
            "expect slices len not equal in chunk {i}"
        );
        for j in 0..slices.len() {
            assert_eq!(
                slices[j], expected_slices[i][j],
                "expect slice equal in chunk {i} slice {j}"
            );
        }
    }
}

pub async fn test_close_session(m: &mut (impl Engine + AsRef<CommonMeta> + Clone)) {}

pub async fn test_concurrent_dir(m: &mut (impl Engine + AsRef<CommonMeta> + Clone)) {
    let mut task_set = JoinSet::new();
    for i in 0..100 {
        let m = m.clone();
        let _ = task_set.spawn(async move {
            let (d1, _) = match m.mkdir(ROOT_INODE, "d1", 0o640, 0o22, 0).await {
                Ok(res) => res,
                Err(e) => {
                    assert!(e.is_entry_exists(&ROOT_INODE, "d1"), "{e}");
                    m.lookup(ROOT_INODE, "d1", true).await.expect("lookup d1")
                }
            };

            let (d2, _) = match m.mkdir(ROOT_INODE, "d2", 0o640, 0o22, 0).await {
                Ok(res) => res,
                Err(e) => {
                    assert!(e.is_entry_exists(&ROOT_INODE, "d2"), "e");
                    m.lookup(ROOT_INODE, "d2", true).await.expect("lookup d2")
                }
            };
            let name = format!("file{i}");
            m.create(d1, &name, 0o644, 0, OFlag::empty())
                .await
                .expect(&format!("create d1/{name}"));
            m.rename(d1, &name, d2, &name, RenameMask::empty())
                .await
                .expect(&format!("rename d1/{name} -> d2/{name}"));
        });
    }
    while let Some(res) = task_set.join_next().await {
        res.expect("concurrent dir");
    }

    for i in 0..100 {
        let m = m.clone();
        let _ = task_set.spawn(async move {
            let (d2, _) = m.lookup(ROOT_INODE, "d2", true).await.expect("lookup d2");
            let name = format!("file{i}");
            m.unlink(d2, &name, false)
                .await
                .expect(&format!("unlink d2/{name}"));
            if let Err(e) = m.rmdir(ROOT_INODE, "d1", false).await {
                assert!(
                    e.is_dir_not_empty(&ROOT_INODE, "d1") || e.is_no_entry_found(&ROOT_INODE, "d1")
                );
            }
            if let Err(e) = m.rmdir(ROOT_INODE, "d2", false).await {
                assert!(
                    e.is_dir_not_empty(&ROOT_INODE, "d2") || e.is_no_entry_found(&ROOT_INODE, "d2")
                );
            }
        });
    }
    while let Some(res) = task_set.join_next().await {
        res.expect("concurrent dir");
    }
}

async fn test_attr_flags(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_quota(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_atime(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_access(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_open_cache(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_case_incensi(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_check_and_repair(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_dir_stat(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

async fn test_clone(m: &mut (impl Engine + AsRef<CommonMeta>)) {}

pub async fn test_acl(m: &mut (impl Engine + AsRef<CommonMeta>)) {
    let mut format = test_format();
    format.enable_acl = true;
    m.init(format, false).await.unwrap();

    let test_dir = "test_dir";
    let (test_dir_ino, _) = m
        .mkdir(ROOT_INODE, test_dir, 0o644, 0, 0)
        .await
        .expect("mkdir test_dir");
    let rule = Rule {
        owner: 7,
        group: 7,
        mask: 7,
        other: 7,
        named_users: vec![Entry {
            id: 1001,
            perm: 4,
        }],
        named_groups: vec![],
    };

    // case: setfacl
    /*
    // case: setfacl
    if st := m.SetFacl(ctx, testDirIno, aclAPI.TypeAccess, rule); st != 0 {
        t.Fatalf("setfacl error: %s", st)
    }

    // case: getfacl
    rule2 := &aclAPI.Rule{}
    if st := m.GetFacl(ctx, testDirIno, aclAPI.TypeAccess, rule2); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    assert.True(t, rule.IsEqual(rule2))

    // case: setfacl will sync mode (group class is mask)
    attr2 := &Attr{}
    if st := m.GetAttr(ctx, testDirIno, attr2); st != 0 {
        t.Fatalf("getattr error: %s", st)
    }
    assert.Equal(t, uint16(0777), attr2.Mode)

    // case: setattr will sync acl
    set := uint16(0) | SetAttrMode
    attr2 = &Attr{
        Mode: 0555,
    }
    if st := m.SetAttr(ctx, testDirIno, set, 0, attr2); st != 0 {
        t.Fatalf("setattr error: %s", st)
    }

    rule3 := &aclAPI.Rule{}
    if st := m.GetFacl(ctx, testDirIno, aclAPI.TypeAccess, rule3); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    rule2.Owner = 5
    rule2.Mask = 5
    rule2.Other = 5
    assert.True(t, rule3.IsEqual(rule2))

    // case: remove acl
    rule3.Mask = 0xFFFF
    rule3.NamedUsers = nil
    rule3.NamedGroups = nil
    if st := m.SetFacl(ctx, testDirIno, aclAPI.TypeAccess, rule3); st != 0 {
        t.Fatalf("setattr error: %s", st)
    }

    st := m.GetFacl(ctx, testDirIno, aclAPI.TypeAccess, nil)
    assert.Equal(t, ENOATTR, st)

    attr2 = &Attr{}
    if st := m.GetAttr(ctx, testDirIno, attr2); st != 0 {
        t.Fatalf("getattr error: %s", st)
    }
    assert.Equal(t, uint16(0575), attr2.Mode)

    // case: set normal default acl
    if st := m.SetFacl(ctx, testDirIno, aclAPI.TypeDefault, rule); st != 0 {
        t.Fatalf("setfacl error: %s", st)
    }

    // case: get normal default acl
    rule2 = &aclAPI.Rule{}
    if st := m.GetFacl(ctx, testDirIno, aclAPI.TypeDefault, rule2); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    assert.True(t, rule2.IsEqual(rule))

    // case: mk subdir with normal default acl
    subDir := "sub_dir"
    var subDirIno Ino
    attr2 = &Attr{}

    mode := uint16(0222)
    // cumask will be ignored
    if st := m.Mkdir(ctx, testDirIno, subDir, mode, 0022, 0, &subDirIno, attr2); st != 0 {
        t.Fatalf("create %s: %s", subDir, st)
    }
    defer m.Rmdir(ctx, testDirIno, subDir)

    // subdir inherit default acl
    rule3 = &aclAPI.Rule{}
    if st := m.GetFacl(ctx, subDirIno, aclAPI.TypeDefault, rule3); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    assert.True(t, rule3.IsEqual(rule2))

    // subdir access acl
    rule3 = &aclAPI.Rule{}
    if st := m.GetFacl(ctx, subDirIno, aclAPI.TypeAccess, rule3); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    rule2.Owner &= (mode >> 6) & 7
    rule2.Mask &= (mode >> 3) & 7
    rule2.Other &= mode & 7
    assert.True(t, rule3.IsEqual(rule2))

    // case: set minimal default acl
    rule = &aclAPI.Rule{
        Owner:       5,
        Group:       5,
        Mask:        0xFFFF,
        Other:       5,
        NamedUsers:  nil,
        NamedGroups: nil,
    }
    if st := m.SetFacl(ctx, testDirIno, aclAPI.TypeDefault, rule); st != 0 {
        t.Fatalf("setfacl error: %s", st)
    }

    // case: get minimal default acl
    rule2 = &aclAPI.Rule{}
    if st := m.GetFacl(ctx, testDirIno, aclAPI.TypeDefault, rule2); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    assert.True(t, rule2.IsEqual(rule))

    // case: mk subdir with minimal default acl
    subDir2 := "sub_dir2"
    var subDirIno2 Ino
    attr2 = &Attr{}

    mode = uint16(0222)
    if st := m.Mkdir(ctx, testDirIno, subDir2, mode, 0022, 0, &subDirIno2, attr2); st != 0 {
        t.Fatalf("create %s: %s", subDir, st)
    }
    defer m.Rmdir(ctx, testDirIno, subDir2)
    assert.Equal(t, uint16(0), attr2.Mode)

    // subdir inherit default acl
    rule3 = &aclAPI.Rule{}
    if st := m.GetFacl(ctx, subDirIno2, aclAPI.TypeDefault, rule3); st != 0 {
        t.Fatalf("getfacl error: %s", st)
    }
    assert.True(t, rule3.IsEqual(rule2))

    // subdir have no access acl
    rule3 = &aclAPI.Rule{}
    st = m.GetFacl(ctx, subDirIno2, aclAPI.TypeAccess, rule3)
    assert.Equal(t, ENOATTR, st)

    // test cache all
    sz := m.getBase().aclCache.Size()
    err := m.getBase().en.cacheACLs(ctx)
    assert.Nil(t, err)
    assert.Equal(t, sz, m.getBase().aclCache.Size())

     */
}

async fn test_read_only(m: &mut (impl Engine + AsRef<CommonMeta>)) {}
