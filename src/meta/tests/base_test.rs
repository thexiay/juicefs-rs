use juice_meta::{api::{Attr, Ino, Meta}, config::Format};

pub fn test_format() -> Format {
    Format {
        name: "test".to_string(),
        enable_dir_stats: true,
        ..Format::default()
    }
}

pub async fn test_meta_client(m: Box<dyn Meta>) {}

pub async fn test_truncate_and_delete(m: Box<dyn Meta>) {}

pub async fn test_trash(m: Box<dyn Meta>) {}

pub async fn test_parents(m: Box<dyn Meta>) {}

pub async fn test_remove(m: Box<dyn Meta>) {
    let (_, _) = m.create(1, "f".to_string(), 0644, 0, 0)
        .await
        .expect("create f: ");

    m.remove(1, "f".to_string(), &mut 0).await.expect("rmr f: ");
    let (parent, _) = m.mkdir(1, "d".to_string(), 0755, 0, 0)
        .await
        .expect("mkdir d: ");
    let (_, _) = m.mkdir(parent, "d2".to_string(), 0755, 0, 0)
        .await
        .expect("create d/d2: ");
    let (inode, attr) = m.create(parent, "f".to_string(), 0644, 0, 0)
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
        m.create(1, name.clone(), 0644, 0, 0)
            .await
            .expect(&format!("create {name}"));
    }

    let mut entries = Vec::new();
    m.readdir(1, 1, &mut entries).await.expect("readdir: ");
    if entries.len() != 4099 {
        panic!("entries: {}", entries.len());
    }
    m.remove(1, "d".to_string(), &mut 0).await.expect("rmr d: ");
}

async fn test_resolve(m: Box<dyn Meta>) {}

async fn test_sticky_bit(m: Box<dyn Meta>) {}

async fn test_locks(m: Box<dyn Meta>) {}

async fn test_list_locks(m: Box<dyn Meta>) {}

async fn test_concurrent_write(m: Box<dyn Meta>) {}

async fn test_compaction<M: Meta>(m: M, flag: bool) {}

async fn test_copy_file_range(m: Box<dyn Meta>) {}

async fn test_close_session(m: Box<dyn Meta>) {}

async fn test_concurrent_dir(m: Box<dyn Meta>) {}

async fn test_attr_flags(m: Box<dyn Meta>) {}

async fn test_quota(m: Box<dyn Meta>) {}

async fn test_atime(m: Box<dyn Meta>) {}

async fn test_access(m: Box<dyn Meta>) {}

async fn test_open_cache(m: Box<dyn Meta>) {}

async fn test_case_incensi(m: Box<dyn Meta>) {}

async fn test_check_and_repair(m: Box<dyn Meta>) {}

async fn test_dir_stat(m: Box<dyn Meta>) {}

async fn test_clone(m: Box<dyn Meta>) {}

async fn test_acl(m: Box<dyn Meta>) {}

async fn test_read_only(m: Box<dyn Meta>) {}
