use juice_meta::api::Meta;



pub async fn test_meta_client(m: Box<dyn Meta>) {}

pub async fn test_truncate_and_delete(m: Box<dyn Meta>) {}

pub async fn test_trash(m: Box<dyn Meta>) {}

pub async fn test_parents(m: Box<dyn Meta>) {}

pub async fn test_remove(m: Box<dyn Meta>) {
    
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