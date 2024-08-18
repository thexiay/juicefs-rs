#[derive(Clone)]
pub struct Format {
    pub name: String,
    pub uuid: String,
    pub storage: String,
    pub storage_class: Option<String>,
    pub bucket: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub block_size: i32,
    pub compression: Option<String>,
    pub shards: Option<i32>,
    pub hash_prefix: Option<bool>,
    pub capacity: Option<u64>,
    pub inodes: Option<u64>,
    pub encrypt_key: Option<String>,
    pub encrypt_algo: Option<String>,
    pub key_encrypted: Option<bool>,
    pub upload_limit: Option<i64>, // Mbps
    pub download_limit: Option<i64>, // Mbps
    pub trash_days: i32,
    pub meta_version: Option<i32>,
    pub min_client_version: Option<String>,
    pub max_client_version: Option<String>,
    pub dir_stats: Option<bool>,
    pub enable_acl: bool,
}