use std::{io::Read, path::PathBuf, sync::LazyLock};

use clap::Parser;
use juice_meta::{
    api::{MAX_VERSION, new_client},
    config::Format,
};
use juice_storage::api::new_operator;
use juice_utils::common::{
    meta::{Shards, StorageType},
    storage::{CompressArgo, EncryptAlgo},
};
use opendal::Operator;
use regex::Regex;
use snafu::{ResultExt, whatever};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{CliOpts, Result};

const MIN_CLIENT_VERSION: &str = "1.1.0-A";
const MAX_CLIENT_VERSION: &str = "1.2.0-A";

const STROAGE_IGNORED_ITEM: &str = "testing";
const STROAGE_IGNORED_PREFIX: &str = "testing/";
const STROAGE_TEST_ITEM: &str = "juicefs_uuid";

#[derive(Parser, Debug)]
pub struct FormatOpts {
    #[arg(name = "meta-url")]
    meta_url: String,
    #[arg(name = "name", value_parser = validate_name)]
    name: String,
    /// overwrite existing format
    #[arg(long = "force", default_value_t = false)]
    force: bool,
    /// don't update existing volume
    #[arg(long = "np-update", default_value_t = false)]
    no_update: bool,
    #[command(flatten)]
    data_opts: DataStorageOpts,
    #[command(flatten)]
    data_format_opts: DataFormatOpts,
    #[command(flatten)]
    management_opts: ManagementOpts,
}

#[derive(Parser, Debug)]
struct DataStorageOpts {
    /// Object storage type (e.g. s3, gs, oss, cos) (default: file, refer to documentation for all supported object
    ///  storage types)
    #[arg(long = "storage")]
    storage: StorageType,
    /// Specifies the Endpoint to access the object store for the current mount point.
    #[arg(long = "bucket")]
    bucket: String,
    /// Specifies the Endpoint to access the object store for the current mount point.
    #[arg(long = "endpoint")]
    endpoint: String,
    /// Access Key for object storage (can also be set via the environment variable ACCESS_KEY),
    /// see How to Set Up Object Storage for more.
    #[arg(long = "access-key", env = "ACCESS_KEY", default_value = None)]
    access_key: Option<String>,
    /// Secret Key for object storage (can also be set via the environment variable SECRET_KEY),
    /// see How to Set Up Object Storage for more.
    #[arg(long = "secret-key", env = "SECRET_KEY", default_value = None)]
    secret_key: Option<String>,
    /// session token for object storage, see How to Set Up Object Storage for more.
    #[arg(long = "session-token", env = "SESSION_TOKEN", default_value = None)]
    session_token: Option<String>,
    /// the default storage class
    #[arg(long = "storage-class", default_value = None)]
    storage_class: Option<String>,
}

#[derive(Parser, Debug)]
struct DataFormatOpts {
    /// size of block in KiB (default: 4M). 4M is usually a better default value because many object storage
    /// services use 4M as their internal block size, thus using the same block size in JuiceFS usually yields better
    /// performance.
    #[arg(long = "block-size", default_value_t = 4 * 1024)]
    block_size_kb: u32,

    /// compression algorithm, choose from lz4, zstd, none (default). Enabling compression will inevitably affect
    /// performance. Among the two supported algorithms, lz4 offers a better performance, while zstd comes with a higher
    /// compression ratio, Google for their detailed comparison.
    #[arg(long = "compress")]
    compress_algo: Option<CompressArgo>,

    /// A path to RSA private key (PEM)
    #[arg(long = "encrypt-rsa-key", default_value = None)]
    encrypt_rsa_key_path: Option<PathBuf>,

    #[arg(long = "encrypt-algo")]
    encrypt_algo: Option<EncryptAlgo>,

    /// hash-prefixFor most object storages, if object storage blocks are sequentially named, they will also be closely
    /// stored in the underlying physical regions. When loaded with intensive concurrent consecutive reads, this can
    /// cause hotspots and hinder object storage performance.
    ///
    /// Enabling --hash-prefix will add a hash prefix to name of the blocks (slice ID mod 256, see internal
    /// implementation), this distributes data blocks evenly across actual object storage regions, offering more
    /// consistent performance. Obviously, this option dictates object naming pattern and should be specified when a
    /// file system is created, and cannot be changed on-the-fly.
    ///
    /// Currently, AWS S3 had already made improvements and no longer require application side optimization, but for
    /// other types of object storages, this option still recommended for large scale scenarios.
    #[arg(long = "hash_prefix", default_value_t = false)]
    hash_prefix: bool,

    /// shards If your object storage limit speed in a bucket level (or you're using a self-hosted object storage with
    /// limited performance), you can store the blocks into N buckets by hash of key (default: 0), when N is greater
    /// than 0, bucket should to be in the form of %d, e.g. --bucket "juicefs-%d". --shards cannot be changed afterwards
    /// and must be planned carefully ahead.
    #[arg(long = "shards", default_value = "0")]
    shards: Shards,
}

#[derive(Parser, Debug)]
struct ManagementOpts {
    /// storage space limit in GiB, default to 0 which means no limit. Capacity will include trash files, if trash is
    /// enabled.
    #[arg(long = "capacity", default_value_t = 0)]
    capacity: u64,

    /// limit the number of files, default to 0 which means no limit.
    #[arg(long = "inodes", default_value_t = 0)]
    inodes: u64,

    /// By default, delete files are put into trash, this option controls the number of days before trash files are
    /// expired, default to 1, set to 0 to disable trash.
    #[arg(long = "trash-days", default_value = "1")]
    trash_days: Option<u8>,

    /// enable POSIX ACL，it is irreversible.
    #[arg(long = "enable-acl", default_value_t = true, action = clap::ArgAction::Set)]
    enable_acl: bool,
}

pub static FORMAT_JUICE_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$").unwrap());

fn validate_name(name: &str) -> Result<String, String> {
    if !FORMAT_JUICE_NAME.is_match(name) {
        return Err(format!(
            "Invalid name: {}. Name must match regex: {:#?}",
            name, FORMAT_JUICE_NAME
        ));
    }
    Ok(name.to_string())
}

fn load_encrypt(path: &PathBuf) -> Result<String> {
    let mut file = std::fs::File::open(path).whatever_context("fail to open encrypt file")?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .whatever_context("fail to read encrypt file")?;
    let key = String::from_utf8(buf).whatever_context("fail to parse encrypt file")?;
    Ok(key)
}

fn into_format(format_opts: &FormatOpts) -> Result<Format> {
    let mut format = Format {
        name: format_opts.name.clone(),
        uuid: Uuid::new_v4(),
        storage: format_opts.data_opts.storage.clone(),
        storage_class: format_opts.data_opts.storage_class.clone(),
        bucket: format_opts.data_opts.bucket.clone(),
        endpoint: format_opts.data_opts.endpoint.clone(),
        access_key: format_opts.data_opts.access_key.clone(),
        secret_key: format_opts.data_opts.secret_key.clone(),
        session_token: format_opts.data_opts.session_token.clone(),
        block_size_kb: format_opts.data_format_opts.block_size_kb,
        compression: format_opts.data_format_opts.compress_algo.clone(),
        shards: format_opts.data_format_opts.shards.clone(),
        hash_prefix: format_opts.data_format_opts.hash_prefix,
        capacity: format_opts.management_opts.capacity << 30, // GB
        inodes: format_opts.management_opts.inodes,
        encrypt_key: format_opts
            .data_format_opts
            .encrypt_rsa_key_path
            .as_ref()
            .map(|path| load_encrypt(path))
            .transpose()?,
        encrypt_algo: format_opts.data_format_opts.encrypt_algo.clone(),
        key_encrypted: false,
        trash_days: format_opts.management_opts.trash_days,
        meta_version: MAX_VERSION,
        min_client_version: Some(MIN_CLIENT_VERSION.to_string()),
        enable_dir_stats: true,
        enable_acl: format_opts.management_opts.enable_acl,
        ..Default::default()
    };
    if format.enable_acl {
        format.max_client_version = Some(MAX_CLIENT_VERSION.to_string());
    }
    Ok(format)
}

async fn check_blob_connection(blob: &Operator, uuid: Uuid) -> Result<()> {
    let entries = blob.list("").await.whatever_context("Fail to list blob")?;
    for entry in entries {
        if entry.metadata().is_dir() {
            continue;
        }
        if entry.path() != STROAGE_IGNORED_ITEM && entry.path().starts_with(STROAGE_IGNORED_PREFIX)
        {
            whatever!(
                "Storage {blob:?} is not empty; please clean it up or pick another volume name"
            );
        }
    }
    if let Err(e) = blob
        .write(STROAGE_TEST_ITEM, uuid.into_bytes().to_vec())
        .await
    {
        warn!("Put uuid object: {}", e);
    }
    Ok(())
}

pub async fn juice_format(opts: FormatOpts) -> Result<()> {
    let meta = new_client(&opts.meta_url, juice_meta::config::Config::default()).await;
    let (format, _encrypted) = match meta.load(false).await {
        Ok(_) => {
            if opts.no_update {
                return Ok(());
            } else {
                (into_format(&opts)?, false)
            }
        }
        Err(e) => {
            if e.is_not_init() {
                (into_format(&opts)?, true)
            } else {
                return Err(e).whatever_context("fail to load format");
            }
        }
    };

    // check object storage ok or not
    let blob = new_operator(
        format.storage.clone(),
        &format.bucket,
        &format.endpoint,
        &format.access_key.clone().unwrap_or(String::new()),
        &format.secret_key.clone().unwrap_or(String::new()),
        None
    )
    .whatever_context("Fail to init operator")?;
    check_blob_connection(&blob, format.uuid.clone()).await?;
    info!("Volume is formatted as {:#?}", format);
    if let Err(e) = meta.init(format, opts.force).await {
        whatever!("fail to init meta: {}", e);
    }
    info!("Juice format success.");
    Ok(())
}
