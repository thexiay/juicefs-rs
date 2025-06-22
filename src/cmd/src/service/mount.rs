use std::env;
use std::fmt::{self, Debug, Formatter};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::Duration;
use clap::Parser;
use dirs::home_dir;
use fuse3::raw::Session;
use juice_fuse::JuiceFs;
use juice_meta::api::new_client;
use juice_meta::config::{Config as MetaConfig, Format as MetaFormat};
use juice_storage::api::{new_chunk_store, new_operator};
use juice_storage::{CacheType, Config as ChunkConfig};
use juice_utils::common::meta::{AtimeMode, StorageType};
use juice_utils::common::storage::{CacheEviction, ChecksumLevel, UploadHourRange};
use juice_utils::runtime::LogTarget;
use juice_vfs::Vfs;
use juice_vfs::config::Config as VfsConfig;
use nix::unistd::{getgid, getpid, getppid, getuid};
use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::Result;

#[derive(Parser, Debug)]
pub struct MountOpts {
    /// The database URL for metadata storage, detail to see [here](https://juicefs.com/docs/zh/community/databases_for_metadata)
    meta_url: String,
    /// File system mount points, such as: `/mnt/jfs`, `Z:`.
    #[arg(value_parser = parse_mp)]
    mount_point: PathBuf,
    /// Runs in the background, default is false.
    #[arg(short = 'd', long = "background", default_value_t = false)]
    background: bool,
    /// Disable system logs, default to false.
    #[arg(long = "no-syslog", default_value_t = false)]
    no_syslog: bool,
    /// Force mount even if the mount point has been mounted by the same file system (default: false)
    #[arg(long = "force", default_value_t = false)]
    force: bool,
    /// Location of log files during background runtime (default: $HOME/.juicefs/juicefs.log or /var/log/juicefs.log)
    #[arg(long = "log", env = "JUICEFS_LOG_PATH", default_value_t = default_log_path())]
    pub log_path: String,
    #[arg(required = false, env = "_JFS_META_SID", default_value = None)]
    sid: Option<u64>,
    #[command(flatten)]
    fuse_opts: FuseOpts,
    #[command(flatten)]
    meta_opts: MetaOpts,
    #[command(flatten)]
    meta_cache_opts: MetaCacheOpts,
    #[command(flatten)]
    data_opts: DataStorageOpts,
    #[command(flatten)]
    data_cache_ops: DataCacheOpts,
    #[command(flatten)]
    pub log_opts: LogOpts,
}

#[derive(Parser, Debug)]
pub struct LogOpts {
    #[arg(long = "log-targets")]
    pub log_targets: Vec<LogTarget>,
}

#[derive(Parser, Debug)]
struct FuseOpts {}

#[derive(Parser, Debug)]
struct MetaOpts {
    /// Mount the specified subdirectory and mount the entire file system by default.
    #[arg(long = "subdir", default_value = "/")]
    subdir: String,
    #[arg(long = "backup-meta", default_value_t = 3600)]
    backup_meta: u32,
    #[arg(long = "backup-skip-trash", default_value_t = false)]
    backup_skip_trash: bool,
    #[arg(long = "heartbeat", value_parser = parse_duration_secs, default_value = "12")]
    heartbeat: Duration,
    #[arg(long = "read-only", default_value_t = false)]
    read_only: bool,
    #[arg(long = "no-bgjob", default_value_t = false)]
    no_bgjob: bool,
    #[arg(long = "atime-mode", value_enum, default_value_t)]
    atime_mode: AtimeMode,
    #[arg(long = "skip-dir-nlink", default_value_t = 20)]
    skip_dir_nlink: u32,
    #[arg(long = "skip-dir-mtime", value_parser = parse_duration, default_value = "100ms")]
    skip_dir_mtime: Duration,
}

#[derive(Parser, Debug)]
struct MetaCacheOpts {
    #[arg(long = "attr-cache", default_value_t = 1)]
    attr_cache: u64,
    #[arg(long = "entry-cache", default_value_t = 1)]
    entry_cache: u64,
    #[arg(long = "dir-entry-cache", default_value_t = 1)]
    dir_entry_cache: u64,
    #[arg(long = "open-cache", value_parser = parse_duration_secs_option, default_value = None)]
    open_cache: Option<Duration>,
    #[arg(long = "open-cache-limit", default_value_t = 10000)]
    open_cache_limit: u64,
    #[arg(long = "readdir-cache", default_value_t = false)]
    readdir_cache: bool,
    /// Failed lookup query (return ENOENT) cache expiration time,
    /// default is 0, which means no cache
    #[arg(long = "negative-entry-cache", default_value_t = 0)]
    negative_entry_cache: u64,
}

#[derive(Parser, Debug)]
struct DataStorageOpts {
    #[arg(long = "storage", value_enum, default_value_t = StorageType::File)]
    storage: StorageType,
    /// Specifies the Endpoint to access the object store for the current mount point.
    #[arg(long = "bucket", default_value = "")]
    bucket: String,
    /// Specifies the Endpoint to access the object store for the current mount point.
    #[arg(long = "endpoint", default_value = "")]
    endpoint: String,
    /// The storage type of data written by the client, different object storage providers have different
    /// storage classes, such as "Standard", "Infrequent Access", "Archive" and so on.
    #[arg(long = "storage-class")]
    storage_class: Option<String>,
    /// Timeout time for downloading an object; unit is seconds (default: 60)
    #[arg(long = "get-timeout", default_value_t = 60)]
    get_timeout: u32,
    /// Timeout time for uploading an object; unit is seconds (default: 60)
    #[arg(long = "put-timeout", default_value_t = 60)]
    put_timeout: u32,
    /// The number of retries in network exceptions and the number of retries in metadata requests
    /// is also controlled by this option. If the number of retrys exceeds the EIO Input/output error will be returned.
    ///  (Default: 10)
    #[arg(long = "io-retries", default_value_t = 10)]
    io_retries: u32,
    /// Upload concurrency, default is 20. For write mode with granularity of 4M, 20 concurrency is already a very high
    /// default value. In such a write mode, increasing write concurrency often requires an increase of --buffer-size.
    /// For details, see "Read and Write Buffer". However, when faced with small random writes at a level of 100 K, the
    /// amount of concurrency is large and it is easy to cause blocking and waiting, causing the writing speed to
    /// deteriorate. If the application write mode cannot be improved and merged, then higher write concurrency needs
    /// to be considered to avoid waiting in line.
    #[arg(long = "max-uploads", default_value_t = 20)]
    max_uploads: u32,
    /// The maximum concurrency number of asynchronous writes data blocks to the cache disk. If the maximum concurrency
    /// number is reached, the object storage will be uploaded directly (this option is only valid when "client write
    /// cache" is enabled) (default value: 0, that is, there is no concurrency limit)
    #[arg(long = "max-stage-write", default_value_t = 0)]
    max_stage_write: u32,
    /// Number of connections to delete objects (default: 10)
    #[arg(long = "max-deletes", default_value_t = 10)]
    max_deletes: u32,
    /// Upload bandwidth limit, unit is Mbps (default: 0)
    #[arg(long = "upload-limit", default_value_t = 10)]
    upload_limit: u32,
    /// Download bandwidth limit, unit is Mbps (default: 0)
    #[arg(long = "download-limit", default_value_t = 10)]
    download_limit: u32,
}

#[derive(Parser, Debug)]
struct DataCacheOpts {
    /// Total size of read and write buffer; unit is MiB (default: 300)
    #[arg(long = "buffer-size", default_value_t = 300)]
    buffer_size: u32,
    /// Concurrent read-ahead N blocks (default: 1)
    #[arg(long = "prefetch", default_value_t = 1)]
    prefetch: u32,
    /// Asynchronous upload of objects in the background, default is false
    #[arg(long = "writeback", default_value_t = false)]
    writeback: bool,
    /// After enabling --writeback, you can use this option to control the delayed upload of data to the object store.
    /// The default is 0 seconds, which is equivalent to uploading immediately after writing. This option also supports
    /// units such as s (seconds), m (minutes), and h (time). If the data is deleted by the application within the
    /// waiting time, there is no need to upload it to the object store again. If the data is just temporarily dropped,
    /// you can consider using this option to save resources.
    #[arg(long = "upload-delay", default_value_t = 0)]
    upload_delay: u32,
    /// Once --writeback is enabled, only data blocks are uploaded during the specified time period of the day.
    /// The format of the parameters is <start hour> and <end hour> (including "start hour", but does not include "end
    /// hour", "start hour" must be less than or greater than "end hour"), where the value range of <hour> is 0 to 23.
    /// For example, 0,6 means only uploading data blocks between 0:00 and 5:59 every day, 23,3 means only uploading
    /// data blocks between 23:00 and 2:59 the next day
    #[arg(long = "upload-hours")]
    upload_hours: Option<UploadHourRange>,
    /// Local cache directory path; use: (Linux, macOS) or ; (Windows) to isolate multiple paths
    /// (default: $HOME/.juicefs/cache or /var/jfsCache)
    #[arg(long = "cache-dir", default_value_t = default_cache_dir())]
    cache_dir: CacheDir,
    /// File permissions for cache blocks (default: "0600")
    #[arg(long = "cache-mode", default_value_t = 0o600)]
    cache_mode: u32,
    /// Total size of cached objects; unit is MiB (default: 102400)
    #[arg(long = "cache-size", default_value_t = 102400)]
    cache_size: u32,
    /// Maximum number of cached items (default: unlimited)
    #[arg(long = "cache-items", default_value_t = 0)]
    cache_items: u32,
    /// Minimum remaining space ratio, default is 0.1. If "client write cache" is enabled, this parameter also controls
    /// the space occupied by the write cache. Read "Client Read Cache" for more information.
    #[arg(long = "free-space-ratio", default_value_t = 0.1)]
    free_space_ratio: f32,
    /// Only cache random small block reads, default is false. Read "Client Read Cache" for more information.
    #[arg(long = "cache-partial-only", default_value_t = false)]
    cache_partial_only: bool,
    /// Cache complete data blocks after uploading (default: false)
    #[arg(long = "cache-large-write", default_value_t = false)]
    cache_large_write: bool,
    /// The cached data consistency check level. After enabling Checksum verification, when generating cache files,
    /// checksum will be performed on the data and recorded at the end of the file for verification when reading cache.
    /// The following levels are supported:
    ///
    /// - none: Disable consistency checking, if local data is tampered with, wrong data will be read;
    /// - full (default before 1.3): Check only when reading the complete data block, suitable for sequential reading
    ///   scenarios;
    /// - shrink: Verify the slice data within the read range. The check range does not contain the slice where the read
    ///   boundary is located (can be understood as an open interval), which is suitable for random reading scenarios;
    /// - extend (default after 1.3): Verify the slice data in the read range. The verification range also contains the
    ///   slice where the read boundary is located (which can be understood as a closed interval), so it will bring a
    ///   certain degree of read amplification, suitable for random reading scenarios with extreme requirements for
    ///   accuracy.
    #[arg(long = "verify-cache-checksum", value_enum, default_value_t = ChecksumLevel::Extend)]
    verify_cache_checksum: ChecksumLevel,
    /// Cache eviction strategy (none or 2-random) (default: 2-random)
    #[arg(long = "cache-eviction", value_enum, default_value_t)]
    cache_eviction: CacheEviction,
    /// Interval for scanning the cache directory to rebuild the memory index
    #[arg(long = "cache-scan-interval", value_parser = parse_duration, default_value = "1h")]
    cache_scan_interval: Duration,
    /// Cached blocks that have not been accessed for the set time will be automatically cleared
    /// (even if the value of --cache-eviction is none, these cached blocks will also be deleted),
    /// unit is seconds, value is 0 means never expire (default: 0)
    #[arg(long = "cache-expire", default_value_t = 0)]
    cache_expire: u32,
    /// Maximum pre-read buffer size, unit is MiB(default: 0, which means no limit)
    #[arg(long = "max-readahead", default_value_t = 0)]
    max_readahead: u32,
}

// cache dir wrapper
#[derive(Debug, Eq, PartialEq, Clone)]
struct CacheDir(Vec<PathBuf>);

impl FromStr for CacheDir {
    type Err = String;

    fn from_str(cache_dirs: &str) -> Result<Self, Self::Err> {
        let paths: Vec<&str> = cache_dirs.split(':').collect();
        let mut result = Vec::new();
        for path in paths {
            let path = PathBuf::from(path);
            result.push(path);
        }
        Ok(Self(result))
    }
}

impl fmt::Display for CacheDir {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect::<Vec<String>>()
                .join(":")
        )
    }
}

fn default_cache_dir() -> CacheDir {
    if getuid().is_root() {
        CacheDir(vec![PathBuf::from("/var/jfsCache")])
    } else {
        home_dir()
            .map(|home| {
                let cache_dir = format!("{}/.juicefs/cache", home.display());
                CacheDir(vec![PathBuf::from(cache_dir)])
            })
            .expect("Failed to get home directory")
    }
}

fn default_log_path() -> String {
    let uid = getuid();
    if uid.is_root() {
        String::from("/var/log")
    } else {
        home_dir()
            .expect("fail to get home dir")
            .join(".juicefs")
            .join("log")
            .to_string_lossy()
            .into_owned()
    }
}

fn parse_mp(s: &str) -> Result<PathBuf, String> {
    if s == "/" {
        return Err(format!("should not mount on the root directory"));
    }
    Ok(PathBuf::from(s))
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    match s.parse::<humantime::Duration>() {
        Ok(duration) => Ok(Duration::milliseconds(duration.as_millis() as i64)),
        Err(_) => Err(format!("Invalid duration: {}", s)),
    }
}

fn parse_duration_secs(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(|secs| Duration::seconds(secs as i64))
        .map_err(|e| format!("Invalid duration: {}", e))
}

fn parse_duration_secs_option(s: &str) -> Result<Option<Duration>, String> {
    match s.parse::<u64>() {
        Ok(secs) => {
            if secs == 0 {
                Ok(None)
            } else {
                Ok(Some(Duration::seconds(secs as i64)))
            }
        }
        Err(e) => Err(format!("Invalid duration: {}", e)),
    }
}

fn into_meta_config(opts: &MountOpts) -> MetaConfig {
    MetaConfig {
        retries: opts.data_opts.io_retries,
        max_deletes_task: opts.data_opts.max_deletes,
        skip_dir_nlink: opts.meta_opts.skip_dir_nlink as isize,
        read_only: opts.meta_opts.read_only,
        enable_bg_job: opts.meta_opts.no_bgjob,
        open_cache: opts
            .meta_cache_opts
            .open_cache
            .map(|d| d.to_std().expect("invalid duration")),
        open_cache_limit: opts.meta_cache_opts.open_cache_limit,
        heartbeat: opts.meta_opts.heartbeat.to_std().expect("invalid duration"),
        mount_point: opts.mount_point.clone(),
        subdir: opts.meta_opts.subdir.clone(),
        atime_mode: opts.meta_opts.atime_mode.clone(),
        skip_dir_mtime: opts
            .meta_opts
            .skip_dir_mtime
            .to_std()
            .expect("invalid duration"),
        sid: opts.sid,
        ..Default::default()
    }
}

fn into_chunk_config(opts: &MountOpts) -> ChunkConfig {
    ChunkConfig {
        cache_type: CacheType::Disk,
        cache_dirs: opts.data_cache_ops.cache_dir.0.clone(),
        cache_file_mode: Permissions::from_mode(opts.data_cache_ops.cache_mode),
        cache_size: (opts.data_cache_ops.cache_size as u64) << 20,
        ..Default::default()
    }
}

fn into_vfs_config(
    meta: &MetaConfig,
    format: &MetaFormat,
    chunk: &ChunkConfig,
    opts: &MountOpts,
) -> VfsConfig {
    VfsConfig {
        meta: meta.clone(),
        format: format.clone(),
        chunk: chunk.clone(),
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
    }
}

async fn backgroup_tasks() {
    // metrics and backup meta
}

pub async fn juice_mount(shutdown: CancellationToken, opts: &MountOpts) -> Result<()> {
    // 1. init vfs
    let meta_config = into_meta_config(&opts);
    let meta = new_client(&opts.meta_url, meta_config.clone()).await;
    let format = match meta.load(true).await {
        Ok(format) => format,
        Err(e) => {
            panic!("Failed to load meta: {}", e);
        }
    };

    meta.chroot(&opts.meta_opts.subdir)
        .await
        .expect(format!("chroot {} err", opts.meta_opts.subdir).as_str());
    meta.new_session(true).await.expect("new session err");
    let chunk_config = into_chunk_config(&opts);
    let vfs_config = into_vfs_config(&meta_config, &format, &chunk_config, &opts);
    let operator = new_operator(
        vfs_config.format.storage.clone(),
        &vfs_config.format.bucket,
        &vfs_config.format.endpoint,
        &vfs_config
            .format
            .access_key
            .clone()
            .unwrap_or(String::new()),
        &vfs_config
            .format
            .secret_key
            .clone()
            .unwrap_or(String::new()),
        None,
    )
    .whatever_context("operator init err:")?;
    info!("Data use {:?}", operator.info());
    let store = new_chunk_store(chunk_config.clone(), operator).whatever_context("store init: ")?;
    let vfs = Vfs::new(vfs_config, meta.clone(), store);
    // TODO: update format

    // 2. do mount
    std::fs::create_dir_all(&opts.mount_point).whatever_context("create mount point fail")?;
    let fs = JuiceFs::new(vfs);
    let not_unprivileged = env::var("NOT_UNPRIVILEGED").ok().as_deref() == Some("1");
    let mut mount_options = fs.mount_opts();
    mount_options.uid(getuid().as_raw()).gid(getgid().as_raw());

    info!(
        "Mounting volumn {} at {}",
        format.name,
        opts.mount_point.display()
    );
    let mut mount_handle = if !not_unprivileged {
        Session::new(mount_options)
            .mount_with_unprivileged(fs, &opts.mount_point)
            .await
            .unwrap()
    } else {
        Session::new(mount_options)
            .mount(fs, &opts.mount_point)
            .await
            .unwrap()
    };

    let handle = &mut mount_handle;
    tokio::select! {
        res = handle => res.unwrap(),
        _ = shutdown.cancelled() => {
            mount_handle.unmount().await.unwrap();
            meta.close_session().await.unwrap();
            info!("The juicefs mount process exit successfully, mountpoint: {}", opts.mount_point.display())
        }
    }
    Ok(())
}
