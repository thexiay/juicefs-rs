use std::{
    fs::{OpenOptions, Permissions},
    io::Write,
    ops::{AsyncFn, AsyncFnMut, Range},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::Command,
    sync::Arc,
};

use bytes::BufMut;
use chrono::{Local, TimeDelta};
use clap::Parser;
use comfy_table::{
    Cell, Cells, Color, ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS,
    presets::UTF8_FULL,
};
use indicatif::{MultiProgress, ProgressBar};
use nix::unistd::{Uid, getuid};
use rand::RngCore;
use snafu::{ResultExt, ensure_whatever, whatever};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};
use tracing::warn;

use crate::{
    Result,
    utils::{find_mount_point, get_value_color},
};

#[derive(Parser, Debug)]
pub struct BenchOpts {
    /// Path to the JuiceFS mount point
    path: PathBuf,
    /// Block size; unit is MiB (default: 1)
    #[arg(long, default_value_t = 1)]
    block_size: u64,
    /// Large file size; unit is MiB (default: 1024)
    #[arg(long, default_value_t = 1024)]
    big_file_size: u64,
    /// Small file size; unit is KiB (default: 128)
    #[arg(long, default_value_t = 128)]
    small_file_size: u64,
    /// Number of small files; default is 100
    #[arg(long, default_value_t = 100)]
    small_file_count: u64,
    /// Number of concurrent threads; default is 1
    #[arg(long, default_value_t = 1)]
    threads: u64,
    /// drop linux kernel caches before benchmark
    #[arg(long, default_value_t = false, env = "SKIP_DROP_CACHES")]
    skip_drop_caches: bool,
}

#[derive(Clone, Debug)]
enum Api {
    Stats,
    Read,
    Write,
}

struct BenchCase {
    tmpdir: PathBuf,
    threads: u64,
    name: String,
    file_size: u64,
    block_size: u64,
    file_cnt: u64,  // file count
    block_cnt: u64, // block count
}

impl BenchCase {
    fn new(
        tmpdir: PathBuf,
        threads: u64,
        name: String,
        file_size: u64,
        block_size: u64,
        file_cnt: u64,
    ) -> Self {
        let (block_cnt, block_size, file_size) = if file_size <= block_size {
            (1, file_size, file_size)
        } else {
            let block_cnt = (file_size - 1) / block_size + 1;
            (block_cnt, block_size, block_cnt * block_size)
        };
        Self {
            tmpdir,
            threads,
            name,
            file_size,
            block_size,
            file_cnt,
            block_cnt,
        }
    }

    async fn run(self: Arc<Self>, api: Api, bars: &MultiProgress) -> TimeDelta {
        let mut task_group = JoinSet::new();
        let start = Local::now();
        let bar = bars.add(ProgressBar::new(self.file_cnt * self.threads));
        for i in 0..self.threads {
            let bench = self.clone();
            let api = api.clone();
            let bar = bar.clone();
            task_group.spawn(async move { bench.run_each(api, bar, i).await });
        }
        let _ = task_group.join_all().await;
        bar.finish();
        Local::now() - start
    }

    async fn run_each(&self, api: Api, bar: ProgressBar, index: u64) {
        match api {
            Api::Stats => {
                for i in 0..self.file_cnt {
                    let fname = self.tmpdir.join(format!("{}.{}.{}", self.name, index, i));
                    fs::metadata(&fname)
                        .await
                        .expect(&format!("Failed to stat file: {}", fname.display()));
                    bar.inc(1);
                }
            }
            Api::Read => {
                for i in 0..self.file_cnt {
                    let fname = self.tmpdir.join(format!("{}.{}.{}", self.name, index, i));
                    let mut file = fs::OpenOptions::new()
                        .read(true)
                        .open(&fname)
                        .await
                        .expect(&format!("Failed to open file: {}", fname.display()));
                    let mut buf = vec![0_u8; self.block_size as usize];
                    for _ in 0..self.block_cnt {
                        file.read_exact(&mut buf)
                            .await
                            .expect(&format!("Failed to read file: {}", fname.display()));
                        bar.inc(1);
                    }
                }
            }
            Api::Write => {
                for i in 0..self.file_cnt {
                    let fname = self.tmpdir.join(format!("{}.{}.{}", self.name, index, i));
                    let mut file = fs::OpenOptions::new()
                        .write(true)
                        .read(true)
                        .create(true)
                        .truncate(true)
                        .open(&fname)
                        .await
                        .expect(&format!("Failed to open file: {}", fname.display()));
                    let mut buf = vec![0_u8; self.block_size as usize];
                    rand::thread_rng().fill_bytes(&mut buf);
                    for _ in 0..self.block_cnt {
                        file.write_all(&buf)
                            .await
                            .expect(&format!("Failed to write file: {}", fname.display()));
                        bar.inc(1);
                    }
                }
            }
        }
    }
}

enum Action {
    Write,
    Read,
    Stat,
}

pub async fn juice_bench(opts: BenchOpts) -> Result<()> {
    // pre check
    ensure_whatever!(opts.block_size > 0, "Block size must be greater than 0");
    ensure_whatever!(opts.threads > 0, "Thread count must be greater than 0");
    ensure_whatever!(
        opts.big_file_size >= opts.block_size,
        "Big file size must be greater than block size"
    );
    ensure_whatever!(
        opts.small_file_size <= (opts.block_size << 10),
        "Small file size must be less than block size"
    );
    let (program, args) = if !cfg!(target_os = "linux") {
        whatever!("currently only support linux");
    } else {
        if getuid() == Uid::from_raw(0) {
            ("sh", vec!["-c", "echo 3 > /proc/sys/vm/drop_caches"])
        } else {
            ("sudo", vec!["sh -c 'echo 3 > /proc/sys/vm/drop_caches'"])
        }
    };

    // prepare
    // prepare tmpdir
    let tmpdir = opts.path.clone().join(format!(
        "__juicefs_benchmark_{}__",
        Local::now().timestamp_millis()
    ));
    fs::create_dir_all(&tmpdir)
        .await
        .whatever_context("create dir failed")?;
    fs::set_permissions(&tmpdir, Permissions::from_mode(0o777))
        .await
        .whatever_context("set permissions failed")?;
    // drop cache
    if !opts.skip_drop_caches {
        let status = Command::new(program)
            .args(args)
            .status()
            .whatever_context("failed to run command")?;
        if let Err(e) = status.exit_ok() {
            warn!("Failed to drop caches: {}", e);
        }
    } else {
        warn!("Clear cache operation has been skipped");
    }

    if let Ok(mp) = find_mount_point(&opts.path) {}

    // run benchmark
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["ITEM", "VALUE", "COST"]);
    let bars = MultiProgress::new();
    // run big object
    if opts.big_file_size > 0 {
        let case = Arc::new(BenchCase::new(
            tmpdir.clone(),
            opts.threads,
            "big file".to_string(),
            opts.big_file_size << 20, // convert MiB to bytes
            opts.block_size << 20,    // convert MiB to bytes
            opts.big_file_size,
        ));
        let write_elapse = case.clone().run(Api::Write, &bars).await;
        let write_value = ((case.file_size << 20) * case.threads) as f64 * 1000.0
            / write_elapse.num_milliseconds() as f64;
        let write_cost = write_elapse.num_milliseconds() as f64 / case.file_cnt as f64 / 1000.0;
        table.add_row(vec![
            Cell::new("Write big file"),
            Cell::new(format!("{write_value} MiB/s"))
                .fg(get_value_color(write_value, 100_f64..200_f64)),
            Cell::new(format!("{write_cost} s/file"))
                .fg(get_value_color(write_cost, 10_f64..50_f64)),
        ]);

        let read_cost = case.clone().run(Api::Read, &bars).await;
        let read_value = ((case.file_size << 20) * case.threads) as f64 * 1000.0
            / read_cost.num_milliseconds() as f64;
        let read_cost = read_cost.num_milliseconds() as f64 / case.file_cnt as f64 / 1000.0;
        table.add_row(vec![
            Cell::new("Read big file"),
            Cell::new(format!("{read_value} MiB/s"))
                .fg(get_value_color(read_value, 100_f64..200_f64)),
            Cell::new(format!("{read_cost} s/file")).fg(get_value_color(read_cost, 10_f64..50_f64)),
        ]);
    }
    // run small object
    if opts.small_file_size > 0 {
        let case = Arc::new(BenchCase::new(
            tmpdir.clone(),
            opts.threads,
            "small file".to_string(),
            opts.small_file_size << 10, // convert KiB to bytes
            opts.block_size << 20,      // convert MiB to bytes
            opts.small_file_count,
        ));

        let write_elapse = case.clone().run(Api::Write, &bars).await;
        let write_value =
            (case.file_cnt * case.threads) as f64 * 1000.0 / write_elapse.num_milliseconds() as f64;
        let write_cost = write_elapse.num_milliseconds() as f64 / case.file_cnt as f64;
        table.add_row(vec![
            Cell::new("Write small file"),
            Cell::new(format!("{write_value} files/s"))
                .fg(get_value_color(write_value, 100_f64..200_f64)),
            Cell::new(format!("{write_cost} ms/file"))
                .fg(get_value_color(write_cost, 10_f64..50_f64)),
        ]);

        let read_cost = case.clone().run(Api::Read, &bars).await;
        let read_value =
            (case.file_cnt * case.threads) as f64 * 1000.0 / read_cost.num_milliseconds() as f64;
        let read_cost = read_cost.num_milliseconds() as f64 / case.file_cnt as f64;
        table.add_row(vec![
            Cell::new("Read small file"),
            Cell::new(format!("{read_value} files/s"))
                .fg(get_value_color(read_value, 100_f64..200_f64)),
            Cell::new(format!("{read_cost} ms/file"))
                .fg(get_value_color(read_cost, 10_f64..50_f64)),
        ]);

        let stat_cost = case.clone().run(Api::Stats, &bars).await;
        let stat_value =
            (case.file_cnt * case.threads) as f64 * 1000.0 / stat_cost.num_milliseconds() as f64;
        let stat_cost = stat_cost.num_milliseconds() as f64 / case.file_cnt as f64;
        table.add_row(vec![
            Cell::new("Stat small file"),
            Cell::new(format!("{stat_value} files/s"))
                .fg(get_value_color(stat_value, 100_f64..200_f64)),
            Cell::new(format!("{stat_cost} s/file")).fg(get_value_color(stat_cost, 10_f64..50_f64)),
        ]);
    }

    // cleanup
    if let Err(e) = fs::remove_dir_all(&tmpdir).await {
        warn!(
            "Failed to remove temporary directory {}: {}",
            tmpdir.display(),
            e
        );
    }

    // TODO: stats
    println!("{}", table);
    Ok(())
}
