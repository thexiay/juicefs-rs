use chrono::Local;
use clap::Parser;
use juice_storage::api::new_operator;
use juice_utils::common::meta::{Shards, StorageType};
use snafu::ResultExt;
use tracing::info;

use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::Result;

const PASS: &str = "pass";
const NSPT: &str = "not supported";
const FAILED: &str = "failed";

#[derive(Parser, Debug)]
pub struct ObjBenchOpts {
    #[arg(long = "storage", value_enum, default_value_t = StorageType::File)]
    storage: StorageType,
    /// Specifies the Endpoint to access the object store for the current mount point.
    #[arg(long = "bucket", default_value = "")]
    bucket: String,
    /// Specifies the Endpoint to access the object store for the current mount point.
    #[arg(long = "endpoint", default_value = "")]
    endpoint: String,
    /// Access Key for object storage (can also be set via the environment variable ACCESS_KEY),
    /// see How to Set Up Object Storage for more.
    #[arg(long = "access-key", env = "ACCESS_KEY", default_value = None)]
    access_key: Option<String>,
    /// Secret Key for object storage (can also be set via the environment variable SECRET_KEY),
    /// see How to Set Up Object Storage for more.
    #[arg(long = "secret-key", env = "SECRET_KEY", default_value = None)]
    secret_key: Option<String>,
    /// shards If your object storage limit speed in a bucket level (or you're using a self-hosted object storage with
    /// limited performance), you can store the blocks into N buckets by hash of key (default: 0), when N is greater
    /// than 0, bucket should to be in the form of %d, e.g. --bucket "juicefs-%d". --shards cannot be changed afterwards
    /// and must be planned carefully ahead.
    #[arg(long = "shards", default_value = "0")]
    shards: Shards,
    /// 每个 IO 块的大小（以 KiB 为单位）（默认值：4096）
    #[arg(long = "block-size", default_value = "4096")]
    block_size: u32,
    /// 大文件的总大小（以 MiB 为单位）（默认值：1024）
    #[arg(long = "big-object-size", default_value_t = 1024)]
    big_object_size: u32,
    /// 每个小文件的大小（以 KiB 为单位）（默认值：128）
    #[arg(long = "small-object-size", default_value_t = 128)]
    small_object_size: u32,
    /// 小文件的数量（默认值：100）
    #[arg(long = "small-objects", default_value_t = 100)]
    small_objects: u32,
    /// 跳过功能测试（默认值：false）
    #[arg(long = "skip-functional-tests", default_value_t = false)]
    skip_functional_tests: bool,
    /// 上传下载等操作的并发数（默认值：4）
    #[arg(long = "threads", short = 'p', default_value_t = 4)]
    threads: u32,
}

mod functional {
    use std::ops::{AsyncFnOnce, RangeBounds};

    use crate::Result;
    use bytes::Bytes;
    use chrono::{TimeDelta, Utc};
    use comfy_table::{
        Attribute, Cell, Color, ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS,
        presets::UTF8_FULL,
    };
    use opendal::{Buffer, ErrorKind, Operator};
    use rand::RngCore;
    use tracing::debug;

    use super::{NSPT, PASS};

    struct Case {
        title: String,
        sync: bool,
    }

    impl From<(&str, bool)> for Case {
        fn from(tuple: (&str, bool)) -> Self {
            Self {
                title: tuple.0.to_string(),
                sync: tuple.1,
            }
        }
    }

    async fn run_case<F>(case: Case, blob: &Operator, table: &mut Table, fn_: F)
    where
        F: AsyncFnOnce(&Operator) -> Result<(), opendal::Error>,
    {
        let r = match fn_(&blob).await {
            Ok(_) => Cell::new(PASS)
                .fg(Color::Green)
                .add_attribute(Attribute::Bold),
            Err(e) if e.kind() == ErrorKind::Unsupported => Cell::new(NSPT)
                .fg(Color::Yellow)
                .add_attribute(Attribute::Bold),
            Err(e) => {
                debug!("run case failed: {}", e);
                Cell::new(e).fg(Color::Red)
            }
        };
        let title = Cell::new(case.title).add_attribute(Attribute::Bold);
        table.add_row(vec![
            if case.sync {
                Cell::new("sync").fg(Color::Blue)
            } else {
                Cell::new("basic").fg(Color::Blue)
            },
            title,
            r,
        ]);
    }

    async fn get(
        blob: &Operator,
        key: &str,
        range: impl RangeBounds<u64>,
    ) -> Result<Bytes, opendal::Error> {
        let reader = blob.reader(key).await?;
        let buffer = reader.read(range).await?;
        Ok(buffer.to_bytes())
    }

    const KEY: &str = "put_test_file";

    async fn run_fs_case(
        case: Case,
        blob: &Operator,
        results: &mut Table,
        fn_: impl AsyncFnOnce(&Operator) -> Result<(), opendal::Error>,
    ) {
        run_case(case, blob, results, async |blob: &Operator| {
            let br = Buffer::from("hello");
            blob.write(KEY, br).await?;
            fn_(blob).await?;
            Ok(())
        })
        .await;
        let _ = blob.delete(KEY).await; // nolint:errcheck
    }

    pub(crate) async fn testsing(blob: Operator) {
        println!("Start Functional Testing ...");
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec!["CATEGORY", "TEST", "RESULT"]);

        run_case(
            Case::from(("put an object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = Buffer::from("hello");
                blob.write(KEY, br).await
            },
        )
        .await;

        blob.delete(KEY).await.ok(); //nolint:errcheck

        run_case(
            Case::from(("get an object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = Bytes::from("hello");
                blob.write(KEY, Buffer::from(br.clone())).await?;

                let data = get(blob, KEY, 0..).await?;
                if data != br {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get an object: expect 'hello', but got {:?}",
                            data
                        ),
                    ));
                }

                let data = get(blob, KEY, 0..5).await?;
                if data != br {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get an object: expect 'hello', but got {:?}",
                            data
                        ),
                    ));
                }
                Ok(())
            },
        )
        .await;
        blob.delete(KEY).await.ok(); //nolint:errcheck

        run_case(
            Case::from(("get non-exist", false)),
            &blob,
            &mut table,
            async |blob: &Operator| match get(blob, "not_exists_file", 0..).await {
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(opendal::Error::new(
                    ErrorKind::Unexpected,
                    format!("get not existed object should failed: {}", e),
                )),
                Ok(_) => Err(opendal::Error::new(
                    ErrorKind::Unexpected,
                    "get not existed object should failed",
                )),
            },
        )
        .await;

        run_case(
            Case::from(("get partial object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = "hello";
                blob.write(KEY, Buffer::from(br)).await?;

                // get first
                let data = get(blob, KEY, 0..1).await?;
                if data != b"h".as_ref() {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get the first byte: expect 'h', but got {:?}",
                            data
                        ),
                    ));
                }

                // get last
                let data = get(blob, KEY, 4..5).await?;
                if data != b"o".as_ref() {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get the last byte: expect 'o', but got {:?}",
                            data
                        ),
                    ));
                }

                // get last 3
                let data = get(blob, KEY, 2..5).await?;
                if data != b"llo".as_ref() {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get the last three bytes: expect 'llo', but got {:?}",
                            data
                        ),
                    ));
                }

                // get middle
                let data = get(blob, KEY, 2..4).await?;
                if data != b"ll".as_ref() {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get two bytes: expect 'll', but got {:?}",
                            data
                        ),
                    ));
                }

                // get the end out of range
                let data = get(blob, KEY, 4..).await?;
                if data != b"o".as_ref() {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to get object with the end out of range, expect 'o', but got {:?}",
                            data
                        ),
                    ));
                }
                // not support invalid range 
                Ok(())
            }
        ).await;
        blob.delete(KEY).await.ok(); //nolint:errcheck

        run_case(
            Case::from(("head an object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = Buffer::from("hello");
                blob.write(KEY, br).await?;
                let meta = blob.stat(KEY).await?;
                if meta.content_length() != 5 {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "failed to head object: expect size 5, but got {}",
                            meta.content_length()
                        ),
                    ));
                }
                Ok(())
            },
        )
        .await;
        blob.delete(KEY).await.ok(); //nolint:errcheck

        run_case(
            Case::from(("delete an object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = Buffer::from("hello");
                blob.write(KEY, br).await?;
                blob.delete(KEY).await?;
                match blob.stat(KEY).await {
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!("stat deleted object should failed: {}", e),
                    )),
                    Ok(_) => Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        "stat deleted object should failed",
                    )),
                }
            },
        )
        .await;

        run_case(
            Case::from(("delete non-exist", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                blob.delete("not_exists_file").await.map_err(|e| {
                    opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "deleting a non-existent object should succeed, but got {}",
                            e.kind()
                        ),
                    )
                })
            },
        )
        .await;

        run_case(
            Case::from(("list objects", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = Buffer::from("hello");
                blob.write(KEY, br.clone()).await?;
                let objs = blob.list("").await?;
                if objs.len() != 1 {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "list should return 1 keys, but got {} keys: {}",
                            objs.len(),
                            objs.iter().map(|o| o.path()).collect::<Vec<_>>().join(", ")
                        ),
                    ));
                }
                if objs[0].path() != KEY {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!("first key should be {}, but got {}", KEY, objs[1].path()),
                    ));
                }
                if objs[0].metadata().content_length() != 5 {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "size of first key should be 5, but got {}",
                            objs[1].metadata().content_length()
                        ),
                    ));
                }
                let now = Utc::now();
                if let Some(mtime) = objs[0].metadata().last_modified()
                    && mtime < now - TimeDelta::seconds(30)
                    && mtime > now + TimeDelta::seconds(-30)
                {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "mtime of key should be within 30 seconds, but got {:?}",
                            objs[1].metadata().last_modified()
                        ),
                    ));
                }

                let key_total = 100;
                let mut sorted_keys = Vec::with_capacity(key_total);
                for i in 0..key_total {
                    let k = format!("hashKey{i}");
                    sorted_keys.push(k.clone());
                    blob.write(&k, br.clone()).await?;
                }
                sorted_keys.sort();
                let objs = blob.list("hashKey").await?;
                if objs.len() != key_total {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "list should return {} keys, but got {} keys",
                            key_total,
                            objs.len()
                        ),
                    ));
                }
                for i in 0..key_total {
                    if objs[i].path() != sorted_keys[i] {
                        return Err(opendal::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "the {}th key should be {}, but got {}",
                                i,
                                sorted_keys[i],
                                objs[i].path()
                            ),
                        ));
                    }
                }
                Ok(())
            },
        )
        .await;
        blob.delete(KEY).await.ok(); //nolint:errcheck
        for i in 0..100 {
            let k = format!("hashKey{i}");
            let _ = blob.delete(&k).await; //nolint:errcheck
        }

        const SPECIAL_KEY: &str = "测试编码文件{\"name\":\"juicefs\"}\u{1F}%uFF081%uFF09.jpg";
        run_case(
            Case::from(("special key", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let br = Buffer::from("1");
                blob.write(SPECIAL_KEY, br).await?;
                let objs = blob.list("测试编码文件").await?;
                if objs.len() != 1 || objs[0].path() != SPECIAL_KEY {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "list encode file failed: expect key {}, but got {:?}",
                            SPECIAL_KEY, objs
                        ),
                    ));
                }
                Ok(())
            },
        )
        .await;
        blob.delete(SPECIAL_KEY).await.ok(); //nolint:errcheck

        run_fs_case(
            Case::from(("put a big object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                let fsize = 256 << 20;
                let buff_l = 4 << 20;
                let mut buff = vec![0u8; buff_l];
                rand::thread_rng().fill_bytes(&mut buff);
                let count = (fsize as f64 / buff_l as f64).floor() as usize;
                let mut content = vec![0u8; fsize];
                for i in 0..count {
                    content[i * buff_l..(i + 1) * buff_l].copy_from_slice(&buff);
                }
                blob.write(KEY, Buffer::from(content)).await?;
                Ok(())
            },
        )
        .await;
        blob.delete(KEY).await.ok(); //nolint:errcheck

        run_fs_case(
            Case::from(("put an empty object", false)),
            &blob,
            &mut table,
            async |blob: &Operator| {
                // Copy empty objects
                blob.write("empty_test_file", Buffer::from("")).await?;
                // Copy `/` suffixed object
                match blob.write("slash_test_file/", Buffer::from("1")).await {
                    Err(e) if e.kind() == ErrorKind::IsADirectory => (),
                    other => {
                        return Err(opendal::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "slash suffixed object is not excepted error. {}",
                                other
                                    .err()
                                    .map(|e| e.to_string())
                                    .unwrap_or("unexcepted success".to_string())
                            ),
                        ));
                    }
                };
                Ok(())
            },
        )
        .await;
        blob.delete("empty_test_file").await.ok(); //nolint:errcheck
        blob.delete("slash_test_file/").await.ok(); //nolint:errcheck

        // TODO: change owner/group
        println!("{table}");
    }
}

mod performance {
    use std::{
        ops::Range,
        sync::{Arc, OnceLock},
    };

    use bytes::Bytes;
    use chrono::Local;
    use comfy_table::{
        Cell, Cells, Color, ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS,
        presets::UTF8_FULL,
    };
    use futures::StreamExt;
    use indicatif::{MultiProgress, ProgressBar, ProgressFinish, ProgressStyle};
    use opendal::{Buffer, ErrorKind, Operator};
    use rand::RngCore;
    use tokio::{sync::Semaphore, task::JoinSet};
    use tracing::error;

    use crate::tool::obj_bench::FAILED;

    use super::ObjBenchOpts;

    #[derive(Clone, Debug)]
    enum ApiInfo {
        SmallPut,
        SmallGet,
        Put,
        Get,
        List,
        Head,
        Delete,
    }

    impl ApiInfo {
        fn title(&self) -> String {
            match self {
                ApiInfo::SmallPut => "put samll objects".to_string(),
                ApiInfo::SmallGet => "get small objects".to_string(),
                ApiInfo::Put => "upload objects".to_string(),
                ApiInfo::Get => "download objects".to_string(),
                ApiInfo::List => "list objects".to_string(),
                ApiInfo::Head => "head objects".to_string(),
                ApiInfo::Delete => "delete objects".to_string(),
            }
        }

        fn count(&self, opts: &ObjBenchOpts) -> u32 {
            match self {
                ApiInfo::List => 50,
                ApiInfo::Put | ApiInfo::Get => {
                    ((opts.big_object_size << 10) as f64 / opts.block_size as f64).ceil() as u32
                }
                _ => opts.small_objects,
            }
        }

        fn start_key(&self, opts: &ObjBenchOpts) -> u32 {
            match self {
                ApiInfo::Put | ApiInfo::Get => opts.small_objects,
                _ => 0,
            }
        }

        async fn after(&self, opts: &ObjBenchOpts, blob: &Operator) {
            match self {
                ApiInfo::Get => {
                    // Clean up all objects uploaded in the test
                    let start_key = self.start_key(opts);
                    let count = self.count(opts);
                    for i in start_key..start_key + count {
                        let _ = blob.delete(&i.to_string()).await; //nolint:errcheck
                    }
                }
                _ => {}
            }
        }

        fn get_result(&self, opts: &ObjBenchOpts, cost: chrono::TimeDelta) -> Cells {
            let title: Cell = Cell::new(self.title());
            let threads = opts.threads as f64;
            let sobj_cnt = opts.small_objects as f64;
            let bobj_cnt = ((opts.big_object_size << 10) as f64 / opts.block_size as f64).ceil();
            let block_size = opts.block_size << 10;
            let cost = cost.num_nanoseconds().unwrap_or(0) as f64 / 1_000_000_000.0;

            fn get_color_value(value: f64, range: Range<f64>) -> Color {
                if value > range.end {
                    Color::Green
                } else if value > range.start {
                    Color::Yellow
                } else {
                    Color::Red
                }
            }

            fn get_color_cost(value: f64, range: Range<f64>) -> Color {
                if value < range.start {
                    Color::Green
                } else if value < range.end {
                    Color::Yellow
                } else {
                    Color::Red
                }
            }

            match self {
                ApiInfo::SmallPut => {
                    let speed = sobj_cnt / cost;
                    let cost_per_object = cost * 1000_f64 * threads / sobj_cnt;
                    let speed_color = get_color_value(speed, 10.0..30.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 30.0..100.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} objects/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/object"))
                            .fg(cost_per_object_color),
                    ]
                    .into()
                }
                ApiInfo::SmallGet => {
                    let speed = sobj_cnt / cost;
                    let cost_per_object = cost * 1000_f64 * threads / sobj_cnt;
                    let speed_color = get_color_value(speed, 10.0..30.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 30.0..100.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} objects/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/object"))
                            .fg(cost_per_object_color),
                    ]
                    .into()
                }
                ApiInfo::Put => {
                    let speed = (block_size >> 20) as f64 * bobj_cnt / cost; // MB
                    let cost_per_object = cost * 1000_f64 * threads / bobj_cnt;
                    let speed_color = get_color_value(speed, 100.0..150.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 50.0..150.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} MiB/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/object"))
                            .fg(cost_per_object_color),
                    ]
                    .into()
                }
                ApiInfo::Get => {
                    let speed = (block_size >> 20) as f64 * bobj_cnt / cost; // MB
                    let cost_per_object = cost * 1000_f64 * threads / bobj_cnt;
                    let speed_color = get_color_value(speed, 100.0..150.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 50.0..150.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} MiB/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/object"))
                            .fg(cost_per_object_color),
                    ]
                    .into()
                }
                ApiInfo::List => {
                    let speed = sobj_cnt * 100_f64 / cost;
                    let cost_per_object = cost * 10_f64 * threads;
                    let speed_color = get_color_value(speed, 1000.0..10000.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 100.0..200.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} objects/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/op")).fg(cost_per_object_color),
                    ]
                    .into()
                }
                ApiInfo::Head => {
                    let speed = sobj_cnt / cost;
                    let cost_per_object = cost * 1000_f64 * threads / sobj_cnt;
                    let speed_color = get_color_value(speed, 10.0..30.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 30.0..100.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} objects/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/object"))
                            .fg(cost_per_object_color),
                    ]
                    .into()
                }
                ApiInfo::Delete => {
                    let speed = sobj_cnt / cost;
                    let cost_per_object = cost * 1000_f64 * threads / sobj_cnt;
                    let speed_color = get_color_value(speed, 10.0..30.0);
                    let cost_per_object_color = get_color_cost(cost_per_object, 30.0..100.0);

                    vec![
                        title,
                        Cell::new(format!("{speed:.2} objects/s")).fg(speed_color),
                        Cell::new(format!("{cost_per_object:.2} ms/object"))
                            .fg(cost_per_object_color),
                    ]
                    .into()
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    struct BenchmarkObj {
        bars: MultiProgress,
        blob: Operator,
        opts: Arc<ObjBenchOpts>,
        seed: Bytes,
        small_seed: Bytes,
    }

    impl BenchmarkObj {
        async fn run(&self, api: ApiInfo) -> Cells {
            let bar = add_bar(&self.bars, &api.title(), api.count(&self.opts) as u64);
            let start = Local::now();
            let mut task_group = JoinSet::new();
            let first_err = Arc::new(OnceLock::new());
            let semaphore = Arc::new(Semaphore::const_new(self.opts.threads as usize));

            let start_key = api.start_key(&self.opts);
            let end_key = start_key + api.count(&self.opts);
            for i in start_key..end_key {
                let api = api.clone();
                let first_err = first_err.clone();
                let bar = bar.clone();
                let bench = self.clone();
                let semaphore = semaphore.clone();
                task_group.spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    if let Err(e) = bench.run_each(api, i, start_key).await {
                        first_err.set(e).ok();
                    }
                    bar.inc(1);
                });
            }
            task_group.join_all().await;
            bar.finish();
            api.after(&self.opts, &self.blob).await;
            match first_err.get() {
                Some(e) => {
                    error!("test {} failed: {}", api.title(), e);
                    return Cells::from(vec![
                        Cell::new(api.title()),
                        Cell::new(FAILED).fg(Color::Red),
                        Cell::new(FAILED).fg(Color::Red),
                    ]);
                }
                None => {}
            }
            api.get_result(&self.opts, Local::now() - start)
        }

        async fn run_each(
            &self,
            api: ApiInfo,
            cur_key: u32,
            start_key: u32,
        ) -> Result<(), opendal::Error> {
            fn gen_data(seed: &Bytes, off: usize, target_len: usize) -> Buffer {
                let alls = vec![seed.slice(off..seed.len()), seed.slice(0..off)];
                Buffer::from(alls).slice(0..target_len)
            }

            fn check_data(
                seed: &Bytes,
                content: &Buffer,
                off: usize,
            ) -> Result<(), opendal::Error> {
                // only check head 10 bytes
                let part = std::cmp::min(10, seed.len());
                let mock_data = gen_data(seed, off, part).to_bytes();
                let part_content = content.slice(0..part).to_bytes();
                if part_content != mock_data {
                    return Err(opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "data mismatch for small get: expected {:?}, got {:?}",
                            mock_data, part_content
                        ),
                    ));
                }
                Ok(())
            }

            let off = (cur_key - start_key) as usize;
            match api {
                ApiInfo::SmallPut => {
                    let data = gen_data(&self.small_seed, off, self.small_seed.len());
                    self.blob.write(&cur_key.to_string(), data).await
                }
                ApiInfo::SmallGet => {
                    let content = self.blob.read(&cur_key.to_string()).await?;
                    check_data(&self.small_seed, &content, off)
                }
                ApiInfo::Put => {
                    let data = gen_data(&self.seed, off, self.seed.len());
                    self.blob.write(&cur_key.to_string(), data).await
                }
                ApiInfo::Get => {
                    let content = self.blob.read(&cur_key.to_string()).await?;
                    check_data(&self.seed, &content, off)
                }
                ApiInfo::List => {
                    let mut all = self.blob.lister("").await?;
                    while let Some(_) = all.next().await {}
                    Ok(())
                }
                ApiInfo::Head => {
                    let _ = self.blob.stat(&cur_key.to_string()).await;
                    Ok(())
                }
                ApiInfo::Delete => self.blob.delete(&cur_key.to_string()).await,
            }
        }
    }

    fn add_bar(bars: &MultiProgress, title: &str, count: u64) -> ProgressBar {
        let progress_style = ProgressStyle::with_template(
            "{prefix:.bold} count: {pos}/{len} [{wide_bar}] {speed:>10} {eta:>10} {used:>10}",
        )
        .unwrap_or_else(|e| {
            eprintln!("Failed to create progress style: {}", e);
            ProgressStyle::default_bar()
        })
        .progress_chars("=> ");
        let pb = ProgressBar::new(count)
            .with_style(progress_style.clone())
            .with_prefix(title.to_string())
            .with_finish(ProgressFinish::WithMessage("done".into()));
        bars.add(pb)
    }

    pub(crate) async fn testsing(blob: Operator, opts: ObjBenchOpts) {
        println!("Start Performance Testing ...");
        let mut seed = vec![0u8; (opts.block_size << 10) as usize];
        rand::thread_rng().fill_bytes(&mut seed);
        let mut small_seed = vec![0u8; (opts.small_object_size << 10) as usize];
        rand::thread_rng().fill_bytes(&mut small_seed);
        let bench = BenchmarkObj {
            bars: MultiProgress::new(),
            blob,
            opts: Arc::new(opts),
            seed: Bytes::from(seed),
            small_seed: Bytes::from(small_seed),
        };
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec!["ITEM", "VALUE", "COST"]);
        let cases = vec![
            ApiInfo::SmallPut,
            ApiInfo::SmallGet,
            ApiInfo::Put,
            ApiInfo::Get,
            ApiInfo::List,
            ApiInfo::Head,
            ApiInfo::Delete,
        ];
        for case in cases {
            let result = bench.run(case.clone()).await;
            table.add_row(result);
        }

        println!(
            "Benchmark finished! block-size: {} KiB, big-object-total-size: {} MiB, small-object-size: {} KiB, small-objects: {}, big-objects: {}, threads: {}",
            bench.opts.block_size,
            bench.opts.big_object_size,
            bench.opts.small_object_size,
            bench.opts.small_objects,
            (bench.opts.big_object_size << 10) / bench.opts.block_size,
            bench.opts.threads
        );
        println!("{table}");
    }
}

/// First, functional testing will be performed on the interface of object storage, and the following are test cases:
/// 1. Create bucket
/// 2. Upload object
/// 3. Download Object
/// 4. Download non-existent objects
/// 5. Get some contents of the object
/// 6. Get object meta information
/// 7. Delete an object
/// 8. Delete non-existent objects
/// 9. List objects
/// 10. Upload large objects
/// 11. Upload empty object
/// 12. Upload in blocks
/// 13. Change the file owner and group to which it belongs (requires root permission to run)
/// 14. Change file permissions
/// 15. Change the mtime of the file (last modified time)
///
/// Then perform performance testing:
/// 1. Upload --small-objects --small-object-size objects in concurrently with --threads
/// 2. Download the object uploaded in Step 1 and check the content
/// 3. Split the --big-object-size object according to the size of --block-size and upload it with --threads concurrency.
/// 4. Download the objects uploaded in Step 3 and check the contents, and then clean up all objects uploaded to the object store in Step 3
/// 5. List all objects in the object store 100 times with --threads concurrency
/// 6. Get meta information of all objects uploaded in step 1 with --threads concurrency
/// 7. Change the mtime of all objects uploaded in step 1 with --threads concurrency (last modified time)
/// 8. Change permissions for all objects uploaded in step 1 with --threads concurrency
/// 9. Change the owner and group of all objects uploaded in Step 1 with --threads concurrency (requires root permission to run)
/// 10. Delete all objects uploaded in step 1 with --threads concurrency
///
/// Finally clean the test files.
pub async fn juice_obj_bench(opts: ObjBenchOpts) -> Result<()> {
    assert!(opts.threads > 0, "threads should not be set to zero");
    assert!(
        opts.small_objects > 0,
        "Small objects count must be greater than 0"
    );
    assert!(opts.block_size > 0, "Block size must be greater than 0");
    assert!(
        opts.big_object_size > 0,
        "Big object size must be greater than 0"
    );
    assert!(
        opts.small_object_size > 0,
        "Small object size must be greater than 0"
    );
    info!("Running JuiceFS benchmark...");

    let root_path = format!("__juicefs_benchmark_{}__/", Local::now().timestamp_millis());
    let blob = new_operator(
        opts.storage.clone(),
        &opts.bucket,
        &opts.endpoint,
        opts.access_key.as_deref().unwrap_or(""),
        opts.secret_key.as_deref().unwrap_or(""),
        Some(&root_path),
    )
    .whatever_context("Failed to create operator")?;

    if !opts.skip_functional_tests {
        functional::testsing(blob.clone()).await;
    }
    performance::testsing(blob.clone(), opts).await;
    blob.delete(&root_path).await.ok(); //nolint:errcheck
    Ok(())
}
