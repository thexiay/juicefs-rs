use criterion::{criterion_group, criterion_main, Criterion};
use juice_storage::api::{new_chunk_store, ChunkStore, Config, SliceReader, SliceWriter};
use opendal::{services::Memory, Buffer, Operator};
use tempfile::tempdir;
use tokio::runtime::Runtime;

fn bench_cache_read(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let operator = Operator::new(Memory::default().root("/tmp"))
        .unwrap()
        .finish();
    let mut config = Config::default();
    config.max_block_size = 4 << 20;
    config.cache_dirs = vec![dir.path().to_path_buf()];

    let rt = Runtime::new().unwrap();
    let store = rt.block_on(async {
        let store = new_chunk_store(config, operator).unwrap();
        let mut writer = store.new_writer(1);
        writer
            .write_all_at(Buffer::from(vec![0; 1024]), 0)
            .expect("write fail");
        writer.finish().await.expect("write fail");
        store
    });

    c.bench_function("cache read", |bencher| {
        bencher.to_async(&rt).iter(|| async {
            let reader = store.new_reader(1, 1024);
            reader.read_all_at(0, 1024).await
        });
    });
}

#[warn(dead_code)]
fn bench_uncache_read(c: &mut Criterion) {}

criterion_group!(benches, bench_cache_read);
criterion_main!(benches);
