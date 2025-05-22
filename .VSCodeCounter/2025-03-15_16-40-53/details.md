# Details

Date : 2025-03-15 16:40:53

Directory /data/gitcode/juicefs-rs

Total : 61 files,  17179 codes, 828 comments, 1710 blanks, all 19717 lines

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [Cargo.lock](/Cargo.lock) | TOML | 2,401 | 0 | 277 | 2,678 |
| [Cargo.toml](/Cargo.toml) | TOML | 57 | 0 | 3 | 60 |
| [src/lib.rs](/src/lib.rs) | Rust | 12 | 0 | 3 | 15 |
| [src/meta/Cargo.toml](/src/meta/Cargo.toml) | TOML | 42 | 0 | 3 | 45 |
| [src/meta/src/acl.rs](/src/meta/src/acl.rs) | Rust | 170 | 6 | 29 | 205 |
| [src/meta/src/api.rs](/src/meta/src/api.rs) | Rust | 452 | 209 | 97 | 758 |
| [src/meta/src/base.rs](/src/meta/src/base.rs) | Rust | 2,670 | 195 | 158 | 3,023 |
| [src/meta/src/config.rs](/src/meta/src/config.rs) | Rust | 174 | 12 | 11 | 197 |
| [src/meta/src/context.rs](/src/meta/src/context.rs) | Rust | 59 | 7 | 14 | 80 |
| [src/meta/src/error.rs](/src/meta/src/error.rs) | Rust | 296 | 7 | 32 | 335 |
| [src/meta/src/lib.rs](/src/meta/src/lib.rs) | Rust | 23 | 0 | 4 | 27 |
| [src/meta/src/openfile.rs](/src/meta/src/openfile.rs) | Rust | 131 | 6 | 23 | 160 |
| [src/meta/src/quota.rs](/src/meta/src/quota.rs) | Rust | 472 | 20 | 49 | 541 |
| [src/meta/src/random\_test.rs](/src/meta/src/random_test.rs) | Rust | 3 | 0 | 2 | 5 |
| [src/meta/src/rds/check.rs](/src/meta/src/rds/check.rs) | Rust | 84 | 0 | 11 | 95 |
| [src/meta/src/rds/conn.rs](/src/meta/src/rds/conn.rs) | Rust | 63 | 3 | 12 | 78 |
| [src/meta/src/rds/mod.rs](/src/meta/src/rds/mod.rs) | Rust | 6 | 0 | 1 | 7 |
| [src/meta/src/rds/redis.rs](/src/meta/src/rds/redis.rs) | Rust | 3,031 | 104 | 210 | 3,345 |
| [src/meta/src/rds/test.rs](/src/meta/src/rds/test.rs) | Rust | 146 | 13 | 38 | 197 |
| [src/meta/src/session.rs](/src/meta/src/session.rs) | Rust | 2 | 0 | 3 | 5 |
| [src/meta/src/slice.rs](/src/meta/src/slice.rs) | Rust | 285 | 11 | 30 | 326 |
| [src/meta/src/test.rs](/src/meta/src/test.rs) | Rust | 751 | 27 | 61 | 839 |
| [src/meta/src/utils.rs](/src/meta/src/utils.rs) | Rust | 82 | 7 | 16 | 105 |
| [src/storage/Cargo.toml](/src/storage/Cargo.toml) | TOML | 40 | 0 | 4 | 44 |
| [src/storage/benches/bench\_cache\_store\_read.rs](/src/storage/benches/bench_cache_store_read.rs) | Rust | 34 | 0 | 6 | 40 |
| [src/storage/src/api.rs](/src/storage/src/api.rs) | Rust | 55 | 24 | 21 | 100 |
| [src/storage/src/buffer.rs](/src/storage/src/buffer.rs) | Rust | 215 | 0 | 22 | 237 |
| [src/storage/src/cache/disk/cache.rs](/src/storage/src/cache/disk/cache.rs) | Rust | 1,683 | 79 | 115 | 1,877 |
| [src/storage/src/cache/disk/mod.rs](/src/storage/src/cache/disk/mod.rs) | Rust | 3 | 0 | 1 | 4 |
| [src/storage/src/cache/mem.rs](/src/storage/src/cache/mem.rs) | Rust | 0 | 0 | 2 | 2 |
| [src/storage/src/cache/mod.rs](/src/storage/src/cache/mod.rs) | Rust | 137 | 18 | 27 | 182 |
| [src/storage/src/cached\_store.rs](/src/storage/src/cached_store.rs) | Rust | 756 | 59 | 93 | 908 |
| [src/storage/src/compress.rs](/src/storage/src/compress.rs) | Rust | 26 | 3 | 9 | 38 |
| [src/storage/src/error.rs](/src/storage/src/error.rs) | Rust | 92 | 2 | 15 | 109 |
| [src/storage/src/lib.rs](/src/storage/src/lib.rs) | Rust | 16 | 0 | 3 | 19 |
| [src/storage/src/pre\_fetcher.rs](/src/storage/src/pre_fetcher.rs) | Rust | 6 | 0 | 2 | 8 |
| [src/storage/src/single\_flight.rs](/src/storage/src/single_flight.rs) | Rust | 132 | 0 | 13 | 145 |
| [src/storage/src/uploader.rs](/src/storage/src/uploader.rs) | Rust | 33 | 0 | 7 | 40 |
| [src/storage/src/utils.rs](/src/storage/src/utils.rs) | Rust | 35 | 0 | 5 | 40 |
| [src/test/Cargo.toml](/src/test/Cargo.toml) | TOML | 13 | 0 | 2 | 15 |
| [src/test/src/bin/test\_async.rs](/src/test/src/bin/test_async.rs) | Rust | 50 | 7 | 9 | 66 |
| [src/test/src/bin/test\_deref.rs](/src/test/src/bin/test_deref.rs) | Rust | 24 | 1 | 7 | 32 |
| [src/test/src/bin/test\_drop.rs](/src/test/src/bin/test_drop.rs) | Rust | 27 | 0 | 6 | 33 |
| [src/test/src/bin/test\_redis.rs](/src/test/src/bin/test_redis.rs) | Rust | 8 | 0 | 3 | 11 |
| [src/test/src/bin/test\_time.rs](/src/test/src/bin/test_time.rs) | Rust | 19 | 0 | 4 | 23 |
| [src/test/src/main.rs](/src/test/src/main.rs) | Rust | 38 | 8 | 9 | 55 |
| [src/utils/Cargo.toml](/src/utils/Cargo.toml) | TOML | 11 | 0 | 3 | 14 |
| [src/utils/src/lib.rs](/src/utils/src/lib.rs) | Rust | 2 | 0 | 0 | 2 |
| [src/utils/src/process.rs](/src/utils/src/process.rs) | Rust | 4 | 0 | 1 | 5 |
| [src/utils/src/runtime.rs](/src/utils/src/runtime.rs) | Rust | 30 | 0 | 8 | 38 |
| [src/vfs/Cargo.toml](/src/vfs/Cargo.toml) | TOML | 24 | 0 | 2 | 26 |
| [src/vfs/src/arena.rs](/src/vfs/src/arena.rs) | Rust | 77 | 0 | 13 | 90 |
| [src/vfs/src/config.rs](/src/vfs/src/config.rs) | Rust | 63 | 0 | 6 | 69 |
| [src/vfs/src/error.rs](/src/vfs/src/error.rs) | Rust | 130 | 0 | 14 | 144 |
| [src/vfs/src/frange.rs](/src/vfs/src/frange.rs) | Rust | 30 | 0 | 7 | 37 |
| [src/vfs/src/handle.rs](/src/vfs/src/handle.rs) | Rust | 139 | 0 | 20 | 159 |
| [src/vfs/src/internal.rs](/src/vfs/src/internal.rs) | Rust | 25 | 0 | 5 | 30 |
| [src/vfs/src/lib.rs](/src/vfs/src/lib.rs) | Rust | 16 | 0 | 1 | 17 |
| [src/vfs/src/reader.rs](/src/vfs/src/reader.rs) | Rust | 838 | 0 | 66 | 904 |
| [src/vfs/src/vfs.rs](/src/vfs/src/vfs.rs) | Rust | 345 | 0 | 39 | 384 |
| [src/vfs/src/writer.rs](/src/vfs/src/writer.rs) | Rust | 591 | 0 | 53 | 644 |

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)