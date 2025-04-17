// https://github.com/libfuse/libfuse/blob/master/include/fuse_kernel.h#L364
pub const FOPEN_DIRECT_IO: u32 = 1 << 0;
pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;
pub const FOPEN_NONSEEKABLE: u32 = 1 << 2;
pub const FOPEN_CACHE_DIR: u32 = 1 << 3;
pub const FOPEN_STREAM: u32 = 1 << 4;
