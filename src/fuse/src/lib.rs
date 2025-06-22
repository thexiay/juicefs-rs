#![feature(duration_constructors)]
#![feature(async_closure)]
#![feature(coroutines)]

mod fs;
mod fuse_kernel;
#[cfg(target_os = "linux")]
mod fs_linux;

pub use fs::JuiceFs;