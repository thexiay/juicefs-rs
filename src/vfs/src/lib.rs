#![feature(let_chains)]
#![feature(coroutines)]
#![feature(gen_blocks)]
#![feature(linked_list_retain)]
#![feature(assert_matches)]
#![feature(once_cell_try_insert)]
#![feature(duration_constructors)]
#![feature(extend_one)]

mod reader;
mod writer;
mod error;
mod arena;
mod frange;
mod vfs;
mod handle;
mod internal;
pub mod config;

pub use vfs::Vfs;

#[cfg(test)]
mod vfs_test;