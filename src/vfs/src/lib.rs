#![feature(let_chains)]
#![feature(coroutines)]
#![feature(gen_blocks)]
#![feature(linked_list_retain)]
#![feature(assert_matches)]
#![feature(once_cell_try_insert)]
#![feature(duration_constructors)]

mod reader;
mod writer;
mod error;
mod arena;
mod config;
mod frange;