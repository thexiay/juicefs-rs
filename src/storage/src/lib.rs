#![feature(const_trait_impl)]
#![feature(async_closure)]
#![feature(async_fn_traits)]
#![feature(let_chains)]

pub mod api;
pub mod error;

mod cache;
mod cached_store;
mod pre_fetcher;
mod single_flight;
mod compress;
mod buffer;
mod uploader;
mod utils;
