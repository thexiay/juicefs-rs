#![feature(const_trait_impl)]
#![feature(async_closure)]
#![feature(async_fn_traits)]
#![feature(let_chains)]
#![feature(never_type)]
#![feature(test)]

pub mod api;
pub mod error;

mod buffer;
mod cache;
mod cached_store;
mod compress;
mod pre_fetcher;
mod single_flight;
mod uploader;
mod utils;

pub use cached_store::CacheType;
pub use cached_store::Config;