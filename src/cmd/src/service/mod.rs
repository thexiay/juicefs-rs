use clap::{arg, Parser};
use mount::MountOpts;

mod mount;
pub use mount::juice_mount;

#[derive(Parser, Debug)]
pub enum ServiceCommands {
    Mount(MountOpts),
}