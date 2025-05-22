use clap::Parser;
use juice_cmd::{cmd, CliOpts};
use tracing::error;

fn main() {
    let cli = CliOpts::parse();
    cmd(cli);
}