use clap::Parser;
use juice_cmd::{CliOpts, cmd};

fn main() {
    let cli = CliOpts::parse();
    cmd(cli);
}
