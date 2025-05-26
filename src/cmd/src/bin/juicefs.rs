use clap::Parser;
use juice_cmd::{cmd, CliOpts};

fn main() {
    let cli = CliOpts::parse();
    cmd(cli);
}
