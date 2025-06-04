mod format;

use clap::Parser;
use format::FormatOpts;
pub use format::juice_format;

#[derive(Parser, Debug)]
pub enum AdminCommands {
    Format(FormatOpts),
    Config { meta_url: String },
}
