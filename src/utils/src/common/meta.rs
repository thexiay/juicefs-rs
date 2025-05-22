use std::{num::ParseIntError, str::FromStr};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq, ValueEnum)]
pub enum StorageType {
    #[default]
    Mem,
    File,
    Cos,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub enum Shards {
    #[default]
    None,
    Shard(u32),
}

#[derive(Snafu, Debug)]
pub enum ShardsParseError {
    #[snafu(display("Exceeding the maximum support shards limit: 256"))]
    Overflow,
    #[snafu(display("Failed to parse shards num `{source}`: {number}"))]
    NumberParse {
        source: ParseIntError,
        number: String,
    },
}

impl FromStr for Shards {
    type Err = ShardsParseError;

    fn from_str(shards: &str) -> Result<Self, Self::Err> {
        let num = shards
            .parse::<u32>()
            .context(NumberParseSnafu { number: shards })?;
        match num {
            0 => Ok(Shards::None),
            1..=256 => Ok(Shards::Shard(num)),
            _ => Err(ShardsParseError::Overflow),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq, ValueEnum)]
pub enum AtimeMode {
    // Set only when the file is created and SetAttr is actively called. Accessing and modifying files 
    // does not affect the atime value. Considering that updating atime requires running additional transactions 
    // and has an impact on performance, it is turned off by default.
    #[default]
    NoAtime,
    // Update only when mtime (file content modification time) or ctime (file metadata modification time) 
    // is newer than atime, or when atime has not been updated for more than 24 hours
    RelAtime,
    // Continuous update of atime
    StrictAtime,
}
