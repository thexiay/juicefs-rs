use std::str::FromStr;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, ValueEnum)]
pub enum CompressArgo {
    ZSTD,
    Snappy,
    LZ4,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq, ValueEnum)]
pub enum EncryptAlgo {
    #[default]
    Aes256GcmRsa,
    ChaCha20Rsa,
}

#[derive(Debug, Clone, Eq, PartialEq)]

pub struct UploadHourRange {
    pub start: u32,
    pub end: u32,
}

#[derive(Snafu, Debug)]
pub enum UploadHourRangeParseError {
    #[snafu(display("Illegal time limit, only support 0~23: {hour}"))]
    InvalidHourRange { hour: String },
    #[snafu(display("Illegal time range, should not equal."))]
    InvalidRange,
    #[snafu(display("Illegal str, should format in '<start hour>,<end hour>', but in {f}"))]
    InvalidFormat { f: String },
}

impl FromStr for UploadHourRange {
    type Err = UploadHourRangeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return InvalidFormatSnafu { f: s.to_string() }.fail();
        }

        let start = parts[0].trim()
            .parse::<u32>()
            .map_err(|_| InvalidHourRangeSnafu { hour: parts[0] }.build())?;
        if !(0..=23).contains(&start) {
            return InvalidHourRangeSnafu { hour: parts[0] }.fail();
        }
        let end = parts[1].trim()
            .parse::<u32>()
            .map_err(|_| InvalidHourRangeSnafu { hour: parts[1] }.build())?;
        if !(0..=23).contains(&end) {
            return InvalidHourRangeSnafu { hour: parts[1] }.fail();
        }

        if start == end {
            return InvalidRangeSnafu.fail();
        }
        Ok(UploadHourRange { start, end })
    }
}

impl ToString for UploadHourRange {
    fn to_string(&self) -> String {
        format!("{},{}", self.start, self.end)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, ValueEnum)]
pub enum ChecksumLevel {
    /// Disable checksum verification, if local cache data is tampered, bad data will be read
    None,
    /// Perform verification when reading the full block, use this for sequential read scenarios
    #[default]
    Full,
    /// Perform verification on parts that's fully included within the read range, use this for random read scenarios
    Shrink,
    /// Perform verification on parts that fully include the read range, this causes read amplifications and is only
    /// used for random read scenarios demanding absolute data integrity.
    Extend,
}

#[derive(Debug, Clone, Default, PartialEq, ValueEnum)]
pub enum CacheEviction {
    #[default]
    None,
    Random,
}
