use std::{
    collections::HashMap, fs::File, io::{BufRead, BufReader}, ops::Range, os::unix::fs::MetadataExt, path::{Path, PathBuf}
};

use comfy_table::Color;
use juice_meta::api::ROOT_INODE;
use snafu::{ResultExt, whatever};
use tracing::warn;

use crate::Result;

pub fn find_mount_point(path: &Path) -> Result<PathBuf> {
    for p in path.ancestors() {
        let metadata = std::fs::metadata(path).whatever_context("Failed to get metadata")?;
        let inode = metadata.ino();
        if inode == ROOT_INODE {
            return Ok(p.to_path_buf());
        }
    }
    whatever!("{} is not inside JuiceFS", path.display())
}

pub fn read_stats(path: &Path) -> HashMap<String, f64> {
    let mut stats = HashMap::new();
    match File::open(path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                if let Ok(ref line) = line
                    && let Some((key, value)) = line.split_once(' ')
                    && let Ok(value) = value.trim().parse::<f64>()
                {
                    stats.insert(key.trim().to_string(), value);
                } else {
                    warn!(
                        "Failed to parse line in stats file {}: {:?}",
                        path.display(),
                        line
                    );
                }
            }
        }
        Err(e) => {
            warn!("Failed to open stats file {}: {}", path.display(), e);
        }
    }
    stats
}


pub fn get_value_color(value: f64, range: Range<f64>) -> Color {
    if value > range.end {
        Color::Green
    } else if value > range.start {
        Color::Yellow
    } else {
        Color::Red
    }
}

pub fn get_cost_color(value: f64, range: Range<f64>) -> Color {
    if value < range.start {
        Color::Green
    } else if value > range.start {
        Color::Yellow
    } else {
        Color::Red
    }
}