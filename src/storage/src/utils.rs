use std::path::Path;

use nix::sys::{stat::stat, statfs::statfs};
use tracing::warn;


pub fn disk_usage(path: &Path) -> (u64, u64, u64, u64) {
    statfs(path).map(|stat| {
        (
            stat.blocks() * stat.block_size() as u64,
            stat.blocks_available() * stat.block_size() as u64,
            stat.files(),
            stat.files_free(),
        )
    }).unwrap_or_else(|e| {
        warn!("statfs {:?}: {}", path, e);
        (1, 1, 1, 1)
    })
}

pub fn is_in_root_volumn(path: &Path) -> bool {
    let dstat = match stat(path) {
        Ok(stat) => stat,
        Err(e) => {
            warn!("statfs {:?}: {}", path, e);
            return false;
        }
    };
    let rstat = match stat(Path::new("/")) {
        Ok(stat) => stat,
        Err(e) => {
            warn!("statfs /: {}", e);
            return false;
        }
    };
    
    dstat.st_dev == rstat.st_dev
}