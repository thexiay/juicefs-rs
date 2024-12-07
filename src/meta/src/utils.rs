use std::{collections::HashMap, sync::Mutex, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::{
    api::{Attr, ModeMask},
    context::{Gid, Uid, UserExt},
};

#[derive(Default)]
pub struct FreeID {
    pub next: u64,
    pub maxid: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlockRecord {
    pub r#type: u32,
    pub pid: u32,
    pub start: u64,
    pub end: u64,
}

pub struct OwnerKey {
    pub sid: u64,
    pub owner: u64,
}

pub struct PLockItem {
    pub owner_key: OwnerKey,
    pub plock_record: PlockRecord,
}

pub struct FLockItem {
    pub owner_key: OwnerKey,
    pub r#type: String,
}

pub enum DeleteFileOption {
    Deferred,
    Immediate {
        // if force, it will wait semaphore to be acquired
        // if not, it will skip this delete
        force: bool,
    },
}

pub fn align_4k(length: u64) -> i64 {
    if length == 0 {
        1 << 12
    } else {
        (((length - 1) >> 12 + 1) << 12) as i64
    }
}

pub fn access_mode(attr: &Attr, uid: &Uid, gids: &Vec<Gid>) -> ModeMask {
    if uid.is_root() {
        return ModeMask::FULL;
    }
    let mode = attr.mode;
    // user permission
    if *uid == attr.uid {
        return ModeMask::FULL & ModeMask::from_bits_truncate((mode >> 6) as u8);
    }
    // group permission
    for gid in gids {
        if *gid == attr.gid {
            return ModeMask::FULL & ModeMask::from_bits_truncate((mode >> 3) as u8);
        }
    }
    // other permission
    return ModeMask::FULL & ModeMask::from_bits_truncate(mode as u8);
}

/// With relative atime, only update atime if the previous atime is earlier than either the ctime or
/// mtime or if at least a day has passed since the last atime update.
pub fn relation_need_update(attr: &Attr, now: Duration) -> bool {
    attr.mtime > attr.atime
        || attr.ctime > attr.atime
        || now.as_nanos() - attr.atime > Duration::from_hours(24).as_nanos()
}

pub async fn sleep_with_jitter(d: Duration) {
    let j = (d / 20).as_millis() as i64;
    let num = {
        let mut rng = rand::thread_rng();
        rng.gen_range((-j)..j)
    };
    sleep(Duration::from_millis((d.as_millis() as i64 + num) as u64)).await;
}
