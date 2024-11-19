use std::{collections::HashMap, sync::Mutex, time::Duration};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::{api::{Attr, ModeMask}, context::{Gid, Uid, UserExt}};

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
	pub plock_record: PlockRecord
}

pub struct FLockItem {
	pub owner_key: OwnerKey,
	pub r#type: String,
}

pub fn align_4k(length: u64) -> i64 {
	if length == 0 {
		return 1 << 12;
	}
	(((length - 1) >> 12 + 1) << 12) as i64
}

pub fn access_mode(attr: &Attr, uid: &Uid, gids: &Vec<Gid>) -> ModeMask {
	if uid.is_root() {
		return ModeMask::FULL;
	}
	let mode = attr.mode;
	if *uid == attr.uid {
		return ModeMask::FULL & ModeMask::from_bits_truncate((mode >> 6) as u8);
	}
	for gid in gids {
		if *gid == attr.gid {
			return ModeMask::FULL & ModeMask::from_bits_truncate((mode >> 3) as u8);
		}
	}
	return ModeMask::FULL & ModeMask::from_bits_truncate(mode as u8);
}

pub async fn sleep_with_jitter(d: Duration) {
	let j = (d / 20).as_millis() as i64;
	let num = {
		let mut rng = rand::thread_rng();
		rng.gen_range((-j)..j)
	};
	sleep(Duration::from_millis((d.as_millis() as i64 + num) as u64)).await;
}