use std::{collections::HashMap, sync::Mutex};

pub struct FreeID {
	next: u64,
	maxid: u64,
}

pub struct Bar {
	total: i64,
	// todo: *mpb.Bar
}

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

pub fn align_4k(length: u64) -> u64 {
	if length == 0 {
		return 1 << 12;
	}
	((length - 1) >> 12 + 1) << 12
}
