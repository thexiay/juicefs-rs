use std::sync::mpsc::Sender;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use redis::Client as RedisClient;

use crate::acl::{Entry, Rule};
use crate::api::{Attr, Ino, Session, Slice};
use crate::config::Format;
use crate::error::Result;
use crate::base::{Cchunk, CommonMeta, DirStat, Engine, PendingFileScan, PendingSliceScan, TrashSliceScan};
use crate::quota::Quota;
use crate::utils::Bar;
use std::collections::HashMap;

use std::time::SystemTime;


/*
	Node:       i$inode -> Attribute{type,mode,uid,gid,atime,mtime,ctime,nlink,length,rdev}
	Dir:        d$inode -> {name -> {inode,type}}
	Parent:     p$inode -> {parent -> count} // for hard links
	File:       c$inode_$indx -> [Slice{pos,id,length,off,len}]
	Symlink:    s$inode -> target
	Xattr:      x$inode -> {name -> value}
	Flock:      lockf$inode -> { $sid_$owner -> ltype }
	POSIX lock: lockp$inode -> { $sid_$owner -> Plock(pid,ltype,start,end) }
	Sessions:   sessions -> [ $sid -> heartbeat ]
	sustained:  session$sid -> [$inode]
	locked:     locked$sid -> { lockf$inode or lockp$inode }

	Removed files: delfiles -> [$inode:$length -> seconds]
	detached nodes: detachedNodes -> [$inode -> seconds]
	Slices refs: k$sliceId_$size -> refcount

	Dir data length:   dirDataLength -> { $inode -> length }
	Dir used space:    dirUsedSpace -> { $inode -> usedSpace }
	Dir used inodes:   dirUsedInodes -> { $inode -> usedInodes }
	Quota:             dirQuota -> { $inode -> {maxSpace, maxInodes} }
	Quota used space:  dirQuotaUsedSpace -> { $inode -> usedSpace }
	Quota used inodes: dirQuotaUsedInodes -> { $inode -> usedInodes }

	Redis features:
	  Sorted Set: 1.2+
	  Hash Set: 4.0+
	  Transaction: 2.2+
	  Scripting: 2.6+
	  Scan: 2.8+
*/
pub struct RedisEngine {
	rdb: RedisClient,
	prefix: String,
	sha_lookup: String,  // The SHA returned by Redis for the loaded `scriptLookup`
	sha_resolve: String,  // The SHA returned by Redis for the loaded `scriptResolve`
}

#[async_trait]
impl Engine for RedisEngine {
	async fn get_counter(&self, name: String) -> Result<i64> {
		unimplemented!()
	}

	async fn incr_counter(&self, name: String, value: i64) -> Result<i64> {
		unimplemented!()
	}

	async fn set_if_small(&self, name: String, value: i64, diff: i64) -> Result<bool> {
		unimplemented!()
	}

	async fn update_stats(&self, space: i64, inodes: i64) {
		unimplemented!()
	}

	async fn flush_stats(&self) {
		unimplemented!()
	}

	async fn do_load(&self) -> Result<Vec<u8>> {
		unimplemented!()
	}

	async fn do_new_session(&self, sinfo: &[u8], update: bool) -> Result<()> {
		unimplemented!()
	}

	async fn do_refresh_session(&self) -> Result<()> {
		unimplemented!()
	}

	async fn do_find_stale_sessions(&self, limit: i32) -> Result<Vec<u64>> {
		unimplemented!()
	}

	async fn do_clean_stale_session(&self, sid: u64) -> Result<()> {
		unimplemented!()
	}

	async fn do_init(&self, format: &Format, force: bool) -> Result<()> {
		unimplemented!()
	}

	async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()> {
		unimplemented!()
	}

	async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()> {
		unimplemented!()
	}

	async fn do_find_deleted_files(&self, ts: i64, limit: i32) -> Result<HashMap<Ino, u64>> {
		unimplemented!()
	}

	async fn do_delete_file_data(&self, inode: Ino, length: u64) {
		unimplemented!()
	}

	async fn do_cleanup_slices(&self) {
		unimplemented!()
	}

	async fn do_cleanup_delayed_slices(&self, edge: i64) -> Result<i32> {
		unimplemented!()
	}

	async fn do_delete_slice(&self, id: u64, size: u32) -> Result<()> {
		unimplemented!()
	}

	async fn do_clone_entry(
		&self,
		src_ino: Ino,
		parent: Ino,
		name: &str,
		ino: Ino,
		attr: &Attr,
		cmode: u8,
		cumask: u16,
		top: bool,
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_attach_dir_node(&self, parent: Ino, dst_ino: Ino, name: &str) -> Result<()> {
		unimplemented!()
	}

	async fn do_find_detached_nodes(&self, t: SystemTime) -> Vec<Ino> {
		unimplemented!()
	}

	async fn do_cleanup_detached_node(&self, detached_node: Ino) -> Result<()> {
		unimplemented!()
	}

	async fn do_get_quota(&self, inode: Ino) -> Result<Quota> {
		unimplemented!()
	}

	async fn do_del_quota(&self, inode: Ino) -> Result<()> {
		unimplemented!()
	}

	async fn do_load_quotas(&self) -> Result<HashMap<Ino, Quota>> {
		unimplemented!()
	}

	async fn do_flush_quotas(&self, quotas: HashMap<Ino, Quota>) -> Result<()> {
		unimplemented!()
	}

	async fn do_get_attr(&self, inode: Ino, attr: &Attr) -> Result<()> {
		unimplemented!()
	}

	async fn do_set_attr(&self, inode: Ino, set: u16, sugidclearmode: u8, attr: &Attr) -> Result<()> {
		unimplemented!()
	}

	async fn do_lookup(&self, parent: Ino, name: &str, inode: &Ino, attr: &Attr) -> Result<()> {
		unimplemented!()
	}

	async fn do_mknod(
		&self, 
		parent: Ino,
		name: &str,
		_type: u8,
		mode: u16,
		cumask: u16,
		path: &str,
		inode: &Ino,
		attr: &Attr,
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_link(&self, inode: Ino, parent: Ino, name: &str, attr: &Attr) -> Result<()> {
		unimplemented!()
	}

	async fn do_unlink(&self, parent: Ino, name: &str, attr: &Attr, skip_check_trash: bool) -> Result<()> {
		unimplemented!()
	}

	async fn do_rmdir(&self, parent: Ino, name: &str, inode: &Ino, skip_check_trash: bool) -> Result<()> {
		unimplemented!()
	}

	async fn do_readlink(&self, inode: Ino, noatime: bool) -> Result<(i64, Vec<u8>)> {
		unimplemented!()
	}

	async fn do_readdir(&self, inode: Ino, plus: u8, entries: &mut Vec<Entry>, limit: i32) -> Result<()> {
		unimplemented!()
	}

	async fn do_rename(
		&self, 
		parent_src: Ino,
		name_src: String,
		parent_dst: Ino,
		name_dst: String,
		flags: u32,
		inode: &mut Ino,
		tinode: &mut Ino,
		attr: &mut Attr,
		tattr: &mut Attr,
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_set_xattr(&self, inode: Ino, name: String, value: Bytes, flags: u32) -> Result<()> {
		unimplemented!()
	}

	async fn do_remove_xattr(&self, inode: Ino, name: String) -> Result<()> {
		unimplemented!()
	}

	async fn do_repair(&self, inode: Ino, attr: &mut Attr) -> Result<()> {
		unimplemented!()
	}

	async fn do_touch_atime(&self, inode: Ino, attr: Attr, ts: SystemTime) -> Result<bool> {
		unimplemented!()
	}

	async fn do_read(&self, inode: Ino, indx: u32) -> Result<Vec<Slice>> {
		unimplemented!()
	}

	async fn do_write(
		&self, 
		inode: Ino,
		indx: u32,
		off: u32,
		slice: Slice,
		mtime: SystemTime,
		num_slices: &mut i32,
		delta: &mut DirStat,
		attr: &mut Attr,
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_truncate(
		&self, 
		inode: Ino,
		flags: u8,
		length: u64,
		delta: &mut DirStat,
		attr: &mut Attr,
		skip_perm_check: bool,
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_fallocate(
		&self, 
		inode: Ino,
		mode: u8,
		off: u64,
		size: u64,
		delta: &mut DirStat,
		attr: &mut Attr,
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_compact_chunk(
		&self, 
		inode: Ino,
		indx: u32,
		origin: &[u8],
		ss: &mut [Slice],
		skipped: i32,
		pos: u32,
		id: u64,
		size: u32,
		delayed: &[u8],
	) -> Result<()> {
		unimplemented!()
	}

	async fn do_get_parents(&self, inode: Ino) -> HashMap<Ino, i32> {
		unimplemented!()
	}

	async fn do_update_dir_stat(&self, batch: HashMap<Ino, DirStat>) -> Result<()> {
		unimplemented!()
	}

	async fn do_get_dir_stat(&self, ino: Ino, try_sync: bool) -> Result<DirStat> {
		unimplemented!()
	}

	async fn do_sync_dir_stat(&self, ino: Ino) -> Result<DirStat> {
		unimplemented!()
	}

	async fn scan_trash_slices(&self, trash_slice_scan: TrashSliceScan) -> Result<()> {
		unimplemented!()
	}

	async fn scan_pending_slices(&self, pending_slice_scan: PendingSliceScan) -> Result<()> {
		unimplemented!()
	}

	async fn scan_pending_files(&self, pending_file_scan: PendingFileScan) -> Result<()> {
		unimplemented!()
	}

	async fn get_session(&self, sid: u64, detail: bool) -> Result<Session> {
		unimplemented!()
	}

	async fn do_set_facl(&self, ino: Ino, acl_type: u8, rule: &Rule) -> Result<()> {
		unimplemented!()
	}

	async fn do_get_facl(&self, ino: Ino, acl_type: u8, acl_id: u32, rule: &Rule) -> Result<()> {
		unimplemented!()
	}

	async fn cache_acls(&self) -> Result<()> {
		unimplemented!()
	}
}