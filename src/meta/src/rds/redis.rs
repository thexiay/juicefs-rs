use async_trait::async_trait;
use bytes::Bytes;
use r2d2::{Pool, PooledConnection};
use redis::{
    Client as RedisClient, Commands, Connection, ConnectionLike, Pipeline, RedisError, RedisResult,
    ScanOptions, Script, ToRedisArgs,
};
use regex::Regex;
use snafu::{ensure, ResultExt};
use std::hash::{DefaultHasher, Hasher};
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use tokio::time::{self, sleep, Instant};
use tracing::{debug, error, warn};

use crate::acl::{Entry, Rule};
use crate::api::{Attr, INodeType, Ino, Meta, Session, Slice, Slices, CHUNK_SIZE, TRASH_INODE};
use crate::base::{
    Cchunk, CommonMeta, DirStat, Engine, PendingFileScan, PendingSliceScan, SessionState, TrashSliceScan, N_LOCK
};
use crate::config::Format;
use crate::error::{ConnectionSnafu, EmptyKeySnafu, MyError, RedisDetailSnafu, Result, SysSnafu};
use crate::quota::Quota;
use crate::utils::{self, Bar};
use std::collections::HashMap;

use std::time::{Duration, SystemTime};

pub const TXN_RETRY_LIMIT: u32 = 50;
pub const LOOKUP_SCRIPT_CODE: &str = r#"
    local buf = redis.call('HGET', KEYS[1], KEYS[2])
    if not buf then
        error("ENOENT")
    end
    local ino = struct.unpack(">I8", string.sub(buf, 2))
    -- double float has 52 significant bits
    if ino > 4503599627370495 then
        error("ENOTSUP")
    end
    return {ino, redis.call('GET', "i" .. string.format("%.f", ino))}
"#;
/*
    Key and Value map:
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

    Redis Action:
        
*/
// TODO: make conn async
pub struct RedisEngine {
    rdb: Pool<RedisClient>, // TODO: single connection or multiple connections?
    prefix: String,
    lookup_script: Script,  // lookup lua script
    resolve_script: Script, // reslove lua script
    state: Arc<SessionState>,
}

impl RedisEngine {
    pub fn new() -> Self {
        todo!()
    }
}

impl RedisEngine {
    fn should_retry(err: &RedisError) -> bool {
        !err.is_unrecoverable_error()
    }

    fn pool_conn(&self) -> Result<PooledConnection<RedisClient>> {
        self.rdb.get().context(ConnectionSnafu)
    }

    /// settings -> Option<String>
    fn settings(&self) -> String {
        format!("{}{}", self.prefix, "settings")
    }

    /// AllSessions:  {prefix}allSessions -> { sid }
    fn all_sessions(&self) -> String {
        format!("{}{}", self.prefix, "allSessions")
    }

    /// Sessions:   sessions -> [ $sid -> heartbeat ]
    fn session_infos(&self) -> String {
        format!("{}{}", self.prefix, "sessionInfos")
    }

    /// sustained:  session$sid -> [$inode]
    fn sustained(&self, sid: u64) -> String {
        format!("{}{}{}", self.prefix, "session", sid)
    }

    /// locked:     locked$sid -> { lockf$inode or lockp$inode }
    fn locked(&self, sid: u64) -> String {
        format!("{}{}{}", self.prefix, "locked", sid)
    }

    /// Removed files: delfiles -> [$inode:$length -> seconds]
    fn del_files(&self) -> String {
        format!("{}{}", self.prefix, "delfiles")
    }

	/// detached nodes: detachedNodes -> [$inode -> seconds]
    fn detached_nodes(&self) -> String {
        format!("{}{}", self.prefix, "detachedNodes")
    }

	/// Slices refs: k$sliceId_$size -> refcount
    fn slice_refs(&self) -> String {
        format!("{}{}", self.prefix, "sliceRefs")
    }

	/// Dir data length:   dirDataLength -> { $inode -> length }
    fn dir_data_length_key(&self) -> String {
        format!("{}{}", self.prefix, "dirDataLength")
    }

	/// Dir used space:    dirUsedSpace -> { $inode -> usedSpace }
    fn dir_used_space_key(&self) -> String {
        format!("{}{}", self.prefix, "dirUsedSpace")
    }

	/// Dir used inodes:   dirUsedInodes -> { $inode -> usedInodes }
    fn dir_used_inodes_key(&self) -> String {
        format!("{}{}", self.prefix, "dirUsedInodes")
    }

	/// Quota:             dirQuota -> { $inode -> {maxSpace, maxInodes} }
    fn dir_quota_key(&self) -> String {
        format!("{}{}", self.prefix, "dirQuota")
    }

	/// Quota used space:  dirQuotaUsedSpace -> { $inode -> usedSpace }
    fn dir_quota_used_space_key(&self) -> String {
        format!("{}{}", self.prefix, "dirQuotaUsedSpace")
    }

	/// Quota used inodes: dirQuotaUsedInodes -> { $inode -> usedInodes }
    fn dir_quota_used_inodes_key(&self) -> String {
        format!("{}{}", self.prefix, "dirQuotaUsedInodes")
    }

    /// Node: {prefix}i{inode} -> Attr{type,mode,uid,gid,atime,mtime,ctime,nlink,length,rdev}
    fn inode_key(&self, inode: Ino) -> String {
        format!("{}i{}", self.prefix, inode)
    }

    /// Dir: {prefix}d{inode} -> {name -> {inode,type}}
    fn dir_key(&self, inode: Ino) -> String {
        format!("{}d{}", self.prefix, inode)
    }
    
    /// Parent:     p$inode -> {parent -> count} // for hard links
    fn parent_key(&self, inode: Ino) -> String {
        format!("{}p{}", self.prefix, inode)
    }

    /// File:       c$inode_$indx -> [Slice{pos,id,length,off,len}]
    fn chunk_key(&self, inode: u64, indx: u64) -> String {
        format!("{}c{}_{}", self.prefix, inode, indx)
    }
   
    /// Symlink:    s$inode -> target
    fn sym_key(&self, inode: Ino) -> String {
        format!("{}s{}", self.prefix, inode)
    }

    /// Xattr:      x$inode -> {name -> value}
    fn xattr_key(&self, inode: Ino) -> String {
        format!("{}x{}", self.prefix, inode)
    }

    /// POSIX lock: lockp$inode -> { $sid_$owner -> Plock(pid,ltype,start,end) }
    fn lockp_key(&self, inode: Ino) -> String {
        format!("{}lockp{}", self.prefix, inode)
    }

    /// Flock:      lockf$inode -> { $sid_$owner -> ltype }
    fn lockf_key(&self, inode: Ino) -> String {
        format!("{}lockf{}", self.prefix, inode)
    }

    fn to_delete(inode: Ino, length: u64) -> String {
        format!("{}:{}", inode, length)
    }

    fn used_space_key(&self) -> String {
        format!("{}{}", self.prefix, "usedSpace")
    }

    fn total_inodes_key(&self) -> String {
        format!("{}{}", self.prefix, "totalInodes")
    }

    fn slice_key(id: u64, size: u32) -> String {
        format!("k{}_{}", id, size)
    }

    fn expire_time(&self) -> u64 {
        (SystemTime::now() + self.state.conf.heartbeat * 5)
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }

    // For now only deleted files
    async fn clean_legacies(&self) {}
}

impl RedisEngine {
    async fn txn_with_retry<
        C: ConnectionLike,
        K: ToRedisArgs,
        T,
        F: FnMut(&mut C, &mut Pipeline) -> RedisResult<Option<T>>,
    >(
        &self,
        conn: &mut C,
        keys: &[K],
        func: &mut F,
    ) -> Result<T> {
        ensure!(!self.state.conf.read_only, SysSnafu { code: libc::EROFS });
        ensure!(!keys.is_empty(), EmptyKeySnafu);

        for key in keys {
            let args = key.to_redis_args();
            if !args.is_empty() && args[0].starts_with(self.prefix.as_bytes()) {
                panic!(
                    "Invalid key {:?} not starts with prefix {}",
                    args, self.prefix
                );
            }
        }

        // Redis use optimistic locking, maybe txn fail, localnode use pessimistic lock to reduce conflicts in advanc
        let mut hasher = DefaultHasher::new();
        keys[0]
            .to_redis_args()
            .iter()
            .for_each(|arg| hasher.write(arg));
        let hash = hasher.finish();
        let _ = self.state.txn_locks[(hash % N_LOCK as u64) as usize].lock();

        let mut last_err = None;
        for i in 0..TXN_RETRY_LIMIT {
            ensure!(
                !self.state.canceled.load(Ordering::Relaxed),
                SysSnafu { code: libc::EINTR }
            );
            match redis::transaction(conn, keys, &mut *func) {
                Ok(res) => return Ok(res), // ignore return value
                Err(err) => {
                    if RedisEngine::should_retry(&err) {
                        debug!("Transaction failed, restart it (tried {}): {}", i + 1, err);
                        last_err.replace(err);
                        sleep(Duration::from_millis(i as u64)).await;
                        continue;
                    } else {
                        warn!(
                            "Transaction succeeded after {} tries ({:?}), keys: {:?}",
                            i + 1,
                            Instant::now().duration_since(Instant::now()),
                            keys.iter().map(|k| k.to_redis_args()).collect::<Vec<_>>(),
                        );
                        return Err(err.into());
                    }
                }
            }
        }

        warn!("Already tried 50 times, returning: {last_err:?}");
        Err(last_err.unwrap().into())
    }

    fn scan_batch<F: FnMut(redis::Iter<'_, String>) -> Result<()>>(
        &self,
        pattern: String,
        f: &mut F,
    ) -> Result<()> {
        let mut conn = self.pool_conn()?;
        // redis-rs will auto fetch all data batch
        let scan_options = ScanOptions::default()
            .with_pattern(&pattern)
            .with_count(10000);
        let iter = conn.scan_options(scan_options).context(RedisDetailSnafu {
            detail: format!("scan {}", &pattern),
        })?;
        f(iter)
    }

    async fn do_delete_file_data_inner(&self, inode: Ino, length: u64, tracking: &str) {
        let mut conn = match self.pool_conn() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to get connection: {}", e);
                return;
            }
        };
        
        let mut indx = 0;
        let mut p = redis::pipe();
        while indx * CHUNK_SIZE < length {
            let mut keys = Vec::new();
            for _ in 0..1000 {
                if indx * CHUNK_SIZE >= length {
                    break;
                }
                let key = self.chunk_key(inode, indx);
                keys.push(key.clone());
                p.llen(key);
                indx += 1;
            }
            let cmds: Vec<Option<i64>> = match p.query(&mut conn) {
                Ok(cmds) => cmds,
                Err(e) => {
                    error!("Failed to query: {}", e);
                    return;
                }
            };
            for (i, cmd) in cmds.iter().enumerate() {
                if let Some(val) = cmd && *val != 0 {
                    let idx = keys[i]
                        .split('_')
                        .last()
                        .expect("error split")
                        .parse::<u64>()
                        .expect("error parse");
                    self.delete_chunk(inode, idx).await
                        .unwrap_or_else(|err| error!("Failed to delete chunk: {}", err));
                }
            }
        }
        if tracking.is_empty() {
            let tracking = format!("{}:{}", inode, length);
            let () = conn.zrem(self.del_files(), tracking)
                .unwrap_or_else(|err| error!("Failed to remove tracking: {}", err));
        }
    }
    
    async fn delete_chunk(&self, inode: Ino, indx: u64) -> Result<()> {
        let mut conn = self.pool_conn()?;
        let key = self.chunk_key(inode, indx);
        match redis::transaction(&mut conn, &[&key], |c, pipe| {
            let mut todel: Vec<Slice> = Vec::new(); 
            // if not exists, ignore it
            let vals: Vec<String> = c.lrange(&key, 0, -1)?;
            
            let slices = match TryInto::<Slices>::try_into(vals) {
                Ok(Slices(slices)) => slices,
                Err(e) => {
                    error!("Corrupt value for inode {} chunk index {}, use `gc` to clean up leaked slices", inode, indx);
                    Vec::new()
                }
            };
            pipe.del(&key).ignore();
            slices.into_iter().filter(|s| s.id > 0).for_each(|slice| {
                pipe.hincr(self.slice_refs(), Self::slice_key(slice.id, slice.size), -1);
                todel.push(slice);
            });
            let rs: Vec<i64> = pipe.query(c)?;
            Ok(Some((todel, rs)))
        }) {
            Ok((todel, rs)) => {
                assert!(rs.len() == todel.len());
                todel.iter().zip(rs.iter()).filter(|(_, r)| **r < 0).for_each(|(s, _)| {
                    self.delete_slice(s.id, s.size);
                });
                Ok(())
            },
            Err(e) => {
                error!("Delete slice from chunk {} fail: {}, retry later", key, e);
                Err(e.into())
            },
        }
    }

    fn delete_slice(&self, id: u64, size: u32) {

    }
  
}

#[async_trait]
impl Engine for RedisEngine {
    async fn get_counter(&self, name: &str) -> Result<i64> {
        let mut conn = self.pool_conn()?;
        let key = format!("{}{}", self.prefix, name);
        let v = conn.get(key)?;
        Ok(v)
    }

    async fn incr_counter(&self, name: &str, value: i64) -> Result<i64> {
        ensure!(!self.state.conf.read_only, SysSnafu { code: libc::EROFS });
        let mut conn = self.pool_conn()?;
        let name_lower = name.to_lowercase();
        if name_lower.eq("nextinode") || name.eq("nextchunk") {
            let key = format!("{}{}", self.prefix, name.to_lowercase());
            let v: i64 = conn.incr(key, value)?;
            Ok(v + 1)
        } else {
            let key = format!("{}{}", self.prefix, name.to_lowercase());
            Ok(conn.incr(key, value)?)
        }
    }

    async fn set_if_small(&self, name: &str, value: i64, diff: i64) -> Result<bool> {
        let name = format!("{}{}", self.prefix, name);
        let mut conn = self.pool_conn()?;
        self.txn_with_retry(&mut conn, &[&name], &mut |c, pipeline| {
            let old: Option<i64> = c.get(&name)?;
            match old {
                Some(old) if old > value - diff => Ok(Some(false)),
                _ => {
                    pipeline.set(&name, value).query(c)?;
                    Ok(Some(true))
                }
            }
        })
        .await
    }

    async fn update_stats(&self, space: i64, inodes: i64) {
        self.state.fs_stat.update_stats(space, inodes);
    }

    async fn flush_stats(&self) {}

    async fn do_load(&self) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool_conn()?;
        Ok(conn.get(self.settings())?)
    }

    async fn do_new_session(self: Arc<Self>, sinfo: &[u8], update: bool) -> Result<()> {
        let mut conn = self.pool_conn()?;
        let sid = self.state.sid.load(Ordering::SeqCst).to_string();
        conn.zadd(self.all_sessions(), &sid, self.expire_time())
            .context(RedisDetailSnafu {
                detail: format!("set session id {sid}"),
            })?;
        conn.hset(self.session_infos(), &sid, sinfo)
            .context(RedisDetailSnafu {
                detail: "set session info err",
            })?;

        self.lookup_script
            .prepare_invoke()
            .load(&mut conn)
            .context(RedisDetailSnafu {
                detail: "load scriptLookup",
            })?;
        self.resolve_script
            .prepare_invoke()
            .load(&mut conn)
            .context(RedisDetailSnafu {
                detail: "load scriptResolve",
            })?;

        if !self.state.conf.no_bg_job {
            tokio::spawn(async move {
                self.clean_legacies().await;
            });
        }
        Ok(())
    }

    async fn do_refresh_session(&self) -> Result<()> {
        let mut conn = self.pool_conn()?;
        let ssid = self.state.sid.load(Ordering::SeqCst);
        // we have to check sessionInfo here because the operations are not within a transaction
        match conn.hexists(self.session_infos(), &ssid) {
            Ok(false) => {
                warn!("Session 0x{ssid:16x} was stale and cleaned up, but now it comes back again");
                conn.hset(self.session_infos(), &ssid, self.state.new_session_info())?;
            }
            Err(e) => return Err(e.into()),
            _ => (),
        }

        Ok(conn.zadd(self.all_sessions(), ssid, self.expire_time())?)
    }

    async fn do_find_stale_sessions(&self, limit: isize) -> Result<Vec<u64>> {
        let mut conn = self.pool_conn()?;
        let range = (
            "-inf",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        );
        Ok(conn.zrangebyscore_limit(self.all_sessions(), range.0, range.1, 0, limit)?)
        // ignore go version before 1.0-beta3 as well, this version range is different
    }

    async fn do_clean_stale_session(&self, sid: u64) -> Result<()> {
        Ok(())
        /*
        var fail bool
        // release locks
        var ctx = Background
        ssid := strconv.FormatInt(int64(sid), 10)
        key := m.lockedKey(sid)
        if inodes, err := m.rdb.SMembers(ctx, key).Result(); err == nil {
            for _, k := range inodes {
                owners, err := m.rdb.HKeys(ctx, k).Result()
                if err != nil {
                    logger.Warnf("HKeys %s: %s", k, err)
                    fail = true
                    continue
                }
                var fields []string
                for _, o := range owners {
                    if strings.Split(o, "_")[0] == ssid {
                        fields = append(fields, o)
                    }
                }
                if len(fields) > 0 {
                    if err = m.rdb.HDel(ctx, k, fields...).Err(); err != nil {
                        logger.Warnf("HDel %s %s: %s", k, fields, err)
                        fail = true
                        continue
                    }
                }
                if err = m.rdb.SRem(ctx, key, k).Err(); err != nil {
                    logger.Warnf("SRem %s %s: %s", key, k, err)
                    fail = true
                }
            }
        } else {
            logger.Warnf("SMembers %s: %s", key, err)
            fail = true
        }

        key = m.sustained(sid)
        if inodes, err := m.rdb.SMembers(ctx, key).Result(); err == nil {
            for _, sinode := range inodes {
                inode, _ := strconv.ParseInt(sinode, 10, 0)
                if err = m.doDeleteSustainedInode(sid, Ino(inode)); err != nil {
                    logger.Warnf("Delete sustained inode %d of sid %d: %s", inode, sid, err)
                    fail = true
                }
            }
        } else {
            logger.Warnf("SMembers %s: %s", key, err)
            fail = true
        }

        if !fail {
            if err := m.rdb.HDel(ctx, m.sessionInfos(), ssid).Err(); err != nil {
                logger.Warnf("HDel sessionInfos %s: %s", ssid, err)
                fail = true
            }
        }
        if fail {
            return fmt.Errorf("failed to clean up sid %d", sid)
        } else {
            if n, err := m.rdb.ZRem(ctx, m.allSessions(), ssid).Result(); err != nil {
                return err
            } else if n == 1 {
                return nil
            }
            return m.rdb.ZRem(ctx, legacySessions, ssid).Err()
        }
         */
    }

    async fn do_init(&self, format: Format, force: bool) -> Result<()> {
        let mut conn = self.pool_conn()?;
        let body: Option<String> = conn.get(self.settings())?;
        if let Some(ref body) = body {
            // if old exist, check if it can be update
            let old: Format = serde_json::from_str(body)?;
            if !old.dir_stats && format.dir_stats {
                // remove dir stats as they are outdated
                conn.del(&[self.dir_used_inodes_key(), self.dir_used_space_key()])
                    .context(RedisDetailSnafu {
                        detail: "remove dir stats",
                    })?;
            }
            format.check_ugrade(&old, force)?;
        }

        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let mut attr = Attr {
            typ: INodeType::TypeDirectory,
            atime: ts,
            mtime: ts,
            ctime: ts,
            nlink: 2,
            length: 4 << 10,
            parent: 1,
            ..Default::default()
        };
        if format.trash_days > 0 {
            attr.mode = 0555;
            conn.set_nx(self.inode_key(TRASH_INODE), bincode::serialize(&attr)?)?;
        }

        conn.set(self.settings(), serde_json::to_string(&format)?)?;
        let mut lock = self.state.fmt.write();
        *lock = format;
        match body {
            Some(_) => Ok(()),
            None => {
                // root inode
                attr.mode = 0777;
                Ok(conn.set(self.inode_key(1), bincode::serialize(&attr)?)?)
            }
        }
    }

    async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()> {
        let mut conn = self.pool_conn()?;
        let mut pipe = redis::pipe();
        let re = Regex::new(&format!(r"{}c(\d+)_(\d+)", &self.prefix)).expect("error regex");
        let mut f = move |iter: redis::Iter<'_, String>| -> Result<()> {
            let keys: Vec<String> = iter.collect();
            for key in keys.iter() {
                pipe.llen(key);
            }
            let cnt_cmds: Vec<Option<i64>> = pipe.query(&mut conn).context(RedisDetailSnafu {
                detail: "scan all chunks",
            })?;
            assert!(cnt_cmds.len() == keys.len());

            for (key, cnt) in keys.iter().zip(cnt_cmds.iter()) {
                match *cnt {
                    Some(cnt) if cnt > 1 && matches!(re.captures(key), Some(caps)) => {
                        let caps = re.captures(key).expect("error regex");
                        let inode = caps
                            .get(1)
                            .unwrap()
                            .as_str()
                            .parse::<u64>()
                            .expect("error parse");
                        let indx = caps
                            .get(2)
                            .unwrap()
                            .as_str()
                            .parse::<u32>()
                            .expect("error parse");
                        ch.send(Cchunk::new(inode, indx, cnt as isize))
                            .map_err(|_| MyError::SendError)?;
                    }
                    _ => error!("No way to happen for chunk key: {}", key),
                }
            }
            Ok(())
        };
        self.scan_batch(format!("{}{}", self.prefix, "c*_*"), &mut f)
    }

    async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()> {
        let mut conn = self.pool_conn()?;
        match self
            .txn_with_retry(&mut conn, &[self.inode_key(inode)], &mut |c, pipe| {
                let inode_v: Option<Vec<u8>> = c.get(self.inode_key(inode))?;
                match inode_v {
                    Some(inode_v) => {
                        let attr: Attr = bincode::deserialize(&inode_v).map_err(|e| {
                            RedisError::from((redis::ErrorKind::ExtensionError, "", e.to_string()))
                        })?;
                        let delete_space = i64::try_from(utils::align_4k(attr.length))
                            .ok()
                            .map(i64::checked_neg)
                            .expect("error convert")
                            .expect("error convert");
                        let ts = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs();
                        pipe.zadd(self.del_files(), Self::to_delete(inode, attr.length), ts);
                        pipe.del(self.inode_key(inode));
                        pipe.decr(self.used_space_key(), delete_space);
                        pipe.decr(self.total_inodes_key(), 1);
                        pipe.srem(self.sustained(sid), inode.to_string());
                        pipe.query(c)?;
                        Ok(Some(delete_space))
                    }
                    None => Ok(None),
                }
            })
            .await
        {
            Ok(delete_space) if delete_space < 0 => {
                self.update_stats(delete_space, -1).await;
                /*
                self.try_delete_file_data(inode, attr.length, false);
                self.update_dir_quota(attr.parent, delete_space, -1);
                */
                Ok(())
            }
            Err(e) => Err(e),
            _ => Ok(()),
        }
    }

    async fn do_find_deleted_files(&self, ts: i64, limit: isize) -> Result<HashMap<Ino, u64>> {
        let mut conn = self.pool_conn()?;
        let vals: Vec<String> = conn.zrangebyscore_limit(self.del_files(), "-inf", ts, 0, limit)?;
        let mut files = HashMap::new();
        for val in vals {
            let ps: Vec<_> = val.split(':').collect();
            if ps.len() != 2 {
                continue;
            }
            let inode = ps[0].parse::<Ino>().expect("error parse");
            let length = ps[1].parse::<u64>().expect("error parse");
            files.insert(inode, length); 
        }
        Ok(files)
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

    async fn do_set_attr(
        &self,
        inode: Ino,
        set: u16,
        sugidclearmode: u8,
        attr: &Attr,
    ) -> Result<()> {
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

    async fn do_unlink(
        &self,
        parent: Ino,
        name: &str,
        attr: &Attr,
        skip_check_trash: bool,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn do_rmdir(
        &self,
        parent: Ino,
        name: &str,
        inode: &Ino,
        skip_check_trash: bool,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn do_readlink(&self, inode: Ino, noatime: bool) -> Result<(i64, Vec<u8>)> {
        unimplemented!()
    }

    async fn do_readdir(
        &self,
        inode: Ino,
        plus: u8,
        entries: &mut Vec<Entry>,
        limit: i32,
    ) -> Result<()> {
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

fn check_trait<M: Meta>(item: M) {
    
}

fn a(comm: CommonMeta<RedisEngine>) {
    check_trait(comm);
}
