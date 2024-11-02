use async_trait::async_trait;
use bytes::Bytes;
use deadpool::managed::{Manager, Object, Pool};
use futures::StreamExt;
use juice_utils::process::Bar;
use redis::aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig};
use redis::{
    cmd, pipe, AsyncCommands, Client, Commands, InfoDict, IntoConnectionInfo, Pipeline, RedisError,
    RedisResult, ScanOptions, Script, ToRedisArgs,
};
use regex::Regex;
use snafu::{ensure, ResultExt};
use tokio::sync::mpsc::Sender;
use std::hash::{DefaultHasher, Hasher};
use std::ops::DerefMut;
use std::result::Result as StdResult;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::{self, sleep, Instant};
use tracing::{debug, error, info, warn};

use crate::acl::{self, AclId, AclType, Rule};
use crate::api::{
    gids, uid, Attr, Entry, Flag, INodeType, Ino, InoExt, Meta, ModeMask, Session, Slice, Slices,
    CHUNK_SIZE, MAX_FILE_NAME_LEN, ROOT_INODE, TRASH_INODE,
};
use crate::base::{
    Cchunk, CommonMeta, DirStat, Engine, MetaOtherFunction, PendingFileScan, PendingSliceScan,
    TrashSliceScan, NEXT_CHUNK, NEXT_INODE, N_LOCK,
};
use crate::config::{Config, Format};
use crate::error::{
    ConnectionSnafu, EmptyKeySnafu, FileExistSnafu, MyError, NotFoundInoSnafu, RedisDetailSnafu,
    Result, SysSnafu,
};
use crate::openfile::INVALIDATE_ATTR_ONLY;
use crate::quota::Quota;
use crate::utils;
use std::collections::HashMap;

use std::time::{Duration, SystemTime};

use super::check::RedisInfo;
use super::conn::{ExclusiveConnection, RedisClient};

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
pub const RESOLVE_SCRIPT_CODE: &str = r#"
local function unpack_attr(buf)
    local x = {}
    x.flags, x.mode, x.uid, x.gid = struct.unpack(">BHI4I4", string.sub(buf, 0, 11))
    x.type = math.floor(x.mode / 4096) % 8
    x.mode = x.mode % 4096
    return x
end

local function get_attr(ino)
    local encoded_attr = redis.call('GET', "i" .. string.format("%.f", ino))
    if not encoded_attr then
        error("ENOENT")
    end
    return unpack_attr(encoded_attr)
end

local function lookup(parent, name)
    local buf = redis.call('HGET', "d" .. string.format("%.f", parent), name)
    if not buf then
        error("ENOENT")
    end
    return struct.unpack(">BI8", buf)
end

local function has_value(tab, val)
    for index, value in ipairs(tab) do
        if value == val then
            return true
        end
    end
    return false
end

local function can_access(ino, uid, gids)
    if uid == 0 then
        return true
    end

    local attr = get_attr(ino)
    local mode = 0
    if attr.uid == uid then
        mode = math.floor(attr.mode / 64) % 8
    elseif has_value(gids, tostring(attr.gid)) then
        mode = math.floor(attr.mode / 8) % 8
    else
        mode = attr.mode % 8
    end
    return mode % 2 == 1
end

local function resolve(parent, path, uid, gids)
    local _maxIno = 4503599627370495
    local _type = 2
    for name in string.gmatch(path, "[^/]+") do
        if _type == 3 or parent > _maxIno then
            error("ENOTSUP")
        elseif _type ~= 2 then
            error("ENOTDIR")
        elseif parent > 1 and not can_access(parent, uid, gids) then 
            error("EACCESS")
        end
        _type, parent = lookup(parent, name)
    end
    if parent > _maxIno then
        error("ENOTSUP")
    end
    return {parent, redis.call('GET', "i" .. string.format("%.f", parent))}
end

return resolve(tonumber(KEYS[1]), KEYS[2], tonumber(KEYS[3]), ARGV)
"#;

/**
 * conn: ConnectionLike
 * key:
 * body: Fn() -> Result<Option<T>>,
 *       Ok(Some(T)) => exec ok
 *       Ok(None) => exec faile, but can retry to exec
 *       Err(e) => exec faile, can't retry to exec
 */
macro_rules! async_transaction {
    ($conn:expr, $keys:expr, $body:expr) => {
        loop {
            cmd("WATCH").arg($keys).query_async($conn).await?;
            match $body {
                Ok(Some(response)) => {
                    cmd("UNWATCH").query_async($conn).await?;
                    break Ok(response);
                }
                Ok(None) => continue,
                Err(e) => break Err(e),
            }
        }
    };
}

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
/// Redis Client Engine
///
/// pool: A connection which want to exclusive, and it will be used in transaction.If use
///       [`redis::aio::MultiplexedConnection`] to as pool conn, you cann't stop user clone it,
///       it may break transaction. So we use [`ExclusiveConnection`] to as pool conn.
///       [relational issue](https://github.com/redis-rs/redis-rs/issues/1257)
/// shared_conn: A shared connection.It use [`redis::aio::MultiplexedConnection`]
pub struct RedisEngine {
    // TODO: single connection or multiple connections?
    /// Redis Sentinel mode
    /// Redis Cluster mode
    /// Redis normal mode
    pool: Pool<RedisClient>,
    shared_conn: ConnectionManager,
    prefix: String,
    lookup_script: Script,  // lookup lua script
    resolve_script: Script, // reslove lua script
    meta: CommonMeta,
}

impl RedisEngine {
    fn should_retry(err: &RedisError) -> bool {
        !err.is_unrecoverable_error()
    }

    async fn exclusive_conn(&self) -> Result<Object<RedisClient>> {
        self.pool.get().await.context(ConnectionSnafu)
    }

    fn share_conn(&self) -> ConnectionManager {
        self.shared_conn.clone()
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

    /// Dir: {prefix}d{inode} -> {name -> {type,inode}}
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

    fn acl_key(&self) -> String {
        format!("{}acl", self.prefix)
    }

    fn trash_entry(&self, parent: Ino, ino: Ino, name: &str) -> String {
        let mut s = format!("{}-{}-{}", parent, ino, name);
        if s.len() > MAX_FILE_NAME_LEN {
            s = s[..MAX_FILE_NAME_LEN].to_string();
            warn!(
                "File name is too long as a trash entry, truncating it: {} -> {}",
                name, s
            );
        }
        s
    }

    fn expire_time(&self) -> u64 {
        (SystemTime::now() + self.meta.conf.heartbeat * 5)
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }

    // For now only deleted files
    async fn clean_legacies(&self) {}
}

impl RedisEngine {
    /// redis URL:
    /// `{redis|rediss}://[<username>][:<password>@]<hostname>[:port][/<db>]`
    pub async fn new(driver: &str, addr: &str, conf: Config) -> Result<Box<Arc<dyn Meta>>> {
        let url = format!("{}://{}", driver, addr);
        let conn_info = url.into_connection_info()?;
        let prefix = conn_info.redis.db.to_string();
        // TODO: should merge conf into `ConnectionInfo`
        let client = RedisClient::new(Client::open(conn_info.clone())?);
        let conn = client.get_connection().await?;
        let pool = Pool::builder(client).max_size(16).build().unwrap();

        let meta = CommonMeta::new(addr, conf);
        let engine = RedisEngine {
            pool,
            shared_conn: conn,
            prefix,
            lookup_script: Script::new(LOOKUP_SCRIPT_CODE),
            resolve_script: Script::new(RESOLVE_SCRIPT_CODE),
            meta,
        };

        engine.check_server_config().await?;
        Ok(Box::new(Arc::new(engine)))
    }

    async fn check_server_config(&self) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let info: InfoDict =
            redis::cmd("INFO")
                .query_async(conn)
                .await
                .context(RedisDetailSnafu {
                    detail: "prase redis info".to_string(),
                })?;
        let redis_info = RedisInfo::new(info);

        // TODO: check
        Ok(())
        /*
                rawInfo, err := m.rdb.Info(Background).Result()
        if err != nil {
            logger.Warnf("parse info: %s", err)
            return
        }
        rInfo, err := checkRedisInfo(rawInfo)
        if err != nil {
            logger.Warnf("parse info: %s", err)
        }
        if rInfo.storageProvider == "" && rInfo.maxMemoryPolicy != "" && rInfo.maxMemoryPolicy != "noeviction" {
            logger.Warnf("maxmemory_policy is %q,  we will try to reconfigure it to 'noeviction'.", rInfo.maxMemoryPolicy)
            if _, err := m.rdb.ConfigSet(Background, "maxmemory-policy", "noeviction").Result(); err != nil {
                logger.Errorf("try to reconfigure maxmemory-policy to 'noeviction' failed: %s", err)
            } else if result, err := m.rdb.ConfigGet(Background, "maxmemory-policy").Result(); err != nil {
                logger.Warnf("get config maxmemory-policy failed: %s", err)
            } else if len(result) == 1 && result["maxmemory-policy"] != "noeviction" {
                logger.Warnf("reconfigured maxmemory-policy to 'noeviction', but it's still %s", result["maxmemory-policy"])
            } else {
                logger.Infof("set maxmemory-policy to 'noeviction' successfully")
            }
        }
        start := time.Now()
        _, err = m.rdb.Ping(Background).Result()
        if err != nil {
            logger.Errorf("Ping redis: %s", err.Error())
            return
        }
        logger.Infof("Ping redis latency: %s", time.Since(start))
             */
    }

    /*
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
        ensure!(!self.meta.conf.read_only, SysSnafu { code: libc::EROFS });
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
        let _ = self.meta.txn_locks[(hash % N_LOCK as u64) as usize].lock();

        let mut last_err = None;
        for i in 0..TXN_RETRY_LIMIT {
            ensure!(
                !self.meta.canceled.load(Ordering::Relaxed),
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
    */

    async fn scan_iter<'a>(
        &self,
        conn: &'a mut impl AsyncCommands,
        pattern: String,
    ) -> Result<redis::AsyncIter<'a, String>> {
        // redis-rs will auto fetch all data batch
        let scan_options = ScanOptions::default()
            .with_pattern(&pattern)
            .with_count(10000);
        conn.scan_options(scan_options)
            .await
            .context(RedisDetailSnafu {
                detail: format!("scan {}", &pattern),
            })
    }

    async fn do_delete_file_data_inner(&self, inode: Ino, length: u64, tracking: &str) {
        let mut pool_conn = match self.exclusive_conn().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to get connection: {}", e);
                return;
            }
        };
        let conn = pool_conn.deref_mut();

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
            let cmds: Vec<Option<i64>> = match p.query_async(conn).await {
                Ok(cmds) => cmds,
                Err(e) => {
                    error!("Failed to query: {}", e);
                    return;
                }
            };
            for (i, cmd) in cmds.iter().enumerate() {
                if let Some(val) = cmd
                    && *val != 0
                {
                    let idx = keys[i]
                        .split('_')
                        .last()
                        .expect("error split")
                        .parse::<u64>()
                        .expect("error parse");
                    self.delete_chunk(inode, idx)
                        .await
                        .unwrap_or_else(|err| error!("Failed to delete chunk: {}", err));
                }
            }
        }
        if tracking.is_empty() {
            let tracking = format!("{}:{}", inode, length);
            let () = conn
                .zrem(self.del_files(), tracking)
                .await
                .unwrap_or_else(|err| error!("Failed to remove tracking: {}", err));
        }
    }

    async fn delete_chunk(&self, inode: Ino, indx: u64) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let key = self.chunk_key(inode, indx);
        match async_transaction!(conn, &[&key], {
            let mut todel: Vec<Slice> = Vec::new();
            // if not exists, ignore it
            let vals: Vec<String> = conn.lrange(&key, 0, -1).await?;

            let slices = match TryInto::<Slices>::try_into(vals) {
                Ok(Slices(slices)) => slices,
                Err(e) => {
                    error!("Corrupt value for inode {} chunk index {}, use `gc` to clean up leaked slices", inode, indx);
                    Vec::new()
                }
            };
            let mut pipe = pipe();
            pipe.atomic().del(&key).ignore();
            slices.into_iter().filter(|s| s.id > 0).for_each(|slice| {
                pipe.hincr(self.slice_refs(), Self::slice_key(slice.id, slice.size), -1);
                todel.push(slice);
            });
            let rs: Vec<i64> = pipe.query_async(conn).await?;
            Ok(Some((todel, rs)))
        }) {
            Ok((todel, rs)) => {
                assert!(rs.len() == todel.len());
                todel
                    .iter()
                    .zip(rs.iter())
                    .filter(|(_, r)| **r < 0)
                    .for_each(|(s, _)| {
                        self.delete_slice(s.id, s.size);
                    });
                Ok(())
            }
            Err(e) => {
                error!("Delete slice from chunk {} fail: {}, retry later", key, e);
                Err(e)
            }
        }
    }

    fn delete_slice(&self, id: u64, size: u32) {}

    async fn get_acl(
        &self,
        pipe: &mut Pipeline,
        conn: &mut impl ConnectionLike,
        id: u32,
    ) -> Result<Rule> {
        // 这里必须拿到，拿不到就是有逻辑上的错误
        let mut cache = self.meta.acl_cache.lock().await;
        if let Some(rule) = cache.get(id) {
            return Ok(rule);
        }

        let cmds: Option<Vec<u8>> = pipe.hget(self.acl_key(), id).query_async(conn).await?;
        match cmds {
            None => SysSnafu { code: libc::EIO }.fail(),
            Some(cmds) => {
                let rule: Rule = bincode::deserialize(&cmds)?;
                cache.put(id, rule.clone());
                Ok(rule)
            }
        }
    }

    async fn insert_acl(
        &self,
        pipe: &mut Pipeline,
        conn: &mut impl ConnectionLike,
        rule: Rule,
    ) -> Result<AclId> {
        if rule.is_empty() {
            return Ok(acl::NONE);
        }

        self.load_miss_acls(pipe, conn)
            .await
            .map_err(|e| warn!("SetFacl: load miss acls error: {}", e));
        // set acl
        let mut cache = self.meta.acl_cache.lock().await;
        match cache.get_id(&rule) {
            Some(acl_id) => Ok(acl_id),
            None => {
                let new_id = self.incr_counter(acl::ACL_COUNTER, 1).await?;
                pipe.hset_nx(self.acl_key(), new_id, bincode::serialize(&rule)?)
                    .query_async::<()>(conn);
                cache.put(new_id as u32, rule);
                Ok(new_id as u32)
            }
        }
    }

    async fn load_miss_acls(
        &self,
        pipe: &mut Pipeline,
        conn: &mut impl ConnectionLike,
    ) -> Result<()> {
        let mut cache = self.meta.acl_cache.lock().await;
        let miss_ids = cache.get_miss_ids();
        match miss_ids.is_empty() {
            true => Ok(()),
            false => {
                let miss_keys: Vec<String> = miss_ids.iter().map(|id| id.to_string()).collect();
                let vals: Vec<Option<Vec<u8>>> = pipe
                    .hget(self.acl_key(), miss_keys)
                    .query_async(conn)
                    .await?;
                for (i, data) in vals.iter().enumerate() {
                    match data {
                        Some(bytes) => {
                            let rule: Rule = bincode::deserialize(bytes)?;
                            cache.put(miss_ids[i], rule);
                        }
                        None => warn!("Missing acl id: {}", miss_ids[i]),
                    };
                }
                Ok(())
            }
        }
    }
}

impl AsRef<CommonMeta> for RedisEngine {
    fn as_ref(&self) -> &CommonMeta {
        &self.meta
    }
}

#[async_trait]
impl Engine for RedisEngine {
    async fn get_counter(&self, name: &str) -> Result<i64> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let key = format!("{}{}", self.prefix, name);
        let v = conn.get(key).await?;
        Ok(v)
    }

    async fn incr_counter(&self, name: &str, value: i64) -> Result<i64> {
        ensure!(!self.meta.conf.read_only, SysSnafu { code: libc::EROFS });
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let name_lower = name.to_lowercase();
        if name_lower.eq(NEXT_INODE) || name.eq(NEXT_CHUNK) {
            let key = format!("{}{}", self.prefix, name.to_lowercase());
            let v: i64 = conn.incr(key, value).await?;
            Ok(v + 1)
        } else {
            let key = format!("{}{}", self.prefix, name.to_lowercase());
            Ok(conn.incr(key, value).await?)
        }
    }

    async fn set_if_small(&self, name: &str, value: i64, diff: i64) -> Result<bool> {
        let name = format!("{}{}", self.prefix, name);
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[&name], {
            let old: Option<i64> = conn.get(&name).await?;
            let mut pipe = pipe();
            match old {
                Some(old) if old > value - diff => Ok(Some(false)),
                _ => {
                    pipe.atomic().set(&name, value).query_async(conn).await?;
                    Ok(Some(true))
                }
            }
        })
    }

    async fn update_stats(&self, space: i64, inodes: i64) {
        self.meta.fs_stat.update_stats(space, inodes);
    }

    async fn flush_stats(&self) {}

    async fn do_load(&self) -> Result<Option<Format>> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let format_body: Option<Vec<u8>> = conn.get(self.settings()).await?;
        match format_body {
            Some(body) => Ok(Some(serde_json::from_slice(&body)?)),
            None => Ok(None),
        }
    }

    async fn do_new_session(self: Arc<Self>, sid: u64, sinfo: &[u8], update: bool) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        conn.zadd(self.all_sessions(), &sid, self.expire_time())
            .await
            .context(RedisDetailSnafu {
                detail: format!("set session id {sid}"),
            })?;
        conn.hset(self.session_infos(), &sid, sinfo)
            .await
            .context(RedisDetailSnafu {
                detail: "set session info err",
            })?;

        self.lookup_script
            .prepare_invoke()
            .load_async(conn)
            .await
            .context(RedisDetailSnafu {
                detail: "load scriptLookup",
            })?;
        self.resolve_script
            .prepare_invoke()
            .load_async(conn)
            .await
            .context(RedisDetailSnafu {
                detail: "load scriptResolve",
            })?;

        if !self.meta.conf.no_bg_job {
            tokio::spawn(async move {
                self.clean_legacies().await;
            });
        }
        Ok(())
    }

    async fn do_refresh_session(&self) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let ssid = match self.meta.sid {
            Some(ref sid) => sid.load(Ordering::SeqCst),
            None => {
                error!("new session has not been called");
                return Ok(())
            },
        };
        // we have to check sessionInfo here because the operations are not within a transaction
        match conn.hexists(self.session_infos(), &ssid).await {
            Ok(false) => {
                warn!("Session 0x{ssid:16x} was stale and cleaned up, but now it comes back again");
                conn.hset(self.session_infos(), &ssid, self.meta.new_session_info())
                    .await?;
            }
            Err(e) => return Err(e.into()),
            _ => (),
        }

        Ok(conn
            .zadd(self.all_sessions(), ssid, self.expire_time())
            .await?)
    }

    async fn do_find_stale_sessions(&self, limit: isize) -> Result<Vec<u64>> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let range = (
            "-inf",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        );
        Ok(conn
            .zrangebyscore_limit(self.all_sessions(), range.0, range.1, 0, limit)
            .await?)
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
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let body: Option<Vec<u8>> = conn.get(self.settings()).await?;
        if let Some(ref body) = body {
            // if old exist, check if it can be update
            let old: Format = serde_json::from_slice(body)?;
            if !old.enable_dir_stats && format.enable_dir_stats {
                // remove dir stats as they are outdated
                conn.del(&[self.dir_used_inodes_key(), self.dir_used_space_key()])
                    .await
                    .context(RedisDetailSnafu {
                        detail: "remove dir stats",
                    })?;
            }
            format.check_ugrade(&old, force)?;
        }

        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();
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
            conn.set_nx(self.inode_key(TRASH_INODE), bincode::serialize(&attr)?)
                .await?;
        }

        conn.set(self.settings(), serde_json::to_vec(&format)?)
            .await?;
        {
            let mut lock = self.meta.fmt.write();
            *lock = Arc::new(format);
        }
        match body {
            Some(_) => Ok(()),
            None => {
                // root inode
                attr.mode = 0777;
                Ok(conn
                    .set(self.inode_key(ROOT_INODE), bincode::serialize(&attr)?)
                    .await?)
            }
        }
    }

    async fn do_reset(&self) -> Result<()> {
        if !self.prefix.is_empty() {
            let mut conn = self.share_conn();
            let mut conn2 = conn.clone();
            let mut iter = self.scan_iter(&mut conn, "*".to_string()).await?;
            while let Some(key) = iter.next().await {
                conn2.del(key).await?;
            }
            Ok(())
        } else {
            let _: () = redis::cmd("FLUSHDB")
                .query_async(&mut self.share_conn())
                .await?;
            Ok(())
        }
    }

    async fn scan_all_chunks(&self, ch: Sender<Cchunk>, bar: &Bar) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let mut pipe = redis::pipe();
        let re = Regex::new(&format!(r"{}c(\d+)_(\d+)", &self.prefix)).expect("error regex");
        let iter = self
            .scan_iter(conn, format!("{}{}", self.prefix, "c*_*"))
            .await?;
        let keys: Vec<String> = iter.collect().await;
        for key in keys.iter() {
            pipe.llen(key);
        }
        let cnt_cmds: Vec<Option<i64>> =
            pipe.query_async(conn).await.context(RedisDetailSnafu {
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
                        .await
                        .map_err(|_| MyError::SendError)?;
                }
                _ => error!("No way to happen for chunk key: {}", key),
            }
        }
        Ok(())
    }

    async fn do_delete_sustained_inode(&self, sid: u64, inode: Ino) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        match async_transaction!(conn, &[self.inode_key(inode)], {
            let inode_v: Option<Vec<u8>> = conn.get(self.inode_key(inode)).await?;
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
                    let mut pipe = pipe();
                    pipe.atomic()
                        .zadd(self.del_files(), Self::to_delete(inode, attr.length), ts)
                        .del(self.inode_key(inode))
                        .decr(self.used_space_key(), delete_space)
                        .decr(self.total_inodes_key(), 1)
                        .srem(self.sustained(sid), inode.to_string())
                        .query_async(conn)
                        .await?;
                    Ok(Some(delete_space))
                }
                None => Ok(None),
            }
        }) {
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
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let vals: Vec<String> = conn
            .zrangebyscore_limit(self.del_files(), "-inf", ts, 0, limit)
            .await?;
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

    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr)> {
        unimplemented!()
    }

    async fn do_mknod(
        &self,
        parent: Ino,
        name: &str,
        mut mode: u16,
        cumask: u16,
        path: &str,
        inode: Ino,
        mut attr: Attr,
    ) -> Result<Attr> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[self.inode_key(parent), self.dir_key(parent)], {
            let pattr_bytes: Vec<u8> = conn.get(self.inode_key(parent)).await?;
            let mut pattr: Attr = bincode::deserialize(&pattr_bytes)?;
            if pattr.typ != INodeType::TypeDirectory {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "type error",
                    "not a directory".to_string(),
                ))
                .into());
            }
            if pattr.parent > TRASH_INODE {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "type error",
                    "not a directory".to_string(),
                ))
                .into());
            }
            self.access(parent, ModeMask::WRITE, &pattr).await?;
            if !(Flag::IMMUTABLE & pattr.flags).is_empty() {
                return SysSnafu { code: libc::EPERM }.fail();
            }

            // check exists
            let buf: Option<Vec<u8>> = conn.hget(self.dir_key(parent), name).await?;
            let found_ino = match buf {
                Some(buf) => {
                    let (ino_type, ino): (INodeType, Ino) = bincode::deserialize(&buf)?;
                    Some((ino_type, ino))
                }
                None if self.meta.conf.case_insensi => self
                    .resolve_case(parent, name)
                    .await?
                    .map(|entry| (entry.attr.typ, entry.inode)),
                None => None,
            };
            if let Some((found_type, found_ino)) = found_ino {
                let found_attr = match found_type {
                    // file for create, directory for subTrash
                    INodeType::TypeFile | INodeType::TypeDirectory => {
                        let exists_attr_bytes: Option<Vec<u8>> =
                            conn.get(self.inode_key(found_ino)).await?;
                        match exists_attr_bytes {
                            Some(attr_bytes) => bincode::deserialize(&attr_bytes)?,
                            None => {
                                // cirrupt entry
                                Attr {
                                    typ: found_type,
                                    parent: parent,
                                    ..Default::default()
                                }
                            }
                        }
                    }
                    _ => attr.clone(),
                };
                return FileExistSnafu {
                    ino: found_ino,
                    attr: found_attr,
                }
                .fail();
            };

            // acl mode search
            mode &= 0o7777;
            let mut pipe = pipe();
            if pattr.default_acl != acl::NONE && attr.typ != INodeType::TypeSymlink {
                // inherit default acl
                if attr.typ == INodeType::TypeDirectory {
                    pattr.default_acl = pattr.default_acl;
                }

                // set access acl by parent's default acl
                let rule = self.get_acl(pipe.atomic(), conn, pattr.default_acl).await?;
                if rule.is_minimal() {
                    pattr.mode = mode & (0xFE00 | rule.get_mode());
                } else {
                    let c_rule = rule.child_access_acl(mode);
                    let c_mode = c_rule.get_mode();
                    let id = self.insert_acl(pipe.atomic(), conn, c_rule).await?;
                    pattr.access_acl = id;
                    pattr.mode = (mode & 0xFE00) | c_mode;
                }
            } else {
                pattr.mode = mode & !cumask;
            }

            // time set
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            let mut update_parent = false;
            if parent != TRASH_INODE {
                if attr.typ == INodeType::TypeDirectory {
                    pattr.nlink += 1;
                    update_parent = true;
                }
                if update_parent || now - pattr.mtime >= self.meta.conf.skip_dir_mtime.as_nanos() {
                    pattr.mtime = now;
                    pattr.ctime = now;
                    update_parent = true;
                }
            }
            attr.atime = now;
            attr.mtime = now;
            attr.ctime = now;

            // different os adapt
            if cfg!(target_os = "macos") {
                pattr.gid = pattr.gid;
            } else if cfg!(target_os = "linux") && pattr.mode & 0o2000 != 0 {
                pattr.gid = pattr.gid;
                if attr.typ == INodeType::TypeDirectory {
                    pattr.mode |= 0o2000;
                } else if pattr.mode & 0o2010 == 0o2010 && 0 != 0 {
                    if let None = gids().iter().find(|gid| **gid == pattr.gid) {
                        pattr.mode &= !0o2000;
                    }
                }
            }

            // redis set kv
            let pipe = pipe.atomic();
            pipe.hset(
                self.dir_key(parent),
                name,
                bincode::serialize(&(&attr.typ, inode))?,
            );
            if update_parent {
                pipe.set(self.inode_key(parent), bincode::serialize(&pattr)?);
            }
            pipe.set(self.inode_key(inode), bincode::serialize(&attr)?);
            if attr.typ == INodeType::TypeSymlink {
                pipe.set(self.sym_key(inode), path);
            }
            if attr.typ == INodeType::TypeDirectory {
                pipe.hset(self.dir_used_inodes_key(), inode, "0");
                pipe.hset(self.dir_data_length_key(), inode, "0");
                pipe.hset(self.dir_used_space_key(), inode, "0");
            }
            pipe.incr(self.used_space_key(), utils::align_4k(0));
            pipe.incr(self.total_inodes_key(), 1);
            pipe.query_async(conn).await?;
            Ok(Some(attr.clone()))
        })
    }

    async fn do_link(&self, inode: Ino, parent: Ino, name: &str, attr: &Attr) -> Result<()> {
        unimplemented!()
    }

    async fn do_unlink(
        self: Arc<Self>,
        parent: Ino,
        name: &str,
        skip_check_trash: bool,
    ) -> Result<Attr> {
        let mut trash = if !skip_check_trash {
            self.check_trash(parent).await?
        } else {
            None
        };
        info!("do unlink trash: {:?}", trash);
        let base_meta = self.as_ref().as_ref();
        if let Some(trash_ino) = trash {
            base_meta
                .open_files
                .invalid(trash_ino, INVALIDATE_ATTR_ONLY)
                .await;
        }

        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let (space_guage, ino_cnt_guage, should_del, open_session_id, ino, attr) =
            async_transaction!(conn, &[self.inode_key(parent), self.dir_key(parent)], {
                let buf: Option<Vec<u8>> = conn.hget(self.dir_key(parent), name).await?;
                let (buf, name) = match buf {
                    Some(entry) => (entry, name.to_owned()),
                    None if base_meta.conf.case_insensi => {
                        if let Some(entry) = self.resolve_case(parent, name).await? {
                            (
                                bincode::serialize(&(entry.attr.typ, entry.inode)).unwrap(),
                                entry.name.to_string(),
                            )
                        } else {
                            return NotFoundInoSnafu { ino: parent }.fail();
                        }
                    }
                    None => return NotFoundInoSnafu { ino: parent }.fail(),
                };

                let (typ, ino): (INodeType, Ino) = bincode::deserialize::<(INodeType, Ino)>(&buf)?;
                if typ == INodeType::TypeDirectory {
                    return SysSnafu { code: libc::EPERM }.fail();
                }

                // first find ino by parent ino and son name, then watch it
                cmd("WATCH")
                    .arg(self.inode_key(ino))
                    .query_async(conn)
                    .await?;
                let (pattr_bytes, attr_bytes): (Vec<u8>, Option<Vec<u8>>) = conn
                    .mget((self.inode_key(ino), self.inode_key(parent)))
                    .await?;
                let mut pattr = bincode::deserialize::<Attr>(&pattr_bytes)?;
                // double check: parent mut be a dir
                if pattr.typ != INodeType::TypeDirectory {
                    return SysSnafu {
                        code: libc::ENOTDIR,
                    }
                    .fail();
                }
                self.access(parent, ModeMask::WRITE | ModeMask::EXECUTE, &pattr)
                    .await?;
                if pattr.flags.contains(Flag::APPEND) || pattr.flags.contains(Flag::IMMUTABLE) {
                    return SysSnafu { code: libc::EPERM }.fail();
                }

                // change time
                let mut update_parent = false;
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_nanos();
                if !parent.is_trash()
                    && now - pattr.mtime >= self.meta.conf.skip_dir_mtime.as_nanos()
                {
                    pattr.mtime = now;
                    pattr.ctime = now;
                    update_parent = true;
                }

                let (mut should_del, mut open_session_id) = (false, None); // only check when consider del ino
                let attr = match attr_bytes {
                    Some(attr_bytes) => {
                        let mut attr = bincode::deserialize::<Attr>(&attr_bytes)?;
                        let uid = uid();
                        if uid != 0
                            && pattr.mode & 0o1000 != 0
                            && uid != pattr.uid
                            && uid != attr.uid
                        {
                            return SysSnafu { code: libc::EACCES }.fail();
                        }
                        if attr.flags.contains(Flag::APPEND) || attr.flags.contains(Flag::IMMUTABLE)
                        {
                            return SysSnafu { code: libc::EPERM }.fail();
                        }
                        // check if trash ino and name already exists, don't repeat generate
                        if let Some(trash_ino) = trash
                            && attr.nlink > 1
                        {
                            if conn
                                .hexists(
                                    self.dir_key(trash_ino),
                                    self.trash_entry(parent, ino, &name),
                                )
                                .await?
                            {
                                trash.take();
                                base_meta
                                    .open_files
                                    .invalid(trash_ino, INVALIDATE_ATTR_ONLY)
                                    .await;
                            }
                        }
                        attr.ctime = now;
                        // check opened, if link is 0, so check if any session open it, if none, del it
                        match trash {
                            Some(trash_ino) if attr.parent > 0 => attr.parent = trash_ino,
                            None => {
                                attr.nlink -= 1;
                                if typ == INodeType::TypeFile && attr.nlink == 0 {
                                    should_del = true;
                                    if let Some(ref sid) = self.meta.sid
                                        && base_meta.open_files.is_open(ino).await
                                    {
                                        open_session_id.replace(sid.load(Ordering::SeqCst));
                                    }
                                }
                            }
                            _ => (),
                        };
                        attr
                    }
                    None => {
                        warn!("no attribute  for inode {}  ({}, {})", ino, parent, name);
                        trash.take();
                        Attr::default()
                    }
                };

                // make trascation
                let mut pipe = pipe();
                pipe.atomic();
                pipe.hdel(self.dir_key(parent), &name);
                if update_parent {
                    pipe.set(self.inode_key(parent), bincode::serialize(&pattr)?);
                }
                let guaga = if attr.nlink > 0 {
                    pipe.set(self.inode_key(ino), bincode::serialize(&attr)?);
                    if let Some(trash_ino) = trash {
                        pipe.hset(
                            self.dir_key(trash_ino),
                            self.trash_entry(parent, ino, &name),
                            buf,
                        );
                        if attr.parent == 0 {
                            pipe.hincr(self.parent_key(ino), trash_ino.to_string(), 1);
                        }
                    }
                    if attr.parent == 0 {
                        pipe.hincr(self.parent_key(ino), parent.to_string(), -1);
                    }
                    (0, 0)
                } else {
                    let guaga = match typ {
                        INodeType::TypeFile => {
                            if let Some(sid) = open_session_id {
                                pipe.set(self.inode_key(ino), bincode::serialize(&attr)?);
                                pipe.sadd(self.sustained(sid), &ino);
                                (0, 0)
                            } else {
                                let ts = SystemTime::now()
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_secs();
                                pipe.zadd(self.del_files(), Self::to_delete(ino, attr.length), ts);
                                pipe.del(self.inode_key(ino));
                                let new_space = -utils::align_4k(attr.length);
                                pipe.incr(self.used_space_key(), new_space);
                                pipe.decr(self.total_inodes_key(), -1);
                                (new_space, -1)
                            }
                        }
                        INodeType::TypeSymlink => {
                            pipe.del(self.inode_key(ino));
                            pipe.del(self.inode_key(ino));
                            let new_space = -utils::align_4k(0);
                            pipe.incr(self.used_space_key(), new_space);
                            pipe.decr(self.total_inodes_key(), -1);
                            (new_space, -1)
                        }
                        _ => {
                            pipe.del(self.inode_key(ino));
                            let new_space = -utils::align_4k(0);
                            pipe.incr(self.used_space_key(), new_space);
                            pipe.decr(self.total_inodes_key(), -1);
                            (new_space, -1)
                        }
                    };
                    pipe.del(self.xattr_key(ino));
                    if attr.parent == 0 {
                        pipe.del(self.parent_key(ino));
                    }
                    guaga
                };
                Ok::<Option<_>, MyError>(Some((
                    guaga.0,
                    guaga.1,
                    should_del,
                    open_session_id,
                    ino,
                    attr.clone(),
                )))
            })?;

        if trash.is_none() {
            if should_del {
                self.clone()
                    .try_spawn_delete_file(
                        ino,
                        attr.length,
                        parent.is_trash(),
                        open_session_id.is_some(),
                    )
                    .await
            }
            self.update_stats(space_guage, ino_cnt_guage).await;
        }
        Ok(attr)
    }

    async fn do_rmdir(
        &self,
        parent: Ino,
        name: &str,
        inode: &Ino,
        skip_check_trash: &[bool],
    ) -> Result<()> {
        unimplemented!()
    }

    async fn do_readlink(&self, inode: Ino, noatime: bool) -> Result<(i64, Vec<u8>)> {
        unimplemented!()
    }

    async fn do_readdir(&self, inode: Ino, plus: u8, limit: i32) -> Result<Option<Vec<Entry>>> {
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
        skip_perm_check: bool,
    ) -> Result<(Attr, DirStat)> {
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

    async fn do_set_facl(&self, ino: Ino, acl_type: AclType, rule: &Rule) -> Result<()> {
        unimplemented!()
    }

    async fn do_get_facl(&self, ino: Ino, acl_type: AclType, acl_id: AclId) -> Result<Rule> {
        unimplemented!()
    }

    async fn cache_acls(&self) -> Result<()> {
        unimplemented!()
    }
}
