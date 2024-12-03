use async_trait::async_trait;
use bytes::{Buf, Bytes};
use deadpool::managed::{Object, Pool};
use futures::StreamExt;
use juice_utils::process::Bar;
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::{
    cmd, from_redis_value, pipe, AsyncCommands, AsyncIter, Client, FromRedisValue, InfoDict,
    IntoConnectionInfo, Pipeline, RedisError, RedisResult, ScanOptions, Script, Value,
};
use regex::Regex;
use snafu::{ensure, ensure_whatever, whatever, FromString, ResultExt};
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::acl::{self, AclExt, AclId, AclType, Rule};
use crate::api::{
    Attr, Entry, Falloc, Flag, Flock, INodeType, Ino, InoExt, Meta, ModeMask, Plock, RenameMask,
    Session, SetAttrMask, Slice, XattrF, CHUNK_SIZE, MAX_FILE_NAME_LEN, ROOT_INODE, TRASH_INODE,
};
use crate::base::{
    Cchunk, CommonMeta, DirStat, Engine, MetaOtherFunction, PendingFileScan, PendingSliceScan,
    TrashSliceScan, NEXT_CHUNK, NEXT_INODE, TOTAL_INODES, USED_SPACE,
};
use crate::config::{Config, Format};
use crate::context::{Uid, UserExt, WithContext};
use crate::error::{
    BrokenPipeSnafu, ConnectionSnafu, DirNotEmptySnafu, EntryExists2Snafu, EntryExistsSnafu,
    InvalidArgSnafu, MetaError, MetaErrorEnum, NoEntryFound2Snafu, NoEntryFoundSnafu,
    NoSuchAttrSnafu, NotDir1Snafu, NotDir2Snafu, OpNotPermittedSnafu, OpNotSupportedSnafu,
    PermissionDeniedSnafu, ReadFSSnafu, RedisDetailSnafu, RenameSameInoSnafu, Result,
};
use crate::openfile::INVALIDATE_ATTR_ONLY;
use crate::quota::{MetaQuota, Quota, QuotaView};
use crate::slice::{PSlice, PSlices, Slices};
use crate::utils::{self, align_4k, DeleteFileOption};
use std::collections::HashMap;

use std::time::{Duration, SystemTime};

use super::check::RedisInfo;
use super::conn::RedisClient;

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
            error("EACCES")
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

#[derive(Clone)]
enum LuaScriptArg {
    Lookup { parent: Ino, name: String },
    Resolve { parent: Ino, name: String, uid: Uid },
}

impl Display for LuaScriptArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LuaScriptArg::Lookup { parent, name } => {
                write!(f, "lookup(parent:{},name:{})", parent, name)
            }
            LuaScriptArg::Resolve { parent, name, uid } => {
                write!(f, "resolve(parent:{},name:{},uid:{})", parent, name, uid)
            }
        }
    }
}

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
pub struct RedisHandle {
    // TODO: single connection or multiple connections?
    /// Redis Sentinel mode
    /// Redis Cluster mode
    /// Redis normal mode
    pool: Pool<RedisClient>,
    shared_conn: ConnectionManager,
    // 单机和集群模式才有这个前缀，哨兵和主从模式没有这个前缀，juicefs中的很多判断区别是这个
    prefix: String,
    lookup_script_sha: String,  // lookup lua script sha
    resolve_script_sha: String, // reslove lua script sha
    meta: CommonMeta,
}

impl RedisHandle {
    fn should_retry(err: &RedisError) -> bool {
        !err.is_unrecoverable_error()
    }

    async fn exclusive_conn(&self) -> Result<Object<RedisClient>> {
        let conn = self.pool.get().await.context(ConnectionSnafu)?;
        Ok(conn)
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
    fn entry_key(&self, inode: Ino) -> String {
        format!("{}d{}", self.prefix, inode)
    }

    /// Parent:     p$inode -> {parent -> count} // for hard links
    fn parent_key(&self, inode: Ino) -> String {
        format!("{}p{}", self.prefix, inode)
    }

    /// File:       c$inode_$indx -> [off,Slice{pos,id,length,off,len}]
    fn chunk_key(&self, inode: Ino, indx: u32) -> String {
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
        format!("{}{}", self.prefix, USED_SPACE)
    }

    fn total_inodes_key(&self) -> String {
        format!("{}{}", self.prefix, TOTAL_INODES)
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
    async fn cleanup_legacies(&self) {}

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
}

/// Redis Engine with Context
/// Context is independent execution context
pub struct RedisEngine {
    engine: Arc<RedisHandle>,
    uid: u32,
    gid: u32,
    gids: Vec<u32>,
    token: CancellationToken,
}

impl Deref for RedisEngine {
    type Target = RedisHandle;
    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

impl AsRef<CommonMeta> for RedisEngine {
    fn as_ref(&self) -> &CommonMeta {
        &self.engine.meta
    }
}

impl RedisEngine {
    /// redis URL:
    /// `{redis|rediss}://[<username>][:<password>@]<hostname>[:port][/<db>]`
    pub async fn new(driver: &str, addr: &str, conf: Config) -> Result<Box<dyn Meta>> {
        let url = format!("{}://{}", driver, addr);
        let conn_info = url.into_connection_info()?;
        let prefix = conn_info.redis.db.to_string();
        // TODO: should merge conf into `ConnectionInfo`
        let client = RedisClient::new(Client::open(conn_info.clone())?);
        let mut conn = client.get_connection().await?;
        let pool = Pool::builder(client).max_size(16).build().unwrap();
        let lookup_script_sha = Script::new(LOOKUP_SCRIPT_CODE)
            .prepare_invoke()
            .load_async(&mut conn)
            .await
            .context(RedisDetailSnafu {
                detail: "load scriptLookup",
            })?;
        let resolve_script_sha = Script::new(RESOLVE_SCRIPT_CODE)
            .prepare_invoke()
            .load_async(&mut conn)
            .await
            .context(RedisDetailSnafu {
                detail: "load scriptResolve",
            })?;

        let meta = CommonMeta::new(addr, conf);
        let engine = RedisHandle {
            pool,
            shared_conn: conn,
            prefix,
            lookup_script_sha,
            resolve_script_sha,
            meta,
        };

        engine.check_server_config().await?;
        Ok(Box::new(RedisEngine {
            engine: Arc::new(engine),
            uid: 0,
            gid: 0,
            gids: Vec::new(),
            token: CancellationToken::new(),
        }))
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

        let mut indx = 0_u32;
        let mut p = redis::pipe();
        while indx as u64 * CHUNK_SIZE < length {
            let mut keys = Vec::new();
            for _ in 0..1000 {
                if indx as u64 * CHUNK_SIZE >= length {
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
                        .parse::<u32>()
                        .expect("error parse");
                    self.delete_chunk(inode, idx)
                        .await
                        .unwrap_or_else(|err| error!("Failed to delete chunk: {}", err));
                }
            }
        }
        let tracking = if tracking.is_empty() {
            format!("{}:{}", inode, length)
        } else {
            tracking.to_string()
        };
        conn.zrem(self.del_files(), tracking)
            .await
            .unwrap_or_else(|err| error!("Failed to remove tracking: {}", err));
    }

    async fn delete_chunk(&self, inode: Ino, indx: u32) -> Result<()> {
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
                pipe.hincr(
                    self.slice_refs(),
                    RedisHandle::slice_key(slice.id, slice.size),
                    -1,
                );
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

    async fn get_acl(&self, conn: &mut impl AsyncCommands, id: AclId) -> Result<Option<Rule>> {
        if !id.is_valid_acl() {
            return Ok(None);
        }
        let mut cache = self.meta.acl_cache.lock().await;
        if let Some(rule) = cache.get(id) {
            return Ok(Some(rule));
        }

        let mut pipe = pipe();
        pipe.atomic();
        let res: Vec<Value> = pipe
            .hget(self.acl_key(), id)
            .query_async(conn)
            .await
            .context(RedisDetailSnafu {
                detail: format!("get acl {}", id),
            })?;
        let cmds = from_redis_value::<Option<Vec<u8>>>(&res[0])?;
        match cmds {
            None => {
                whatever!("Missing acl id: {}", id);
            }
            Some(cmds) => {
                let rule: Rule = bincode::deserialize(&cmds)?;
                cache.put(id, rule.clone());
                Ok(Some(rule))
            }
        }
    }

    async fn insert_acl(&self, conn: &mut impl AsyncCommands, rule: Option<Rule>) -> Result<AclId> {
        let rule = match rule {
            Some(rule) if !rule.is_empty() => rule,
            _ => return Ok(AclId::invalid_acl()),
        };

        self.load_miss_acls(conn)
            .await
            .unwrap_or_else(|e| warn!("SetFacl: load miss acls error: {}", e));
        // set acl
        let mut cache = self.meta.acl_cache.lock().await;
        match cache.get_id(&rule) {
            Some(acl_id) => Ok(acl_id),
            None => {
                let new_id = self.incr_counter(acl::ACL_COUNTER, 1).await?;
                conn.hset_nx(self.acl_key(), new_id, bincode::serialize(&rule)?)
                    .await?;
                cache.put(new_id as u32, rule);
                Ok(new_id as u32)
            }
        }
    }

    async fn load_miss_acls(&self, conn: &mut impl AsyncCommands) -> Result<()> {
        let mut cache = self.meta.acl_cache.lock().await;
        let miss_ids = cache.get_miss_ids();
        match miss_ids.is_empty() {
            true => Ok(()),
            false => {
                let miss_keys: Vec<String> = miss_ids.iter().map(|id| id.to_string()).collect();
                let vals: Vec<Option<Vec<u8>>> = conn.hget(self.acl_key(), miss_keys).await?;
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

    async fn do_fill_session(&self, session: &mut Session, detail: bool) -> Result<()> {
        let mut conn = self.share_conn();
        let sid = session.sid;
        let info: Option<Vec<u8>> = conn.hget(self.session_infos(), sid).await?;
        session.session_info = match info {
            Some(info) => Some(bincode::deserialize(&info)?),
            None => None,
        };

        if detail {
            let inodes: Vec<u64> = conn.smembers(self.sustained(sid)).await?;
            session.sustained = Some(inodes);

            let mut flocks = Vec::new();
            let mut plocks = Vec::new();
            let locks: Vec<String> = conn.smembers(self.locked(sid)).await?;
            for lock in locks {
                let owners: HashMap<String, Vec<u8>> = conn.hgetall(&lock).await?;
                let is_flock = lock.starts_with("lockf");
                let inode = lock[self.prefix.len() + 5..]
                    .parse::<u64>()
                    .with_whatever_context::<_, String, MetaErrorEnum>(|err| {
                        format!(
                            "SMEMBERS {}. fill session {}, semember {}",
                            self.locked(sid),
                            sid,
                            lock
                        )
                    })?;
                for (k, v) in owners {
                    let parts: Vec<&str> = k.split('_').collect();
                    if parts[0] != sid.to_string() {
                        continue;
                    }
                    let owner = match u64::from_str_radix(parts[1], 16) {
                        Ok(owner) => owner,
                        Err(err) => continue,
                    };
                    if is_flock {
                        flocks.push(Flock {
                            inode,
                            owner,
                            l_type: bincode::deserialize(&v)?,
                        });
                    } else {
                        plocks.push(Plock {
                            inode,
                            owner,
                            records: bincode::deserialize(&v)?,
                        });
                    }
                }
            }
            (session.flocks, session.plocks) = (Some(flocks), Some(plocks));
        }
        Ok(())
    }

    /// find inode by parent and name first, then find it by resolve second
    ///
    /// # returns
    ///
    /// (ino_type, ino, name): name could be different if case insensitive
    async fn do_lookup_entry(
        &self,
        conn: &mut impl AsyncCommands,
        parent: Ino,
        name: &str,
    ) -> Result<(INodeType, Ino, String)> {
        match conn
            .hget::<_, _, Option<Vec<u8>>>(self.entry_key(parent), name)
            .await?
        {
            Some(buf) => {
                let (typ, ino) = bincode::deserialize(&buf)
                    .with_whatever_context::<_, _, MetaErrorEnum>(|_| {
                        format!("HGET {} {}", self.entry_key(parent), name)
                    })?;
                Ok((typ, ino, name.to_owned()))
            }
            None => {
                if self.meta.conf.case_insensi {
                    if let Some(entry) = self.lookup_ignore_ascii_case(parent, name).await? {
                        Ok((entry.attr.typ, entry.inode, entry.name.to_string()))
                    } else {
                        NoEntryFoundSnafu { parent, name }.fail()?
                    }
                } else {
                    NoEntryFoundSnafu { parent, name }.fail()?
                }
            }
        }
    }

    async fn do_get_parents_inner(
        &self,
        conn: &mut impl AsyncCommands,
        inode: Ino,
    ) -> HashMap<Ino, u32> {
        match conn
            .hgetall::<_, HashMap<u64, u32>>(self.parent_key(inode))
            .await
        {
            Ok(ps) => ps,
            Err(e) => {
                warn!("Scan parent key of inode {}: {}", inode, e);
                HashMap::new()
            }
        }
    }

    fn handle_script_result(arg: LuaScriptArg, result: RedisResult<Value>) -> Result<(Ino, Attr)> {
        match result {
            Ok(value) => match value {
                Value::Array(res) => {
                    ensure!(
                        res.len() == 2,
                        OpNotSupportedSnafu {
                            op: format!("lua script({arg}) return len err")
                        }
                    );
                    if let Value::Int(ino) = res[0]
                        && let Value::BulkString(ref attr) = res[1]
                    {
                        match bincode::deserialize(attr) {
                            Ok(attr) => Ok((ino as u64, attr)),
                            Err(err) => OpNotSupportedSnafu {
                                op: format!("lua script({arg}) return deser err {err}"),
                            }
                            .fail()?,
                        }
                    } else {
                        OpNotSupportedSnafu {
                            op: format!("lua script({arg}) return output type err: {res:?}"),
                        }
                        .fail()?
                    }
                }
                other => OpNotSupportedSnafu {
                    op: format!("lua script({arg}) return output err: {other:?}"),
                }
                .fail()?,
            },
            Err(error) => {
                if let Some(detail) = error.detail() {
                    let (parent, name) = match arg.clone() {
                        LuaScriptArg::Lookup { parent, name } => (parent, name),
                        LuaScriptArg::Resolve { parent, name, .. } => (parent, name),
                    };
                    if detail.contains("ENOENT") {
                        NoEntryFoundSnafu { parent, name }.fail()?
                    } else if detail.contains("ENOTSUP") {
                        OpNotSupportedSnafu {
                            op: format!("lua script({arg}) invoke err"),
                        }
                        .fail()?
                    } else if detail.contains("EACCES") {
                        return PermissionDeniedSnafu { ino: parent }.fail()?;
                    } else if detail.contains("ENOTDIR") {
                        NotDir1Snafu { parent, name }.fail()?
                    } else {
                        OpNotSupportedSnafu {
                            op: format!("lua script({arg}) return redis err: {error}"),
                        }
                        .fail()?
                    }
                } else {
                    OpNotSupportedSnafu {
                        op: format!("lua script({arg}) return redis err: {error}"),
                    }
                    .fail()?
                }
            }
        }
    }
}

impl Clone for RedisEngine {
    fn clone(&self) -> Self {
        RedisEngine {
            engine: self.engine.clone(),
            uid: self.uid,
            gid: self.gid,
            gids: self.gids.clone(),
            token: self.token.clone(),
        }
    }
}

impl WithContext for RedisEngine {
    fn with_login(&mut self, uid: u32, gids: Vec<u32>) {
        assert!(gids.len() > 0);
        self.uid = uid;
        self.gid = gids[0];
        self.gids = gids;
    }

    fn with_cancel(&mut self, token: CancellationToken) {
        self.token = token;
    }
    fn uid(&self) -> &u32 {
        &self.uid
    }
    fn gid(&self) -> &u32 {
        &self.gid
    }
    fn gids(&self) -> &Vec<u32> {
        &self.gids
    }
    fn token(&self) -> &CancellationToken {
        &self.token
    }
    fn check_permission(&self) -> bool {
        true
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
        ensure!(!self.meta.conf.read_only, ReadFSSnafu);
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

    async fn set_if_small(&self, name: &str, value: Duration, diff: Duration) -> Result<bool> {
        let name = format!("{}{}", self.prefix, name);
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[&name], {
            let old: Option<u64> = conn.get(&name).await?;
            let mut pipe = pipe();
            match old {
                Some(old) if Duration::from_millis(old as u64) + diff > value => Ok(Some(false)),
                _ => {
                    pipe.atomic()
                        .set(&name, value.as_millis() as u64)
                        .query_async(conn)
                        .await?;
                    Ok(Some(true))
                }
            }
        })
    }

    async fn update_stats(&self, space: i64, inodes: i64) {
        self.meta.fs_stat.update_stats(space, inodes);
    }

    // redisMeta updates the usage in each transaction
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

    async fn do_new_session(&self, sid: u64, sinfo: &[u8], update: bool) -> Result<()> {
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

        if self.meta.conf.enable_bg_job {
            let mut engine = self.clone();
            engine.with_cancel(self.token().child_token());
            tokio::spawn(async move {
                engine.cleanup_legacies().await;
            });
        }
        Ok(())
    }

    async fn do_refresh_session(&self, sid: u64) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        // we have to check sessionInfo here because the operations are not within a transaction
        match conn.hexists(self.session_infos(), &sid).await {
            Ok(false) => {
                warn!("Session 0x{sid:16x} was stale and cleaned up, but now it comes back again");
                conn.hset(
                    self.session_infos(),
                    &sid,
                    bincode::serialize(&self.meta.new_session_info()).unwrap(),
                )
                .await?;
            }
            Err(e) => return Err(e.into()),
            _ => (),
        }

        Ok(conn
            .zadd(self.all_sessions(), sid, self.expire_time())
            .await?)
    }

    async fn do_list_sessions(&self) -> Result<Vec<Session>> {
        let mut conn = self.share_conn();
        let keys: Vec<(u64, f64)> = conn.zrange_withscores(self.all_sessions(), 0, -1).await?;
        let mut sessions = Vec::new();
        for (key, ts) in keys {
            let mut session = Session {
                sid: key,
                expire: Duration::from_secs_f64(ts),
                session_info: None,
                sustained: None,
                flocks: None,
                plocks: None,
            };
            if let Err(e) = self.do_fill_session(&mut session, false).await {
                error!("Get session error: {}", e);
                continue;
            }
            sessions.push(session);
        }
        Ok(sessions)
    }

    async fn do_get_session(&self, sid: u64, detail: bool) -> Result<Session> {
        let mut conn = self.share_conn();
        let score: f64 = conn.zscore(self.all_sessions(), sid).await?;
        let mut session = Session {
            sid,
            expire: Duration::from_secs_f64(score),
            session_info: None,
            sustained: None,
            flocks: None,
            plocks: None,
        };
        self.do_fill_session(&mut session, detail).await?;
        Ok(session)
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

    async fn do_init(&self, mut format: Format, force: bool) -> Result<()> {
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
            format.check_ugrade(old, force)?;
        }

        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();
        let mut attr = Attr {
            typ: INodeType::Directory,
            atime: ts,
            mtime: ts,
            ctime: ts,
            nlink: 2,
            length: 4 << 10,
            parent: 1,
            ..Default::default()
        };
        if format.trash_days > 0 {
            attr.mode = 0o555;
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
                attr.mode = 0o777;
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
            let mut iter = conn.scan_match::<&str, Vec<u8>>("*").await?.chunks(10000);
            while let Some(keys) = iter.next().await {
                conn2.del(keys).await?;
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
        let mut shared_conn = self.share_conn();
        let mut pipe = redis::pipe();
        let re = Regex::new(&format!(r"{}c(\d+)_(\d+)", &self.prefix)).expect("error regex");
        let mut iter = shared_conn
            .scan_match::<String, Vec<u8>>(format!("{}{}", self.prefix, "c*_*"))
            .await?
            .chunks(10000);
        while let Some(keys) = iter.next().await {
            let mut pool_conn = self.exclusive_conn().await?;
            let conn = pool_conn.deref_mut();
            for key in keys.iter() {
                pipe.llen(key);
            }
            let cnt_cmds: Vec<Option<i64>> =
                pipe.query_async(conn).await.context(RedisDetailSnafu {
                    detail: "scan all chunks",
                })?;
            assert!(cnt_cmds.len() == keys.len());

            for (key, cnt) in keys.iter().zip(cnt_cmds.iter()) {
                // convert key to string
                let key = match String::from_utf8(key.clone()) {
                    Ok(key) => key,
                    Err(e) => {
                        error!("Invalid key: {:?}", e);
                        continue;
                    }
                };
                match *cnt {
                    Some(cnt) if cnt > 1 && matches!(re.captures(&key), Some(caps)) => {
                        let caps = re.captures(&key).expect("error regex");
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
                            .map_err(|_| MetaErrorEnum::SendError)?;
                    }
                    _ => error!("No way to happen for chunk key: {}", key),
                }
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
                        .zadd(
                            self.del_files(),
                            RedisHandle::to_delete(inode, attr.length),
                            ts,
                        )
                        .del(self.inode_key(inode))
                        .decr(self.used_space_key(), delete_space)
                        .decr(self.total_inodes_key(), 1)
                        .srem(self.sustained(sid), inode.to_string())
                        .query_async(conn)
                        .await?;
                    Ok(Some((delete_space, attr)))
                }
                None => Ok(None),
            }
        }) {
            Ok((delete_space, attr)) => {
                if delete_space < 0 {
                    self.update_stats(delete_space, -1).await;
                    self.try_spawn_delete_file(
                        inode,
                        attr.length,
                        DeleteFileOption::Immediate { force: false },
                    )
                    .await;
                    self.update_quota(attr.parent, delete_space, -1).await;
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn do_find_deleted_files(&self, ts: Duration, limit: isize) -> Result<HashMap<Ino, u64>> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let vals: Vec<String> = conn
            .zrangebyscore_limit(self.del_files(), "-inf", ts.as_secs(), 0, limit)
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
        self.do_delete_file_data_inner(inode, length, "").await;
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

    async fn do_get_quota(&self, inode: Ino) -> Result<Option<QuotaView>> {
        let mut share_conn = self.exclusive_conn().await?;
        let conn = share_conn.deref_mut();

        let mut pipe = pipe();
        pipe.atomic();
        pipe.hget(self.dir_quota_key(), inode);
        pipe.hget(self.dir_quota_used_space_key(), inode);
        pipe.hget(self.dir_quota_used_inodes_key(), inode);
        let (quota, used_space, used_inodes): (Option<Vec<u8>>, Option<i64>, Option<i64>) =
            pipe.query_async(conn).await?;

        if quota.is_none() || used_space.is_none() || used_inodes.is_none() {
            return Ok(None);
        }
        let (max_space, max_inodes) = bincode::deserialize::<(i64, i64)>(&quota.unwrap())
            .with_whatever_context::<_, _, MetaErrorEnum>(|err| {
            format!("invalid quota inode {}, err {}", inode, err)
        })?;
        Ok(Some(QuotaView {
            max_space: max_space,
            max_inodes: max_inodes,
            used_space: used_space.unwrap(),
            used_inodes: used_inodes.unwrap(),
            new_space: 0,
            new_inodes: 0,
        }))
    }

    async fn do_del_quota(&self, inode: Ino) -> Result<()> {
        unimplemented!()
    }

    async fn do_load_quotas(&self) -> Result<HashMap<Ino, Quota>> {
        let mut shared_conn = self.share_conn();
        let mut iter = shared_conn
            .hscan::<String, Vec<u8>>(self.dir_quota_key())
            .await?
            .chunks(10000);
        let mut quotas = HashMap::new();
        while let Some(keys) = iter.next().await {
            if keys.len() % 2 != 0 {
                panic!("Invalid key: {} for load quotas", self.dir_quota_key());
            }
            let mut conn = self.share_conn();
            for i in (0..keys.len()).step_by(2) {
                let (mut key, mut value) = (
                    Bytes::from(keys[i].clone()),
                    Bytes::from(keys[i + 1].clone()),
                );
                if key.len() != std::mem::size_of::<u64>() {
                    error!("Invalid key: {:?}", key);
                    continue;
                }
                let inode = key.get_u64();
                if value.len() != 2 * std::mem::size_of::<i64>() {
                    error!("Invalid value: {:?}", value);
                    continue;
                }
                let max_space = value.get_i64();
                let max_inodes = value.get_i64();
                let used_space = conn
                    .hget::<String, u64, Option<i64>>(self.dir_quota_used_space_key(), inode)
                    .await?
                    .unwrap_or(0);
                let used_inodes = conn
                    .hget::<String, u64, Option<i64>>(self.dir_quota_used_inodes_key(), inode)
                    .await?
                    .unwrap_or(0);
                quotas.insert(
                    inode,
                    Quota {
                        max_space: AtomicI64::new(max_space),
                        max_inodes: AtomicI64::new(max_inodes),
                        used_space: AtomicI64::new(used_space),
                        used_inodes: AtomicI64::new(used_inodes),
                        ..Default::default()
                    },
                );
            }
        }
        Ok(quotas)
    }

    async fn do_flush_quotas(&self, quotas: &HashMap<Ino, (i64, i64)>) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();

        let mut pipe = pipe();
        pipe.atomic();
        for (ino, (new_space, new_inodes)) in quotas.iter() {
            pipe.hincr(self.dir_quota_used_space_key(), ino, new_space);
            pipe.hincr(self.dir_quota_used_inodes_key(), ino, new_inodes);
        }
        pipe.query_async(conn).await?;
        Ok(())
    }

    async fn do_get_attr(&self, inode: Ino) -> Result<Attr> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[self.inode_key(inode)], {
            let attr_bytes = conn
                .get::<_, Option<Vec<u8>>>(self.inode_key(inode))
                .await?
                .ok_or_else(|| NoEntryFound2Snafu { ino: inode }.build())?;
            let attr: Attr = bincode::deserialize(&attr_bytes)?;
            Ok(Some(attr))
        })
    }

    async fn do_set_attr(
        &self,
        inode: Ino,
        set: SetAttrMask,
        _sugidclearmode: u8,
        attr: &Attr,
    ) -> Result<Option<Attr>> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[self.inode_key(inode)], {
            let attr_bytes = conn
                .get::<_, Option<Vec<u8>>>(self.inode_key(inode))
                .await
                .with_context(|_| RedisDetailSnafu {
                    detail: format!("get attr for inode {}", inode),
                })?
                .ok_or_else(|| NoEntryFound2Snafu { ino: inode }.build())?;
            let cur: Attr = bincode::deserialize(&attr_bytes)?;
            if cur.parent > TRASH_INODE {
                return OpNotPermittedSnafu {
                    op: format!("set_attr: rm trash without root user."),
                }
                .fail()?;
            }

            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards");
            let mut rule = self.get_acl(conn, attr.access_acl).await?;

            if let Some(mut dirty_attr) = self
                .merge_attr(inode, set, &cur, attr, now, &mut rule)
                .await?
            {
                dirty_attr.access_acl = self.insert_acl(conn, rule).await?;
                dirty_attr.ctime = now.as_nanos();
                let mut pipe = pipe();
                pipe.atomic();
                pipe.set(self.inode_key(inode), bincode::serialize(&dirty_attr)?)
                    .query_async(conn)
                    .await?;
                Ok(Some(Some(dirty_attr)))
            } else {
                Ok(Some(None))
            }
        })
    }

    async fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, Attr)> {
        let mut conn = self.share_conn();
        if !self.meta.conf.case_insensi && self.prefix.is_empty() {
            let mut cmd = cmd("EVALSHA");
            cmd.arg(&self.lookup_script_sha)
                .arg(2)
                .arg(self.entry_key(parent))
                .arg(name);

            // only handle ENOTSUP, fallback to normal lookup
            let arg = LuaScriptArg::Lookup {
                parent,
                name: name.to_string(),
            };
            match RedisEngine::handle_script_result(arg, cmd.query_async(&mut conn).await) {
                Ok((ino, attr)) => return Ok((ino, attr)),
                Err(err) => {
                    if err.is_op_not_supported() {
                        warn!("invoke script err: {err}");
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        let buf: Vec<u8> = match conn.hget(self.entry_key(parent), name).await? {
            Some(buf) => buf,
            None => {
                return NoEntryFoundSnafu {
                    parent,
                    name: name.to_string(),
                }
                .fail()?
            }
        };
        let (ino_type, ino): (INodeType, Ino) = bincode::deserialize(&buf)?;
        let attr_bytes: Option<Vec<u8>> = conn.get(self.inode_key(ino)).await?;
        match attr_bytes {
            Some(attr_bytes) => Ok((ino, (bincode::deserialize(&attr_bytes))?)),
            None => {
                // corrupt entry
                warn!("no attribute for inode {} ({}, {})", ino, parent, name);
                Ok((
                    ino,
                    Attr {
                        typ: ino_type,
                        ..Default::default()
                    },
                ))
            }
        }
    }

    async fn do_resolve(&self, parent: Ino, path: &str) -> Result<(Ino, Attr)> {
        if self.meta.conf.case_insensi || !self.prefix.is_empty() {
            return OpNotSupportedSnafu {
                op: "cannot resolve if case insensi or prefix is not empty",
            }
            .fail()?;
        }

        let mut conn = self.share_conn();
        let parent = self.meta.check_root(parent);

        let mut cmd = cmd("EVALSHA");
        cmd.arg(&self.resolve_script_sha)
            .arg(3)
            .arg(parent)
            .arg(path)
            .arg(self.uid());
        for gid in self.gids() {
            cmd.arg(gid.to_string());
        }
        let arg = LuaScriptArg::Resolve {
            parent,
            name: path.to_string(),
            uid: *self.uid(),
        };
        let (ino, attr) = RedisEngine::handle_script_result(arg, cmd.query_async(&mut conn).await)?;
        Ok((ino, attr))
    }

    async fn do_mknod(
        &self,
        parent: Ino,
        name: &str,
        cumask: u16,
        path: &str,
        inode: Ino,
        mut attr: Attr,
    ) -> Result<Attr> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[self.inode_key(parent), self.entry_key(parent)], {
            let pattr_bytes: Option<Vec<u8>> = conn.get(self.inode_key(parent)).await?;
            if pattr_bytes.is_none() {
                return NoEntryFound2Snafu { ino: parent }.fail()?;
            }
            let mut pattr: Attr = bincode::deserialize(&pattr_bytes.unwrap())?;
            if pattr.typ != INodeType::Directory {
                return NotDir2Snafu { ino: parent }.fail()?;
            }
            if pattr.parent > TRASH_INODE {
                return NoEntryFoundSnafu {
                    parent,
                    name: name.to_string(),
                }
                .fail()?;
            }
            self.access(parent, ModeMask::WRITE, &pattr).await?;
            ensure!(
                !pattr.flags.intersects(Flag::IMMUTABLE),
                OpNotPermittedSnafu {
                    op: format!("mknod: mknod in an immutable dir."),
                }
            );

            // check exists
            let buf: Option<Vec<u8>> = conn.hget(self.entry_key(parent), name).await?;
            let found_ino = match buf {
                Some(buf) => {
                    let (ino_type, ino): (INodeType, Ino) = bincode::deserialize(&buf)?;
                    Some((ino_type, ino))
                }
                None if self.meta.conf.case_insensi => self
                    .lookup_ignore_ascii_case(parent, name)
                    .await?
                    .map(|entry| (entry.attr.typ, entry.inode)),
                None => None,
            };
            if let Some((found_type, found_ino)) = found_ino {
                let found_attr = match found_type {
                    // file for create, directory for subTrash
                    INodeType::File | INodeType::Directory => {
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
                return EntryExistsSnafu {
                    parent,
                    name: name.to_string(),
                    exist_ino: found_ino,
                    exist_attr: Some(found_attr),
                }
                .fail()?;
            };

            // acl mode search
            attr.mode &= 0o7777;
            let mut pipe = pipe();
            if pattr.default_acl.is_valid_acl() && attr.typ != INodeType::Symlink {
                // inherit default acl
                if attr.typ == INodeType::Directory {
                    attr.default_acl = pattr.default_acl;
                }

                // set access acl by parent's default acl
                let rule = match self.get_acl(conn, pattr.default_acl).await? {
                    Some(rule) => rule,
                    None => whatever!("can't find parent({parent}) acl"),
                };
                if rule.is_minimal() {
                    attr.mode &= 0xFE00 | rule.get_mode();
                } else {
                    let c_rule = rule.child_access_acl(attr.mode);
                    let c_mode = c_rule.get_mode();
                    let id = self.insert_acl(conn, Some(c_rule)).await?;
                    attr.access_acl = id;
                    attr.mode = (attr.mode & 0xFE00) | c_mode;
                }
            } else {
                attr.mode = attr.mode & !cumask;
            }

            // time set
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            let mut update_parent = false;
            if parent != TRASH_INODE {
                if attr.typ == INodeType::Directory {
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
                attr.gid = pattr.gid;
            } else if cfg!(target_os = "linux") && pattr.mode & 0o2000 != 0 {
                attr.gid = pattr.gid;
                if attr.typ == INodeType::Directory {
                    attr.mode |= 0o2000;
                } else if attr.mode & 0o2010 == 0o2010 && *self.uid() != 0 {
                    if let None = self.gids().iter().find(|gid| **gid == pattr.gid) {
                        attr.mode &= !0o2000;
                    }
                }
            }

            // redis set kv
            let pipe = pipe.atomic();
            pipe.hset(
                self.entry_key(parent),
                name,
                bincode::serialize(&(&attr.typ, inode)).unwrap(),
            );
            if update_parent {
                pipe.set(self.inode_key(parent), bincode::serialize(&pattr)?);
            }
            pipe.set(self.inode_key(inode), bincode::serialize(&attr)?);
            if attr.typ == INodeType::Symlink {
                pipe.set(self.sym_key(inode), path);
            }
            if attr.typ == INodeType::Directory {
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

    async fn do_link(&self, inode: Ino, parent: Ino, name: &str) -> Result<Attr> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(
            conn,
            &[
                self.inode_key(parent),
                self.entry_key(parent),
                self.inode_key(inode)
            ],
            {
                let keys = vec![self.inode_key(parent), self.inode_key(inode)];
                let rs: Vec<Option<Vec<u8>>> = conn.mget(&keys).await?;
                if rs.len() != 2 {
                    error!("no attribute for inodes {keys:?}");
                    whatever!("Illigal len for mget {keys:?}, {:?}", keys);
                }
                if rs[0].is_none() {
                    return NoEntryFound2Snafu { ino: parent }.fail()?;
                }
                if rs[1].is_none() {
                    return NoEntryFound2Snafu { ino: inode }.fail()?;
                }

                let mut pattr: Attr = bincode::deserialize(&rs[0].as_ref().unwrap())?;
                if pattr.typ != INodeType::Directory {
                    return NotDir2Snafu { ino: parent }.fail()?;
                }
                if pattr.parent > TRASH_INODE {
                    return NoEntryFound2Snafu { ino: parent }.fail()?;
                }
                self.access(parent, ModeMask::WRITE.union(ModeMask::EXECUTE), &pattr)
                    .await?;
                ensure!(
                    !pattr.flags.intersects(Flag::IMMUTABLE),
                    OpNotPermittedSnafu {
                        op: format!("link in an immutable dir."),
                    }
                );
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Time went backwards");
                let update_pattr =
                    if now.as_nanos() - pattr.mtime >= self.meta.conf.skip_dir_mtime.as_nanos() {
                        pattr.mtime = now.as_nanos();
                        pattr.ctime = now.as_nanos();
                        true
                    } else {
                        false
                    };
                let mut attr: Attr = bincode::deserialize(&rs[1].as_ref().unwrap())?;
                ensure!(
                    attr.typ != INodeType::Directory,
                    OpNotPermittedSnafu {
                        op: format!("link an directory."),
                    }
                );
                ensure!(
                    !attr.flags.intersects(Flag::APPEND.union(Flag::IMMUTABLE)),
                    OpNotPermittedSnafu {
                        op: format!("link target has flag append or immutable."),
                    }
                );

                let old_parent = attr.parent;
                attr.parent = 0;
                attr.ctime = now.as_nanos();
                attr.nlink += 1;
                let entry: Option<Vec<u8>> = conn.hget(self.entry_key(parent), name).await?;
                match entry {
                    Some(buf) => {
                        let (typ, ino): (INodeType, Ino) = bincode::deserialize(&buf)?;
                        return EntryExistsSnafu {
                            parent,
                            name: name.to_string(),
                            exist_ino: inode,
                            exist_attr: None,
                        }
                        .fail()?;
                    }
                    None if self.meta.conf.case_insensi
                        && let Some(entry) =
                            self.lookup_ignore_ascii_case(parent, name).await? =>
                    {
                        return EntryExistsSnafu {
                            parent,
                            name: name.to_string(),
                            exist_ino: entry.inode,
                            exist_attr: Some(entry.attr),
                        }
                        .fail()?;
                    }
                    _ => (),
                }

                let mut pipe = pipe();
                pipe.atomic();
                pipe.hset(
                    self.entry_key(parent),
                    name,
                    bincode::serialize(&(&attr.typ, inode)).unwrap(),
                );
                if update_pattr {
                    pipe.set(self.inode_key(parent), bincode::serialize(&pattr)?);
                }
                pipe.set(self.inode_key(inode), bincode::serialize(&attr)?);
                if old_parent > 0 {
                    pipe.hincr(self.parent_key(inode), old_parent.to_string(), 1);
                }
                pipe.hincr(self.parent_key(inode), parent.to_string(), 1);
                pipe.exec_async(conn).await?;
                Ok(Some(attr))
            }
        )
    }

    async fn do_unlink(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Attr> {
        let mut trash = if !skip_check_trash {
            self.check_trash(parent).await?
        } else {
            None
        };
        trash
            .as_ref()
            .inspect(|trash_ino| info!("do unlink produce new trash: {}", trash_ino));
        let base = self.as_ref();
        if let Some(trash_ino) = trash {
            let file = base.open_files.lock(trash_ino);
            let mut of = file.lock().await;
            of.invalidate_chunk(INVALIDATE_ATTR_ONLY);
        }

        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let (space_guage, ino_cnt_guage, should_del, open_session_id, ino, attr) =
            async_transaction!(conn, &[self.inode_key(parent), self.entry_key(parent)], {
                let (typ, ino, name) = self.do_lookup_entry(conn, parent, name).await?;
                ensure!(
                    typ != INodeType::Directory,
                    OpNotPermittedSnafu {
                        op: format!("unlink target is an directory."),
                    }
                );

                // first find ino by parent ino and son name, then watch it
                cmd("WATCH")
                    .arg(self.inode_key(ino))
                    .query_async(conn)
                    .await?;
                let (pattr_bytes, attr_bytes): (Vec<u8>, Option<Vec<u8>>) = conn
                    .mget((self.inode_key(parent), self.inode_key(ino)))
                    .await?;
                let mut pattr = bincode::deserialize::<Attr>(&pattr_bytes)?;
                // double check: parent mut be a dir and access mode
                if pattr.typ != INodeType::Directory {
                    return NotDir2Snafu { ino: parent }.fail()?;
                }
                self.access(parent, ModeMask::WRITE | ModeMask::EXECUTE, &pattr)
                    .await?;
                ensure!(
                    !pattr.flags.intersects(Flag::APPEND.union(Flag::IMMUTABLE)),
                    OpNotPermittedSnafu {
                        op: "unlink dir is append or immutable.",
                    }
                );

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
                        let uid = self.uid().clone();
                        if !uid.is_root()
                            && pattr.mode & 0o1000 != 0
                            && uid != pattr.uid
                            && uid != attr.uid
                        {
                            return PermissionDeniedSnafu { ino: parent }.fail()?;
                        }
                        ensure!(
                            !attr.flags.intersects(Flag::APPEND.union(Flag::IMMUTABLE)),
                            OpNotPermittedSnafu {
                                op: "unlink file is append or immutable.",
                            }
                        );
                        // check if trash ino and name already exists, don't repeat generate
                        if let Some(trash_ino) = trash
                            && attr.nlink > 1
                        {
                            if conn
                                .hexists(
                                    self.entry_key(trash_ino),
                                    self.trash_entry(parent, ino, &name),
                                )
                                .await?
                            {
                                trash.take();
                                let file = base.open_files.lock(trash_ino);
                                let mut of = file.lock().await;
                                of.invalidate_chunk(INVALIDATE_ATTR_ONLY);
                            }
                        }
                        attr.ctime = now;
                        // check opened, if link is 0, so check if any session open it, if none, del it
                        match trash {
                            Some(trash_ino) if attr.parent > 0 => attr.parent = trash_ino,
                            None => {
                                attr.nlink -= 1;
                                if typ == INodeType::File && attr.nlink == 0 {
                                    should_del = true;
                                    let sid = { self.meta.sid.read().clone() };
                                    if let Some(sid) = sid
                                        && base.open_files.has_any_open(ino).await
                                    {
                                        open_session_id.replace(sid);
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
                pipe.hdel(self.entry_key(parent), &name);
                if update_parent {
                    pipe.set(self.inode_key(parent), bincode::serialize(&pattr)?);
                }
                let guaga = if attr.nlink > 0 {
                    pipe.set(self.inode_key(ino), bincode::serialize(&attr)?);
                    if let Some(trash_ino) = trash {
                        pipe.hset(
                            self.entry_key(trash_ino),
                            self.trash_entry(parent, ino, &name),
                            bincode::serialize(&(typ, ino)).unwrap(),
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
                        INodeType::File => {
                            if let Some(sid) = open_session_id {
                                pipe.set(self.inode_key(ino), bincode::serialize(&attr)?);
                                pipe.sadd(self.sustained(sid), &ino);
                                (0, 0)
                            } else {
                                let ts = SystemTime::now()
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_secs();
                                pipe.zadd(
                                    self.del_files(),
                                    RedisHandle::to_delete(ino, attr.length),
                                    ts,
                                );
                                pipe.del(self.inode_key(ino));
                                let new_space = -utils::align_4k(attr.length);
                                pipe.incr(self.used_space_key(), new_space);
                                pipe.decr(self.total_inodes_key(), 1);
                                (new_space, -1)
                            }
                        }
                        INodeType::Symlink => {
                            pipe.del(self.inode_key(ino));
                            pipe.del(self.inode_key(ino));
                            let new_space = -utils::align_4k(0);
                            pipe.incr(self.used_space_key(), new_space);
                            info!("delete inode ({ino})");
                            pipe.decr(self.total_inodes_key(), 1);
                            (new_space, -1)
                        }
                        _ => {
                            pipe.del(self.inode_key(ino));
                            let new_space = -utils::align_4k(0);
                            pipe.incr(self.used_space_key(), new_space);
                            info!("delete inode ({ino})");
                            pipe.decr(self.total_inodes_key(), 1);
                            (new_space, -1)
                        }
                    };
                    pipe.del(self.xattr_key(ino));
                    if attr.parent == 0 {
                        pipe.del(self.parent_key(ino));
                    }
                    guaga
                };
                match pipe.query_async::<Option<Value>>(conn).await? {
                    Some(_) => Ok::<Option<_>, MetaError>(Some((
                        guaga.0,
                        guaga.1,
                        should_del,
                        open_session_id,
                        ino,
                        attr.clone(),
                    ))),
                    None => Ok(None)
                }
            })?;

        if trash.is_none() {
            if should_del {
                let option = if open_session_id.is_some() {
                    DeleteFileOption::Deferred
                } else {
                    DeleteFileOption::Immediate {
                        force: parent.is_trash(),
                    }
                };
                self.try_spawn_delete_file(ino, attr.length, option).await
            }
            self.update_stats(space_guage, ino_cnt_guage).await;
        }
        Ok(attr)
    }

    async fn do_rmdir(&self, parent: Ino, name: &str, skip_check_trash: bool) -> Result<Ino> {
        let mut trash = if !skip_check_trash {
            self.check_trash(parent).await?
        } else {
            None
        };

        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        let ino = async_transaction!(conn, &[self.inode_key(parent), self.entry_key(parent)], {
            let (typ, ino, _) = self.do_lookup_entry(conn, parent, name).await?;
            if typ != INodeType::Directory {
                return NotDir1Snafu { parent, name }.fail()?;
            }
            // first find ino by parent ino and son name, then watch it
            cmd("WATCH")
                .arg(&[self.inode_key(ino), self.entry_key(ino)])
                .query_async(conn)
                .await?;
            let (pattr_bytes, attr_bytes): (Vec<u8>, Option<Vec<u8>>) = conn
                .mget((self.inode_key(parent), self.inode_key(ino)))
                .await?;
            let mut pattr = bincode::deserialize::<Attr>(&pattr_bytes)?;
            // double check: parent mut be a dir and access mode
            if pattr.typ != INodeType::Directory {
                return NotDir2Snafu { ino: parent }.fail()?;
            }
            self.access(parent, ModeMask::WRITE | ModeMask::EXECUTE, &pattr)
                .await?;
            ensure!(
                !pattr.flags.intersects(Flag::APPEND.union(Flag::IMMUTABLE)),
                OpNotPermittedSnafu {
                    op: "rm dir is append or immutable.",
                }
            );

            // change time
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            pattr.nlink -= 1;
            pattr.mtime = now;
            pattr.ctime = now;

            let cnt: u64 = conn.hlen(self.entry_key(ino)).await?;
            if cnt > 0 {
                return DirNotEmptySnafu { parent, name }.fail()?;
            }
            let attr = match attr_bytes {
                Some(attr_bytes) => {
                    let mut attr = bincode::deserialize::<Attr>(&attr_bytes)?;
                    let uid = self.uid().clone();
                    if !uid.is_root() && pattr.mode & 0o1000 != 0 {
                        ensure!(uid == pattr.uid, PermissionDeniedSnafu { ino: pattr.uid });
                        ensure!(uid == attr.uid, PermissionDeniedSnafu { ino: attr.uid });
                    }
                    if let Some(trash_ino) = trash {
                        attr.ctime = now;
                        attr.parent = trash_ino;
                    }
                    attr
                }
                None => {
                    warn!("no attribute for inode {}  ({}, {})", ino, parent, name);
                    trash.take();
                    Attr::default()
                }
            };

            // make trascation
            let mut pipe = pipe();
            pipe.atomic();
            pipe.hdel(self.entry_key(parent), &name);
            if !parent.is_trash() {
                pipe.set(self.inode_key(parent), bincode::serialize(&pattr).unwrap());
            }
            match trash {
                Some(trash_ino) => {
                    pipe.set(self.inode_key(ino), bincode::serialize(&attr).unwrap());
                    pipe.hset(
                        self.entry_key(trash_ino),
                        self.trash_entry(parent, ino, &name),
                        bincode::serialize(&(typ, ino)).unwrap(),
                    );
                }
                None => {
                    pipe.del(self.inode_key(ino));
                    pipe.del(self.xattr_key(ino));
                    pipe.incr(self.used_space_key(), -utils::align_4k(0));
                    pipe.decr(self.total_inodes_key(), 1);
                }
            }
            pipe.hdel(self.dir_data_length_key(), ino);
            pipe.hdel(self.dir_used_space_key(), ino);
            pipe.hdel(self.dir_used_inodes_key(), ino);
            pipe.hdel(self.dir_quota_key(), ino);
            pipe.hdel(self.dir_quota_used_space_key(), ino);
            pipe.hdel(self.dir_quota_used_inodes_key(), ino);
            match pipe.query_async::<Option<Value>>(conn).await? {
                Some(_) => {
                    info!("delete inode ({ino})");
                    Ok::<_, MetaError>(Some(ino))
                },
                None => Ok::<_, MetaError>(None),
            }
        })?;
        if trash.is_none() {
            self.update_stats(-utils::align_4k(0), -1).await;
        }
        Ok(ino)
    }

    async fn do_read_symlink(&self, inode: Ino, no_atime: bool) -> Result<(Option<u128>, String)> {
        let mut share_conn = self.exclusive_conn().await?;
        let conn = share_conn.deref_mut();
        if no_atime {
            let path: Option<String> = conn.get(self.sym_key(inode)).await?;
            return Ok((None, path.unwrap_or(String::from(""))));
        }

        async_transaction!(conn, &[self.inode_key(inode)], {
            let keys = [self.inode_key(inode), self.sym_key(inode)];
            let rs: Vec<Option<Vec<u8>>> = conn.get(&keys).await?;
            if rs.len() != 2 {
                whatever!("Illigal len for mget {keys:?}, {:?}", keys);
            }
            ensure!(rs[0].is_some(), NoEntryFound2Snafu { ino: inode });
            let mut attr: Attr = bincode::deserialize(&rs[0].as_ref().unwrap())?;
            ensure!(
                attr.typ == INodeType::Symlink,
                InvalidArgSnafu {
                    arg: format!("inode({}) is not a symlink", inode)
                }
            );
            let path = match &rs[1] {
                Some(path) => match String::from_utf8(path.clone()) {
                    Ok(path) => path,
                    Err(e) => {
                        error!("Invalid symbol path: {:?}", e);
                        continue;
                    }
                },
                None => whatever!("symlink path is empty"),
            };
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards");
            if !self.is_atime_need_update(&attr, now) {
                return Ok((None, path));
            }

            attr.atime = now.as_nanos();
            let mut pipe = pipe();
            pipe.atomic();
            pipe.set(self.inode_key(inode), bincode::serialize(&attr).unwrap());
            pipe.query_async(conn).await?;
            Ok(Some((Some(attr.atime), path)))
        })
    }

    async fn do_readdir(
        &self,
        inode: Ino,
        wantattr: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Entry>> {
        let mut conn = self.share_conn();

        let mut entries = Vec::new();
        let mut iter = conn
            .hscan::<String, Vec<u8>>(self.entry_key(inode))
            .await?
            .chunks(10000);
        while let Some(datas) = iter.next().await {
            if datas.len() % 2 != 0 {
                panic!(
                    "Invalid key: {} for readdir, len: {}",
                    self.entry_key(inode),
                    datas.len()
                );
            }
            for i in (0..datas.len()).step_by(2) {
                let (typ, ino): (INodeType, Ino) = bincode::deserialize(&datas[i + 1])?;
                let name = match String::from_utf8(datas[i].clone()) {
                    Ok(name) => {
                        if name.is_empty() {
                            warn!(
                                "corrupt entry with empty name: inode {} parent {}",
                                ino, inode
                            );
                            continue;
                        } else {
                            name
                        }
                    }
                    Err(e) => {
                        error!("Invalid key {} for {}", e, self.entry_key(inode));
                        continue;
                    }
                };
                let entry = Entry {
                    inode: ino,
                    name: name,
                    attr: Attr {
                        typ: typ,
                        ..Default::default()
                    },
                };
                entries.push(entry);
                if let Some(limit_s) = limit
                    && entries.len() >= limit_s
                {
                    break;
                }
            }
        }

        drop(iter);
        if wantattr && !entries.is_empty() {
            // TODO: multiple thread to fill attr
            let keys = entries
                .iter()
                .map(|e| self.inode_key(e.inode))
                .collect::<Vec<_>>();
            let rs: Vec<Option<Vec<u8>>> = conn.mget(keys).await?;
            for (idx, entry_buf) in rs.iter().enumerate() {
                // only find those has attr
                match entry_buf {
                    Some(entry_buf) => {
                        let attr: Attr = bincode::deserialize(entry_buf)?;
                        entries[idx].attr = attr;
                    }
                    None => warn!("no attribute for inode {}", entries[idx].inode),
                }
            }
        }
        Ok(entries)
    }

    async fn do_rename(
        &self,
        parent_src: Ino,
        name_src: &str,
        parent_dst: Ino,
        name_dst: &str,
        flags: RenameMask,
    ) -> Result<(Ino, Attr, Option<(Ino, Attr)>)> {
        let mut watch_keys = vec![
            self.inode_key(parent_src),
            self.entry_key(parent_src),
            self.inode_key(parent_dst),
            self.entry_key(parent_dst),
        ];
        if parent_src.is_trash() {
            watch_keys.swap(0, 2);
        }
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();

        let (ino_src, attr_src, exists_entry, trash, delete_option, new_space, new_inode) =
            async_transaction!(conn, &watch_keys, {
                let (typ_src, ino_src, name_src) =
                    self.do_lookup_entry(conn, parent_src, name_src).await?;
                // because maybe case insensitive, so we need to check after lookup
                if parent_src == parent_dst && name_src == name_dst {
                    return RenameSameInoSnafu.fail()?;
                }
                let mut watch_keys = vec![self.inode_key(ino_src)];
                let exists_dst = match self.do_lookup_entry(conn, parent_dst, name_dst).await {
                    Ok((typ, ino, name)) => Some((typ, ino, name)),
                    Err(err) => {
                        if let MetaErrorEnum::NoEntryFound { parent, name } = err.inner()
                            && *parent == parent_dst
                            && name == name_dst
                        {
                            None
                        } else {
                            return Err(err);
                        }
                    }
                };
                let mut trash = None;
                // check if dst inode exists
                if let Some((ref typ_dst, ref ino_dst, _)) = exists_dst {
                    if flags.intersects(RenameMask::NOREPLACE) {
                        return EntryExistsSnafu {
                            parent: parent_dst,
                            name: name_dst.to_string(),
                            exist_ino: *ino_dst,
                            exist_attr: None,
                        }
                        .fail()?;
                    }
                    watch_keys.push(self.inode_key(*ino_dst));
                    if *typ_dst == INodeType::Directory {
                        watch_keys.push(self.entry_key(*ino_dst));
                    }
                    if !flags.intersects(RenameMask::EXCHANGE) {
                        self.check_trash(parent_dst)
                            .await?
                            .map(|t| trash.replace(t));
                    }
                }
                cmd("WATCH").arg(watch_keys).query_async(conn).await?;

                // data check
                let inodes = {
                    let mut inodes = vec![parent_src, parent_dst, ino_src];
                    exists_dst.iter().for_each(|dst| inodes.push(dst.1));
                    inodes
                };
                let keys = inodes
                    .iter()
                    .map(|inode| self.inode_key(*inode))
                    .collect::<Vec<String>>();
                let rs = conn.mget::<_, Vec<Option<Vec<u8>>>>(&keys).await?;
                if rs.len() != keys.len() {
                    whatever!("Illigal len for mget {keys:?}, {}", rs.len());
                }
                for (r, ino) in rs.iter().zip(inodes.iter()) {
                    if r.is_none() {
                        error!("no attribute for inode {}", ino);
                        return NoEntryFound2Snafu { ino: *ino }.fail()?;
                    }
                }
                let mut pattr_src: Attr = bincode::deserialize(rs[0].as_ref().unwrap())?;
                if pattr_src.typ != INodeType::Directory {
                    return NotDir2Snafu { ino: parent_src }.fail()?;
                }
                self.access(
                    parent_src,
                    ModeMask::WRITE.union(ModeMask::EXECUTE),
                    &pattr_src,
                )
                .await?;
                let mut pattr_dst: Attr = bincode::deserialize(rs[1].as_ref().unwrap())?;
                if pattr_dst.typ != INodeType::Directory {
                    return NotDir2Snafu { ino: parent_dst }.fail()?;
                }
                // TODO: why dst parent inode can't be trash
                if flags.intersects(RenameMask::RESTORE) && pattr_dst.parent > TRASH_INODE {
                    return NoEntryFound2Snafu { ino: parent_dst }.fail()?;
                }
                self.access(
                    parent_dst,
                    ModeMask::WRITE.union(ModeMask::EXECUTE),
                    &pattr_dst,
                )
                .await?;
                ensure!(
                    ino_src != parent_dst && ino_src != pattr_dst.parent,
                    OpNotPermittedSnafu {
                        op: "rename target dir is or subdir of source inode.",
                    }
                );

                let mut attr_src: Attr = bincode::deserialize(rs[2].as_ref().unwrap())?;
                if pattr_src
                    .flags
                    .intersects(Flag::APPEND.union(Flag::IMMUTABLE))
                    || pattr_dst.flags.intersects(Flag::IMMUTABLE)
                    || attr_src
                        .flags
                        .intersects(Flag::APPEND.union(Flag::IMMUTABLE))
                {
                    return OpNotPermittedSnafu { op: "todo." }.fail()?;
                }
                if parent_src != parent_dst
                    && pattr_src.mode & 0o1000 != 0
                    && !self.uid().is_root()
                    && *self.uid() != attr_src.uid
                    && (self.uid() != &pattr_src.uid || attr_src.typ == INodeType::Directory)
                {
                    return PermissionDeniedSnafu { ino: pattr_src.uid }.fail()?;
                }

                // move dst inode to src inode if exchange
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Time went backwards");
                let mut update_parent_dst = false;
                let mut update_parent_src = false;
                let mut delete_option = None;
                let exists_attr_dst = if let Some((ref typ_dst, ref ino_dst, _)) = exists_dst {
                    let mut attr_dst: Attr = bincode::deserialize(rs[3].as_ref().unwrap())?;
                    ensure!(
                        !attr_dst
                            .flags
                            .intersects(Flag::APPEND.union(Flag::IMMUTABLE)),
                        OpNotPermittedSnafu {
                            op: "rename target inode is append or immutable."
                        }
                    );
                    attr_dst.ctime = now.as_nanos();
                    if flags.intersects(RenameMask::EXCHANGE) {  // exchange
                        if parent_src != parent_dst {
                            if *typ_dst == INodeType::Directory {
                                attr_dst.parent = parent_src;
                                pattr_src.nlink += 1;
                                pattr_dst.nlink -= 1;
                                (update_parent_src, update_parent_dst) = (true, true);
                            } else if attr_dst.parent > 0 {
                                attr_dst.parent = parent_src;
                            }
                        }
                    } else {  // replace
                        if *typ_dst == INodeType::Directory {
                            let cnt: u64 = conn.hlen(self.entry_key(*ino_dst)).await?;
                            if cnt != 0 {
                                return DirNotEmptySnafu {
                                    parent: parent_dst,
                                    name: name_dst.to_string(),
                                }
                                .fail()?;
                            }
                            pattr_dst.nlink -= 1;
                            update_parent_dst = true;
                            trash.iter().for_each(|t| attr_dst.parent = *t);
                        } else {
                            if let Some(trash_ino) = trash {
                                if attr_dst.parent > 0 {
                                    attr_dst.parent = trash_ino;
                                }
                            } else {
                                attr_dst.nlink -= 1;
                                if *typ_dst == INodeType::File && attr_dst.nlink == 0 {
                                    delete_option = if self.meta.open_files.has_any_open(*ino_dst).await{
                                        if self.meta.sid.read().is_some() {
                                            Some(DeleteFileOption::Deferred)
                                        } else {
                                            Some(DeleteFileOption::Immediate { force: false })
                                        }
                                    } else {
                                        None
                                    };
                                    // TODO: InvalidateChunk
                                }
                                let file = self.meta.open_files.lock(*ino_dst);
                                let mut of = file.lock().await;
                                of.invalidate_chunk(INVALIDATE_ATTR_ONLY);
                            }
                        }
                    }
                    if !self.uid().is_root()
                        && pattr_dst.mode & 0o1000 != 0
                        && *self.uid() != pattr_dst.uid
                        && *self.uid() != attr_dst.uid
                    {
                        return PermissionDeniedSnafu { ino: pattr_dst.uid }.fail()?;
                    }
                    Some(attr_dst)
                } else {
                    if flags.intersects(RenameMask::EXCHANGE) {
                        return NoEntryFoundSnafu {
                            parent: parent_dst,
                            name: name_dst.to_string(),
                        }
                        .fail()?;
                    }
                    None
                };

                // check src inode
                if !self.uid().is_root()
                    && pattr_src.mode & 0o1000 != 0
                    && *self.uid() != pattr_src.uid
                    && *self.uid() != attr_src.uid
                {
                    return PermissionDeniedSnafu { ino: pattr_src.uid }.fail()?;
                }
                // move src inode to dst
                if parent_src != parent_dst {
                    if typ_src == INodeType::Directory {
                        attr_src.parent = parent_dst;
                        pattr_src.nlink -= 1;
                        pattr_dst.nlink += 1;
                        (update_parent_src, update_parent_dst) = (true, true);
                    } else if attr_src.parent > 0 {
                        attr_src.parent = parent_dst;
                    }
                }

                // change time
                if update_parent_src
                    || now.as_nanos() - pattr_src.mtime >= self.meta.conf.skip_dir_mtime.as_nanos()
                {
                    pattr_src.mtime = now.as_nanos();
                    pattr_src.ctime = now.as_nanos();
                    update_parent_src = true;
                }
                if update_parent_dst
                    || now.as_nanos() - pattr_dst.mtime >= self.meta.conf.skip_dir_mtime.as_nanos()
                {
                    pattr_dst.mtime = now.as_nanos();
                    pattr_dst.ctime = now.as_nanos();
                    update_parent_dst = true;
                }
                attr_src.ctime = now.as_nanos();

                // atomic pipe
                let (mut new_space, mut new_inode) = (0, 0);
                let mut pipe = pipe();
                pipe.atomic();
                // update dst ino
                if flags.intersects(RenameMask::EXCHANGE)
                    && let Some((ref typ_dst, ref ino_dst, _)) = exists_dst
                    && let Some(ref attr_dst) = exists_attr_dst
                {
                    // do exchange
                    pipe.hset(
                        self.entry_key(parent_src),
                        name_src,
                        bincode::serialize(&(typ_dst, ino_dst)).unwrap(),
                    );
                    pipe.set(
                        self.inode_key(*ino_dst),
                        bincode::serialize(attr_dst).unwrap(),
                    );
                    if parent_src != parent_dst && attr_dst.parent == 0 {
                        pipe.hincr(self.parent_key(*ino_dst), parent_src, 1);
                        pipe.hincr(self.parent_key(*ino_dst), parent_dst, -1);
                    }
                } else {
                    // replace or insert
                    pipe.hdel(self.entry_key(parent_src), name_src);
                    if let Some((ref typ_dst, ref ino_dst, _)) = exists_dst
                        && let Some(ref attr_dst) = exists_attr_dst
                    {
                        // do replace
                        let (typ_dst, ino_dst) = (typ_dst.clone(), ino_dst.clone());
                        if let Some(trash_ino) = trash {
                            pipe.set(
                                self.inode_key(trash_ino),
                                bincode::serialize(attr_dst).unwrap(),
                            );
                            pipe.hset(
                                self.entry_key(trash_ino),
                                self.trash_entry(parent_dst, ino_dst, name_dst),
                                bincode::serialize(&(&typ_dst, ino_dst)).unwrap(),
                            );
                            if attr_dst.parent == 0 {
                                pipe.hincr(self.parent_key(ino_dst), trash_ino, 1);
                                pipe.hincr(self.parent_key(ino_dst), parent_dst, -1);
                            }
                        } else if typ_dst != INodeType::Directory && attr_dst.nlink > 0 {
                            pipe.set(
                                self.inode_key(ino_dst),
                                bincode::serialize(&attr_dst).unwrap(),
                            );
                            if attr_dst.parent == 0 {
                                pipe.hincr(self.parent_key(ino_dst), parent_dst, -1);
                            }
                        } else {
                            if typ_dst == INodeType::File {
                                if delete_option.is_some() {
                                    pipe.set(
                                        self.inode_key(ino_dst),
                                        bincode::serialize(&attr_dst).unwrap(),
                                    );
                                    self.meta
                                        .sid
                                        .read()
                                        .map(|sid| pipe.sadd(self.sustained(sid), ino_dst));
                                } else {
                                    pipe.zadd(
                                        self.del_files(),
                                        RedisHandle::to_delete(ino_dst, attr_dst.length),
                                        now.as_secs(),
                                    );
                                    pipe.del(self.inode_key(ino_dst));
                                    (new_space, new_inode) =
                                        (-utils::align_4k(attr_dst.length), 1);
                                    pipe.incr(self.used_space_key(), new_space);
                                    pipe.decr(self.total_inodes_key(), new_inode);
                                }
                            } else {
                                if typ_dst == INodeType::Symlink {
                                    pipe.del(self.sym_key(ino_dst));
                                }
                                pipe.del(self.inode_key(ino_dst));
                                (new_space, new_inode) = (-utils::align_4k(0), 1);
                                pipe.incr(self.used_space_key(), new_space);
                                pipe.decr(self.total_inodes_key(), new_inode);
                            }
                            pipe.del(self.xattr_key(ino_dst));
                            if attr_dst.parent == 0 {
                                pipe.del(self.parent_key(ino_dst));
                            }
                        }
                        if typ_dst == INodeType::Directory {
                            pipe.hdel(self.dir_quota_key(), ino_dst);
                            pipe.hdel(self.dir_quota_used_space_key(), ino_dst);
                            pipe.hdel(self.dir_quota_used_inodes_key(), ino_dst);
                        }
                    }
                }

                // update parent src ino
                if parent_dst != parent_src {
                    if parent_src.is_trash() && update_parent_src {
                        pipe.set(
                            self.inode_key(parent_src),
                            bincode::serialize(&pattr_src).unwrap(),
                        );
                    }
                    if attr_src.parent == 0 {
                        pipe.hincr(self.parent_key(ino_src), parent_dst, 1);
                        pipe.hincr(self.parent_key(ino_src), parent_src, -1);
                    }
                }
                // update src ino
                pipe.set(
                    self.inode_key(ino_src),
                    bincode::serialize(&attr_src).unwrap(),
                );
                pipe.hset(
                    self.entry_key(parent_dst),
                    name_dst,
                    bincode::serialize(&(typ_src, ino_src)).unwrap(),
                );
                // update parent dst ino
                if update_parent_dst {
                    pipe.set(
                        self.inode_key(parent_dst),
                        bincode::serialize(&pattr_dst).unwrap(),
                    );
                }
                pipe.query_async(conn).await?;
                Ok::<_, MetaErrorEnum>(Some((
                    ino_src,
                    attr_src,
                    exists_dst.map(|dst| (dst.1, exists_attr_dst.unwrap())),
                    trash,
                    delete_option,
                    new_space,
                    new_inode,
                )))
            })?;

        // trash is none and not exchange, should delete file.
        if !flags.intersects(RenameMask::EXCHANGE) && trash.is_none() {
            if let Some(ref entry) = exists_entry
                && let Some(delete_option) = delete_option
            {
                self.try_spawn_delete_file(entry.0, entry.1.length, delete_option)
                    .await
            }
            self.update_stats(new_space, new_inode).await;
        }
        Ok((ino_src, attr_src, exists_entry))
    }

    async fn do_set_xattr(
        &self,
        inode: Ino,
        name: &str,
        value: Vec<u8>,
        flags: XattrF,
    ) -> Result<()> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &self.xattr_key(inode), {
            let key = self.xattr_key(inode);
            match flags {
                XattrF::CREATE => {
                    let ok = conn.hset_nx::<_, _, _, bool>(key, name, &value).await?;
                    ensure!(
                        ok,
                        EntryExists2Snafu {
                            ino: inode,
                            exist_attr: None
                        }
                    );
                }
                XattrF::REPLACE => {
                    let ok = conn.hexists::<_, _, bool>(key, name).await?;
                    ensure!(ok, NoSuchAttrSnafu);
                }
                _ => {
                    conn.hset(key, name, &value).await?;
                }
            }
            Ok::<_, MetaError>(Some(()))
        })
    }

    async fn do_get_xattr(&self, inode: Ino, name: &str) -> Result<Vec<u8>> {
        let inode = inode.transfer_root(self.meta.root);
        let mut conn = self.share_conn();
        let vbuff = conn
            .hget::<_, _, Option<Vec<u8>>>(self.xattr_key(inode), name)
            .await?;
        ensure!(vbuff.is_some(), NoSuchAttrSnafu);
        Ok(vbuff.unwrap())
    }

    async fn do_list_xattr(&self, inode: Ino) -> Result<Vec<u8>> {
        let inode = inode.transfer_root(self.meta.root);
        let mut conn = self.share_conn();
        let vals = conn
            .hkeys::<_, Option<Vec<Vec<u8>>>>(self.xattr_key(inode))
            .await?;
        ensure!(vals.is_some(), NoEntryFound2Snafu { ino: inode });
        let mut names = Vec::new();
        for name in vals.unwrap() {
            names.extend_from_slice(&name);
            names.push(0_u8);
        }

        let val = conn
            .get::<_, Option<Vec<u8>>>(self.inode_key(inode))
            .await?;
        ensure!(val.is_some(), NoEntryFound2Snafu { ino: inode });
        let attr: Attr = bincode::deserialize(&val.unwrap())?;

        if attr.access_acl != 0 {
            names.extend_from_slice(b"system.posix_acl_access");
            names.push(0_u8);
        }
        if attr.default_acl != 0 {
            names.extend_from_slice(b"system.posix_acl_default");
            names.push(0_u8);
        }
        Ok(names)
    }

    async fn do_remove_xattr(&self, inode: Ino, name: &str) -> Result<()> {
        let mut conn = self.share_conn();
        let n = conn
            .hdel::<_, _, Option<u32>>(self.xattr_key(inode), name)
            .await?;
        ensure!(n.is_some(), NoEntryFound2Snafu { ino: inode });
        ensure!(n.unwrap() != 0, NoSuchAttrSnafu);
        Ok(())
    }

    async fn do_repair(&self, inode: Ino, attr: &mut Attr) -> Result<()> {
        unimplemented!()
    }

    async fn do_touch_atime(&self, inode: Ino, ts: Duration) -> Result<Attr> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, self.inode_key(inode), {
            let attr_bytes = conn
                .get::<_, Option<Vec<u8>>>(self.inode_key(inode))
                .await?;
            ensure!(attr_bytes.is_some(), NoEntryFound2Snafu { ino: inode });
            let mut attr: Attr = bincode::deserialize(&attr_bytes.unwrap())?;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards");
            if !self.is_atime_need_update(&attr, now) {
                return Ok(attr);
            }
            attr.atime = ts.as_nanos();
            let mut pipe = pipe();
            pipe.atomic();
            pipe.set(self.inode_key(inode), bincode::serialize(&attr).unwrap());
            pipe.query_async(conn).await?;
            Ok::<_, MetaError>(Some(attr))
        })
    }

    async fn do_read(&self, inode: Ino, indx: u32) -> Result<Option<PSlices>> {
        let mut conn = self.share_conn();
        Ok(conn.lrange(self.chunk_key(inode, indx), 0, -1).await?)
    }

    async fn do_write(
        &self,
        inode: Ino,
        indx: u32,
        off: u32,
        slice: Slice,
        mtime: Duration,
    ) -> Result<(u32, DirStat, Attr)> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();
        async_transaction!(conn, &[self.inode_key(inode)], {
            let attr_bytes = conn
                .get::<_, Option<Vec<u8>>>(self.inode_key(inode))
                .await?;
            ensure!(attr_bytes.is_some(), NoEntryFound2Snafu { ino: inode });
            let mut attr: Attr = bincode::deserialize(&attr_bytes.unwrap())?;
            ensure!(
                attr.typ == INodeType::File,
                OpNotPermittedSnafu {
                    op: "write into a non file"
                }
            );
            let new_len = CHUNK_SIZE * indx as u64 + off as u64 + slice.len as u64;
            let delta = if new_len > attr.length {
                attr.length = new_len;
                DirStat {
                    length: (new_len - attr.length) as i64,
                    space: (utils::align_4k(new_len) - utils::align_4k(attr.length)) as i64,
                    inodes: 0,
                }
            } else {
                DirStat::default()
            };
            let parents = if attr.parent > 0 {
                vec![attr.parent]
            } else {
                self.do_get_parents_inner(conn, inode)
                    .await
                    .into_keys()
                    .collect()
            };
            self.check_quotas(delta.space, 0, parents).await?;

            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            attr.mtime = now;
            attr.ctime = now;

            let mut pipe = pipe();
            pipe.atomic();
            pipe.rpush(self.chunk_key(inode, indx), PSlice::new(&slice, off));
            // most of chunk are used by single inode, so use that as the default (1 == not exists)
            // pipe.Incr(ctx, r.sliceKey(slice.ID, slice.Size))
            pipe.set(self.inode_key(inode), bincode::serialize(&attr).unwrap())
                .ignore();
            if delta.space > 0 {
                pipe.incr(self.used_space_key(), delta.space).ignore();
            }
            let rs: Vec<u64> = pipe.query_async(conn).await?;
            ensure_whatever!(rs.len() == 1, "Invalid len for write: {:?}", rs);
            let num_slices = rs[0] as u32;
            Ok::<_, MetaError>(Some((num_slices, delta, attr)))
        })
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
        flag: Falloc,
        off: u64,
        size: u64,
    ) -> Result<(DirStat, Attr)> {
        let mut pool_conn = self.exclusive_conn().await?;
        let conn = pool_conn.deref_mut();

        async_transaction!(conn, &[self.inode_key(inode)], {
            let attr_bytes = conn
                .get::<_, Option<Vec<u8>>>(self.inode_key(inode))
                .await?;
            ensure!(attr_bytes.is_some(), NoEntryFound2Snafu { ino: inode });
            let mut attr: Attr = bincode::deserialize(&attr_bytes.unwrap())?;
            ensure!(attr.typ != INodeType::FIFO, BrokenPipeSnafu { ino: inode });
            ensure!(
                attr.typ == INodeType::File,
                OpNotPermittedSnafu {
                    op: "fallocate a non file"
                }
            );
            ensure!(
                !attr.flags.intersects(Flag::IMMUTABLE),
                OpNotPermittedSnafu {
                    op: "fallocate a immutable  file"
                }
            );
            self.access(inode, ModeMask::WRITE, &attr).await?;
            if attr.flags.intersects(Flag::APPEND) && !flag.difference(Falloc::KEEP_SIZE).is_empty()
            {
                return OpNotPermittedSnafu {
                    op: "fallocate a append file but not keep size".to_string(),
                }
                .fail()?;
            }

            let length = if off + size > attr.length && !flag.intersects(Falloc::KEEP_SIZE) {
                off + size
            } else {
                attr.length
            };
            let old = attr.length;
            let delta = DirStat {
                length: length as i64 - old as i64,
                space: align_4k(length) - align_4k(old),
                inodes: 0,
            };
            let parents = if attr.parent > 0 {
                vec![attr.parent]
            } else {
                self.do_get_parents_inner(conn, inode)
                    .await
                    .into_keys()
                    .collect()
            };
            self.check_quotas(delta.space, 0, parents).await?;

            // modify attr
            attr.length = length;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            attr.mtime = now;
            attr.ctime = now;
            let mut pipe = pipe();
            pipe.atomic();
            pipe.set(self.inode_key(inode), bincode::serialize(&attr).unwrap());
            if flag.intersects(Falloc::ZERO_RANGE.union(Falloc::PUNCH_HOLE)) && off < old {
                let (mut off, mut size) = (off, size);
                if off + size > old {
                    size = old - off;
                }
                while size > 0 {
                    let indx = (off / CHUNK_SIZE) as u32;
                    let coff = off % CHUNK_SIZE;
                    let len = if coff + size > CHUNK_SIZE {
                        CHUNK_SIZE - coff
                    } else {
                        size
                    };
                    pipe.rpush(
                        self.chunk_key(inode, indx),
                        PSlice::new(
                            &Slice {
                                id: 0,
                                size: 0,
                                off: 0,
                                len: len as u32,
                            },
                            coff as u32,
                        ),
                    );
                    off += len;
                    size -= len;
                }
            }
            pipe.incr(self.used_space_key(), align_4k(length) - align_4k(old));
            pipe.query_async(conn).await?;
            Ok::<_, MetaError>(Some((delta, attr)))
        })
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
        /*
            vals, err := m.rdb.HGetAll(ctx, m.parentKey(inode)).Result()
        if err != nil {
            logger.Warnf("Scan parent key of inode %d: %s", inode, err)
            return nil
        }
        ps := make(map[Ino]int)
        for k, v := range vals {
            if n, _ := strconv.Atoi(v); n > 0 {
                ino, _ := strconv.ParseUint(k, 10, 64)
                ps[Ino(ino)] = n
            }
        }
        return ps
             */
        unimplemented!()
    }

    async fn do_flush_dir_stat(&self, batch: HashMap<Ino, DirStat>) -> Result<()> {
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

    async fn do_set_facl(&self, ino: Ino, acl_type: AclType, rule: &Rule) -> Result<()> {
        unimplemented!()
    }

    async fn do_get_facl(&self, ino: Ino, acl_type: AclType, acl_id: AclId) -> Result<Rule> {
        unimplemented!()
    }

    async fn cache_acls(&self) -> Result<()> {
        if !self.get_format().enable_acl {
            return Ok(());
        }
        /*
        vals, err := m.rdb.HGetAll(ctx, m.aclKey()).Result()
        if err != nil {
            return err
        }

        for k, v := range vals {
            id, _ := strconv.ParseUint(k, 10, 32)
            tmpRule := &aclAPI.Rule{}
            tmpRule.Decode([]byte(v))
            m.aclCache.Put(uint32(id), tmpRule)
        }
        return nil
             */
        unimplemented!()
    }
}
