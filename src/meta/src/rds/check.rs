use std::sync::LazyLock;

use redis::InfoDict;
use tracing::warn;

pub static OLDEST_REDIS_VERSION: LazyLock<RedisVersion> = LazyLock::new(|| {
    RedisVersion {
        ver: "4.0.x".to_string(),
        major: 4,
        minor: 0,
    }
});

struct RedisVersion {
    ver: String,
    major: isize,
    minor: isize,
}

impl TryFrom<String> for RedisVersion {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() < 2 {
            return Err(format!("invalid redisVersion: {}", value));
        }
        let major = parts[0]
            .parse()
            .map_err(|e| format!("Failed to parse major version: {}", e))?;
        let minor = parts[1]
            .parse()
            .map_err(|e| format!("Failed to parse minor version: {}", e))?;
        Ok(RedisVersion {
            ver: value,
            major,
            minor,
        })
    }
}

impl RedisVersion {
    fn older_than(&self, v2: &RedisVersion) -> bool {
        if self.major < v2.major {
            return true;
        }
        if self.major > v2.major {
            return false;
        }
        self.minor < v2.minor
    }
}

pub struct RedisInfo {
    aof_enabled: bool,
    max_memory_policy: Option<String>,
    redis_version: Option<RedisVersion>,
    storage_provider: Option<String>,
}

impl RedisInfo {
    pub fn new(redis_info: InfoDict) -> Self {
        let aof_enabled = redis_info
            .get("aof_enabled")
            .map_or(false, |val: String| val == "1");
        if !aof_enabled {
            warn!("AOF is not enabled, you may lose data if Redis is not shutdown properly.");
        }
        let max_memory_policy = redis_info.get("maxmemory_policy");
        let redis_version = redis_info
            .get("redis_version")
            .map(|val: String| {
                RedisVersion::try_from(val.clone()).inspect(|r| {
                    if r.older_than(&OLDEST_REDIS_VERSION) {
                        panic!("Redis version should not be older than 4.0.x");
                    }
                }).map_err(|e| {
                    warn!("Failed to parse redis version: {}", e);
                }).ok()
            }).flatten();

                
        let storage_provider = match redis_info.get("storage_provider") {
            Some(r) if r == "flash" => Some(r),
            _ => None,
        };
        RedisInfo {
            aof_enabled,
            max_memory_policy,
            redis_version,
            storage_provider,
        }
    }
}
