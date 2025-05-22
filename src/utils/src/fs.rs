use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

pub struct FsContext {
    uid: u32,
    gid: u32,
    gids: Vec<u32>,
    pid: u32,
    check_permission: bool,
    start: DateTime<Utc>,
    token: CancellationToken,
}

impl FsContext {
    pub fn new(uid: u32, gid: u32, pid: u32, check_permission: bool) -> Self {
        Self {
            uid,
            gid,
            gids: vec![gid],
            pid,
            check_permission,
            start: Utc::now(),
            token: CancellationToken::new(),
        }
    }

    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

pub mod task_local {
    use chrono::{DateTime, Utc};
    use tokio::task_local;
    use tokio_util::sync::CancellationToken;

    use super::FsContext;

    task_local! {
        static TASK_LOCAL_FS_CTX: FsContext;
    }

    pub fn uid() -> Option<u32> {
        TASK_LOCAL_FS_CTX.try_with(|ctx| ctx.uid).ok()
    }

    pub fn gid() -> Option<u32> {
        TASK_LOCAL_FS_CTX.try_with(|ctx| ctx.gid).ok()
    }

    pub fn gids() -> Option<Vec<u32>> {
        TASK_LOCAL_FS_CTX.try_with(|ctx| ctx.gids.clone()).ok()
    }

    pub fn check_permission() -> Option<bool> {
        TASK_LOCAL_FS_CTX.try_with(|ctx| ctx.check_permission).ok()
    }

    pub fn start() -> Option<DateTime<Utc>> {
        TASK_LOCAL_FS_CTX.try_with(|ctx| ctx.start).ok()
    }

    pub fn token() -> Option<CancellationToken> {
        TASK_LOCAL_FS_CTX.try_with(|ctx| ctx.token.clone()).ok()
    }

    pub async fn scope<F>(ctx: FsContext, f: F) -> F::Output
    where
        F: Future,
    {
        TASK_LOCAL_FS_CTX.scope(ctx, f).await
    }
}
