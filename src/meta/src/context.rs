use dyn_clone::DynClone;
use tokio_util::sync::CancellationToken;

pub type Uid = u32;
pub type Gid = u32;

pub trait UserExt {
    fn is_root(&self) -> bool;
}

impl UserExt for Uid {
    fn is_root(&self) -> bool {
        *self == 0
    }
}

#[derive(Clone, Default)]
pub struct FsContext {
    uid: Uid,
    gid: Gid,
    gids: Vec<Gid>,
    token: CancellationToken,
    check_permission: bool,
}

pub trait WithContext: DynClone {
    /// After Clone, login to switch user
    fn with_login(&mut self, uid: Uid, gids: Vec<Gid>);

    /// After Clone, with cancel to start a subtask with cancel token
    fn with_cancel(&mut self, token: CancellationToken);

    /// Current uid
    fn uid(&self) -> &Uid;

    /// Current gid
    fn gid(&self) -> &Gid;

    /// Current gid list
    fn gids(&self) -> &Vec<Gid>;

    /// Current gid list
    fn token(&self) -> &CancellationToken;

    /// Whether check permission
    fn check_permission(&self) -> bool;
}

impl<T> WithContext for T
where
    T: AsMut<FsContext> + AsRef<FsContext> + DynClone,
{
    fn with_login(&mut self, uid: u32, gids: Vec<u32>) {
        assert!(gids.len() > 0);
		let fs = self.as_mut();
        fs.uid = uid;
        fs.gid = gids[0];
        fs.gids = gids;
    }

    fn with_cancel(&mut self, token: CancellationToken) {
        self.as_mut().token = token;
    }
    fn uid(&self) -> &u32 {
        &self.as_ref().uid
    }
    fn gid(&self) -> &u32 {
        &self.as_ref().gid
    }
    fn gids(&self) -> &Vec<u32> {
        &self.as_ref().gids
    }
    fn token(&self) -> &CancellationToken {
        &self.as_ref().token
    }
    fn check_permission(&self) -> bool {
        true
    }
}
