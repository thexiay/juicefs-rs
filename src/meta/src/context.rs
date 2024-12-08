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