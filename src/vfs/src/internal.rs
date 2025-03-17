use juice_meta::api::{Attr, Ino};

use crate::vfs::VfsInner;

pub const MIN_INTERNAL_NODE: Ino = 0x7FFFFFFF00000000;
pub const LOG_INODE: Ino = MIN_INTERNAL_NODE + 1;
pub const CONTROL_INODE: Ino = MIN_INTERNAL_NODE + 2;
pub const STATS_INODE: Ino = MIN_INTERNAL_NODE + 3;
pub const CONFIG_INODE: Ino = MIN_INTERNAL_NODE + 4;
pub const TRASH_INODE: Ino = juice_meta::api::TRASH_INODE;

impl VfsInner {
    pub fn is_special_node(&self, name: &str) -> bool {
        if let Some(first) = name.chars().next() && first == '.' {
            self.internal_inos.iter().any(|ino| ino.name == name)
        } else {
            false
        }
    }

    pub fn is_special_inode(ino: Ino) -> bool {
        ino >= MIN_INTERNAL_NODE
    }
}