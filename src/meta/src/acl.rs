use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{api::ModeMask, context::{Gid, Uid}};
pub const ACL_COUNTER: &str = "acl_counter";

pub type AclId = u32;
pub trait AclExt {
    fn is_valid_acl(&self) -> bool;
    fn invalid_acl() -> AclId {
        0
    }
}
impl AclExt for AclId {
    fn is_valid_acl(&self) -> bool {
        *self != 0
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub id: AclId,
    pub perm: u16,
}

pub type Entries = Vec<Entry>;

pub enum AclType {
    None,
    Access,
    Default,
}

#[derive(Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rule {
    owner: u16,
    group: u16,
    mask: u16,
    other: u16,
    named_users: Entries,
    named_groups: Entries,
}

impl Rule {
    pub fn can_access(
        &self,
        uid: &Uid,
        gids: &Vec<Gid>,
        f_uid: Uid,
        f_gid: Gid,
        m_mask: ModeMask,
    ) -> bool {
        if *uid == f_uid {
            return ModeMask::FULL & ModeMask::from_bits_truncate(self.owner as u8) & m_mask
                == m_mask;
        }
        for n_user in &self.named_users {
            if *uid == n_user.id {
                return ModeMask::FULL
                    & ModeMask::from_bits_truncate((n_user.perm & self.mask) as u8)
                    & m_mask
                    == m_mask;
            }
        }

        let mut is_grp_matched = false;
        for gid in gids {
            if *gid == f_gid {
                if ModeMask::FULL
                    & ModeMask::from_bits_truncate((self.group & self.mask) as u8)
                    & m_mask
                    == m_mask
                {
                    return true;
                }
                is_grp_matched = true;
            }
        }
        if is_grp_matched {
            return false;
        }

        ModeMask::FULL & ModeMask::from_bits_truncate(self.other as u8) & m_mask == m_mask
    }

    pub fn is_minimal(&self) -> bool {
        self.named_groups.is_empty() && self.named_users.is_empty() && self.mask == 0xFFFF
    }

    pub fn is_empty(&self) -> bool {
        self.named_groups.is_empty()
            && self.named_users.is_empty()
            && self.owner & self.group & self.other & self.mask == 0xFFFF
    }

    pub fn get_mode(&self) -> u16 {
        if self.is_minimal() {
            ((self.owner & 7) << 6) | ((self.group & 7) << 3) | (self.other & 7)
        } else {
            ((self.owner & 7) << 6) | ((self.mask & 7) << 3) | (self.other & 7)
        }
    }

    pub fn set_mode(&mut self, mode: u16) {
        self.owner &= 0xFFF8;
        self.owner |= (mode >> 6) & 7;

        if self.is_minimal() {
            self.group &= 0xFFF8;
            self.group |= (mode >> 3) & 7;
        } else {
            self.mask &= 0xFFF8;
            self.mask |= (mode >> 3) & 7;
        }
        self.other &= 0xFFF8;
        self.other |= mode & 7;
    }

    pub fn child_access_acl(&self, mode: u16) -> Rule {
        let mut c_rule = Rule::default();
        c_rule.owner = (mode >> 6) & 7 & self.owner;
        c_rule.mask = (mode >> 3) & 7 & self.mask;
        c_rule.other = mode & 7 & self.other;

        c_rule.group = self.group;
        c_rule.named_users = self.named_users.clone();
        c_rule.named_groups = self.named_groups.clone();
        c_rule
    }

    pub fn checksum(&self) -> u32 {
        // serialize will never fail
        crc32fast::hash(&bincode::serialize(self).unwrap())
    }
}

/// Cache all rules
/// - cache all rules when meta init.
/// - on getfacl failure, read and cache rule from meta.
/// - on setfacl success, read and cache all missed rules from meta. (considered as a lo/w-frequency operation)
/// - concurrent mounts may result in duplicate rules.
#[derive(Default)]
pub struct AclCache {
    max_id: AclId,
    id_2_rule: HashMap<AclId, Rule>,
    cksum_2_id: HashMap<AclId, Vec<AclId>>,
}

impl AclCache {
    pub fn put(&mut self, id: AclId, r: Rule) {
        if self.id_2_rule.contains_key(&id) {
            return;
        }
        if id > self.max_id {
            self.max_id = id;
        }

        let checksum = r.checksum();
        self.cksum_2_id.entry(checksum).or_insert(vec![]).push(id);
        self.id_2_rule.insert(id, r);
    }

    pub fn get(&self, id: AclId) -> Option<Rule> {
        self.id_2_rule.get(&id).map(|r| (*r).clone())
    }

    pub fn get_all(&self) -> HashMap<AclId, Rule> {
        self.id_2_rule.clone()
    }

    pub fn get_id(&self, r: &Rule) -> Option<AclId> {
        self.cksum_2_id
            .get(&r.checksum())
            .map(|ids| {
                ids.iter()
                    .find(|id| {
                        self.id_2_rule.get(id).map_or(false, |other| other == r)  
                    })
                    .map_or(AclId::invalid_acl(), |id| *id)
            })
    }

    pub fn size(&self) -> usize {
        self.id_2_rule.len()
    }

    pub fn get_miss_ids(&self) -> Vec<AclId> {
        if self.id_2_rule.len() == 0 {
            return vec![];
        }

        let n = self.max_id + 1;
        let mut ret = vec![];
        for i in 1..n {
            if !self.id_2_rule.contains_key(&i) {
                ret.push(i);
            }
        }
        ret
    }

    pub fn clear(&self) {}
}
