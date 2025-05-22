use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{api::ModeMask, context::{Gid, Uid}};
pub const ACL_COUNTER: &str = "acl_counter";

pub type AclId = u32;
pub type Checksum = u32;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rule {
    pub owner: u16,
    pub group: u16,
    pub mask: u16,
    pub other: u16,
    pub named_users: Entries,
    pub named_groups: Entries,
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
        Rule {
            owner: (mode >> 6) & 7 & self.owner,
            group: self.group,
            mask: (mode >> 3) & 7 & self.mask,
            other: mode & 7 & self.other,
            named_users: self.named_users.clone(),
            named_groups: self.named_groups.clone(),
        }
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
    cksum_2_id: HashMap<Checksum, Vec<AclId>>,
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl() {
        let rule = Rule {
            owner: 6,
            group: 4,
            mask: 4,
            other: 4,
            named_users: vec![
                Entry { id: 2, perm: 2 },
                Entry { id: 1, perm: 1 },
            ],
            named_groups: vec![
                Entry { id: 4, perm: 4 },
                Entry { id: 3, perm: 3 },
            ],
        };
        let mut c = AclCache::default();
        c.put(1, rule.clone());
        c.put(2, rule.clone());
        assert_eq!(c.get(1), Some(rule.clone()));
        assert_eq!(c.get(2), Some(rule.clone()));
        assert_eq!(c.get_id(&rule), Some(1));
        
        let mut rule2 = rule.clone();
        rule2.owner = 4;

        c.put(3, rule2.clone());
        assert_eq!(c.get_id(&rule2), Some(3));

        c.put(8, rule2.clone());
        assert_eq!(c.get_miss_ids(), vec![4, 5, 6, 7]);
    }
}
