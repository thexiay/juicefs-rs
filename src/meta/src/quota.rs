
pub struct Quota {
    pub max_space: i64,
    pub max_inodes: i64,
    pub used_space: i64,
    pub used_inodes: i64,
    pub new_space: i64,
    pub new_inodes: i64,
}
