use std::sync::atomic::AtomicI64;

pub struct Quota {
    pub max_space: AtomicI64,
    pub max_inodes: AtomicI64,
    pub used_space: AtomicI64,
    pub used_inodes: AtomicI64,
    pub new_space: AtomicI64,
    pub new_inodes: AtomicI64,
}

impl Quota {
    // Returns true if it will exceed the quota limit
    pub fn check(&self, space: i64, inodes: i64) -> bool {
        if space > 0 {
            let max = self.max_space.load(std::sync::atomic::Ordering::SeqCst);
            if max > 0
                && self.used_space.load(std::sync::atomic::Ordering::SeqCst)
                    + self.new_space.load(std::sync::atomic::Ordering::SeqCst)
                    + space
                    > max
            {
                return true;
            }
        }
        if inodes > 0 {
            let max = self.max_inodes.load(std::sync::atomic::Ordering::SeqCst);
            if max > 0
                && self.used_inodes.load(std::sync::atomic::Ordering::SeqCst)
                    + self.new_inodes.load(std::sync::atomic::Ordering::SeqCst)
                    + inodes
                    > max
            {
                return true;
            }
        }
        return false;
    }
}
