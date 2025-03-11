use std::cmp::{max, min};


#[derive(Clone)]
pub struct Frange {
    pub off: u64,
    pub len: u64,
}

impl Frange {
    pub fn end(&self) -> u64 {
        self.off + self.len
    }

    pub fn is_overlap(&self, other: &Frange) -> bool {
        self.off < other.off + other.len && other.off < self.off + self.len
    }

    pub fn overlap(&self, other: &Frange) -> Option<Frange> {
        if self.is_overlap(other) {
            Some(Frange {
                off: max(self.off, other.off),
                len: min(self.off + self.len, other.off + other.len) - max(self.off, other.off),
            })
        } else {
            None
        }
    }

    pub fn is_include(&self, other: &Frange) -> bool {
        self.off <= other.off && self.end() >= other.end()
    }

    pub fn is_contain(&self, p: u64) -> bool {
        self.off < p && p < self.end()
    }
}