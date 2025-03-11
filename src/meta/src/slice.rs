use std::ops::Deref;

use async_trait::async_trait;
use redis::{ErrorKind, FromRedisValue, RedisError, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};

use crate::{
    api::{Ino, Slice},
    base::{CommonMeta, Engine},
};

/// PSlice is a tree with it's node is a data range with latest data.
#[derive(Serialize, Deserialize, Debug)]
pub struct PSlice {
    id: u64,
    size: u32,
    off: u32,
    len: u32,
    coff: u32,
    #[serde(skip)]
    left: Option<Box<PSlice>>,
    #[serde(skip)]
    right: Option<Box<PSlice>>,
}

impl PSlice {
    pub fn new(slice: &Slice, coff: u32) -> Self {
        PSlice {
            id: slice.id,
            size: slice.size,
            off: slice.off,
            len: slice.len,
            coff,
            left: None,
            right: None,
        }
    }

    fn cut(mut self, coff: u32) -> (Option<PSlice>, Option<PSlice>) {
        if coff <= self.coff {
            let left = self.left.take().or_else(|| {
                if self.coff - coff == 0 {
                    None
                } else {
                    Some(Box::new(PSlice {
                        id: 0,
                        size: 0,
                        off: 0,
                        len: self.coff - coff,
                        coff,
                        left: None,
                        right: None,
                    }))
                }
            });
            match left {
                Some(left) => {
                    let (left, right) = left.cut(coff);
                    self.left = right.map(Box::new);
                    (left, Some(self))
                }
                None => (None, Some(self)),
            }
        } else if coff < self.coff + self.len {
            let l = coff - self.coff;
            let mut right = PSlice {
                id: self.id,
                size: self.size,
                off: self.off + l,
                len: self.len - l,
                coff,
                left: None,
                right: None,
            };
            right.right = self.right.take();
            self.len = l;
            (Some(self), Some(right))
        } else {
            let right = self.right.take().or_else(|| {
                if coff - self.coff - self.len == 0 {
                    None
                } else {
                    Some(Box::new(PSlice {
                        id: 0,
                        size: 0,
                        off: 0,
                        len: coff - self.coff - self.len,
                        coff: self.coff + self.len,
                        left: None,
                        right: None,
                    }))
                }
            });
            match right {
                Some(right) => {
                    let (left, right) = right.cut(coff);
                    self.right = left.map(Box::new);
                    (Some(self), right)
                }
                None => (Some(self), None),
            }
        }
    }

    fn visit(&self, f: &mut impl FnMut(&PSlice)) {
        self.left.as_ref().map(|s| s.visit(f));
        f(self); // self could be freed
        self.right.as_ref().map(|s| s.visit(f));
    }
}

impl ToRedisArgs for PSlice {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let bytes = bincode::serialize(self).unwrap();
        out.write_arg(&bytes);
    }
}

impl FromRedisValue for PSlice {
    fn from_redis_value(v: &Value) -> Result<Self, RedisError> {
        let bytes = <Vec<u8> as FromRedisValue>::from_redis_value(v)?;
        match bincode::deserialize(&bytes) {
            Ok(pslice) => Ok(pslice),
            Err(err) => Err(RedisError::from((
                ErrorKind::TypeError,
                "deserialize error",
                format!("value: {v:?}, error: {err}"),
            ))),
        }
    }
}

#[derive(Debug)]
pub struct PSlices {
    pslices: Vec<PSlice>,
}

impl Deref for PSlices {
    type Target = Vec<PSlice>;

    fn deref(&self) -> &Self::Target {
        &self.pslices
    }
}

impl FromRedisValue for PSlices {
    fn from_redis_value(v: &Value) -> Result<Self, RedisError> {
        let pslices = <Vec<PSlice> as FromRedisValue>::from_redis_value(v)?;
        Ok(PSlices { pslices })
    }
}

impl PSlices {
    /// Build a list of slices from a list of flatten pslices stored in time order
    /// **Return the slices make sure every data in the slices is the newest.**
    pub fn build_slices(self) -> Vec<Slice> {
        let mut root: Option<PSlice> = None;
        for mut pslice in self.pslices {
            if let Some(prev) = root {
                let (left, right) = prev.cut(pslice.coff);
                pslice.left = left.map(Box::new);
                if let Some(right) = right {
                    let (_left, right) = right.cut(pslice.coff + pslice.len);
                    pslice.right = right.map(Box::new);
                }
            }
            root = Some(pslice);
        }
        let mut chunk = Vec::new();
        let mut pos = 0;
        if let Some(root) = root {
            root.visit(&mut |s| {
                if s.coff > pos {
                    // blank interva, fill it with default slice id 0
                    chunk.push(Slice {
                        size: s.coff - pos,
                        len: s.coff - pos,
                        ..Default::default()
                    });
                    pos = s.coff;
                }
                chunk.push(Slice {
                    id: s.id,
                    size: s.size,
                    off: s.off,
                    len: s.len,
                });
                pos += s.len;
            });
        }
        chunk
    }
}

pub struct Slices(pub Vec<Slice>);

impl TryFrom<Vec<String>> for Slices {
    type Error = bincode::Error;

    fn try_from(vals: Vec<String>) -> Result<Self, Self::Error> {
        let mut slices = Vec::with_capacity(vals.len());
        for val in vals {
            let slice = bincode::deserialize(val.as_bytes())?;
            slices.push(slice);
        }
        Ok(Slices(slices))
    }
}

#[async_trait]
pub trait MetaCompact {
    async fn compact(&mut self);

    async fn compact_chunk(&self, inode: Ino, indx: u32, once: bool, force: bool);
}

#[async_trait]
impl<E> MetaCompact for E
where
    E: Engine + AsRef<CommonMeta>,
{
    async fn compact(&mut self) {
        todo!()
    }

    async fn compact_chunk(&self, inode: Ino, indx: u32, once: bool, force: bool) {
        // TODO: fill it
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::api::Slice;

    use super::{PSlice, PSlices};

    // id, (off, end)
    impl From<(u64, Range<u32>)> for PSlice {
        fn from((id, range): (u64, Range<u32>)) -> Self {
            PSlice {
                id,
                size: range.end - range.start,
                off: 0,
                len: range.end - range.start,
                coff: range.start,
                left: None,
                right: None,
            }
        }
    }

    // id, (off, end), (valid off, valid end)
    impl From<(u64, Range<u32>, Range<u32>)> for Slice {
        fn from((id, total, valid): (u64, Range<u32>, Range<u32>)) -> Self {
            Slice {
                id,
                size: total.end - total.start,
                off: valid.start - total.start,
                len: valid.end - valid.start,
            }
        }
    }

    #[test]
    fn slice_overlap() {
        // lead overlap
        let p1: PSlice = (1, 10..20).into();
        let p2: PSlice = (2, 15..25).into();  // latest
        let pslices = PSlices {
            pslices: vec![p1, p2]
        };
        let slices = pslices.build_slices();
        let s1 = (0, 0..10, 0..10).into();
        let s2 = (1, 10..20, 10..15).into();
        let s3 = (2, 15..25, 15..25).into();
        let expect = vec![s1, s2, s3];
        assert_eq!(slices, expect);

        // tail overlap
        let p1: PSlice = (1, 15..25).into();
        let p2: PSlice = (2, 10..20).into();  // latest
        let pslices = PSlices {
            pslices: vec![p1, p2]
        };
        let slices = pslices.build_slices();
        let s1 = (0, 0..10, 0..10).into();
        let s2 = (2, 10..20, 10..20).into();
        let s3 = (1, 15..25, 20..25).into();
        let expect = vec![s1, s2, s3];
        assert_eq!(slices, expect);
    }

    #[test]
    fn slice_contains() {
        // contains
        let p1: PSlice = (1, 10..20).into();
        let p2: PSlice = (2, 15..20).into();  // latest
        let pslices = PSlices {
            pslices: vec![p1, p2]
        };
        let slices = pslices.build_slices();
        let s1 = (0, 0..10, 0..10).into();
        let s2 = (1, 10..20, 10..15).into();
        let s3 = (2, 15..20, 15..20).into();
        let expect = vec![s1, s2, s3];
        assert_eq!(slices, expect);

        // contained
        let p1: PSlice = (1, 15..20).into();
        let p2: PSlice = (2, 10..20).into();  // latest
        let pslices = PSlices {
            pslices: vec![p1, p2]
        };
        let slices = pslices.build_slices();
        let s1 = (0, 0..10, 0..10).into();
        let s2 = (2, 10..20, 10..20).into();
        let expect = vec![s1, s2];
        assert_eq!(slices, expect);
    }
}
