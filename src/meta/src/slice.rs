use std::{collections::LinkedList, ops::Deref};

use async_trait::async_trait;
use redis::{ErrorKind, FromRedisValue, RedisError, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};

use crate::{
    api::{Ino, Slice},
    base::{CommonMeta, Engine},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct PSlice {
    id: u64,
    size: u32,
    off: u32,
    len: u32,
    pos: u32,
    #[serde(skip)]
    left: Option<Box<PSlice>>,
    #[serde(skip)]
    right: Option<Box<PSlice>>,
}

impl PSlice {
    pub fn new(slice: &Slice, pos: u32) -> Self {
        PSlice {
            id: slice.id,
            size: slice.size,
            off: slice.off,
            len: slice.len,
            pos,
            left: None,
            right: None,
        }
    }

    fn cut(mut self, pos: u32) -> (Option<PSlice>, Option<PSlice>) {
        if pos <= self.pos {
            let left = self.left.unwrap_or(Box::new(PSlice {
                id: 0,
                size: 0,
                off: 0,
                len: self.pos - pos,
                pos: pos,
                left: None,
                right: None,
            }));
            let (left, right) = left.cut(pos);
            self.left = right.map(Box::new);
            (left, Some(self))
        } else if pos < self.pos + self.len {
            let l = pos - self.pos;
            let mut right = PSlice {
                id: self.id,
                size: self.size,
                off: self.off + l,
                len: self.len - l,
                pos: pos,
                left: None,
                right: None,
            };
            right.right = self.right;
            self.len = l;
            self.right = None;
            (Some(self), Some(right))
        } else {
            let right = self.right.unwrap_or(Box::new(PSlice {
                id: 0,
                size: 0,
                off: 0,
                len: pos - self.pos - self.len,
                pos: self.pos + self.len,
                left: None,
                right: None,
            }));
            let (left, right) = right.cut(pos);
            self.right = left.map(Box::new);
            (Some(self), right)
        }
    }

    fn visit(&self, f: &mut impl FnMut(&PSlice)) {
        self.left.as_ref().map(|s| s.visit(f));
        let right = self.right.as_ref();
        f(self); // self could be freed
        right.map(|s| s.visit(f));
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
    /// Build a list of slices from a list of flatten pslices
    /// pslices should distribute the data to make data as equal as possible in each slice.
    pub fn build_slices(self) -> Vec<Slice> {
        let mut root: Option<PSlice> = None;
        for mut pslice in self.pslices {
            if let Some(prev) = root {
                let (left, right) = prev.cut(pslice.pos);
                pslice.left = left.map(Box::new);
                if let Some(right) = right {
                    let (_, right) = right.cut(pslice.pos + pslice.len);
                    pslice.right = right.map(Box::new);
                }
            }
            root = Some(pslice);
        }
        let mut chunk = Vec::new();
        let mut pos = 0;
        if let Some(root) = root {
            root.visit(&mut |s| {
                if s.pos > pos {
                    chunk.push(Slice {
                        size: s.pos - pos,
                        len: s.pos - pos,
                        ..Default::default()
                    });
                    pos = s.pos;
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
