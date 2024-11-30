use async_trait::async_trait;

use crate::{api::Ino, base::{CommonMeta, Engine}};

#[async_trait]
pub trait MetaCompact {
    async fn compact(&mut self);

    async fn compact_chunk(&self, inode: Ino, indx: u32, once: bool, force: bool);
}


#[async_trait]
impl<E> MetaCompact for E
where
    E: Engine + AsRef<CommonMeta> 
{

    async fn compact(&mut self) {
        todo!()
    }

    async fn compact_chunk(&self, inode: Ino, indx: u32, once: bool, force: bool) {
        // TODO: fill it
    }
}
