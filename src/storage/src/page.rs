use bytes::Bytes;

pub struct Page {
    refs: i32,
    offheap: bool,
    dep: Option<Box<Page>>,
    data: Bytes,
    stack: Bytes,
}