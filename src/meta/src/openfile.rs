use std::collections::HashMap;

use crate::api::{Attr, Slice};

pub struct OpenFile {
    attr: Attr,
    refs: i32,
    last_check: i64,
    first: Vec<Slice>,
    chunks: HashMap<u32, Vec<Slice>>,
}
