use std::{collections::HashMap, sync::Mutex};

struct Request {

}

pub struct Controller {
    lock: Mutex<()>,
    rs: HashMap<String, Request>,
}