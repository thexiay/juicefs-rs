use std::collections::HashMap;


pub enum ArenaNode<T> {
    Occupied {
        data: T,
        next: Option<usize>,
        prev: Option<usize>,
    },
    Vacant,
}

pub struct ArenaList<T> {
    list: Vec<ArenaNode<T>>,
    first_free: usize,
    ids: HashMap<usize, Option<usize>>,
    id: usize,
}

impl<T> ArenaList<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let list = Vec::with_capacity(capacity);
        Self {
            list,
            first_free: 0,
            ids: HashMap::new(),
            id: 0,
        }
    }

    pub fn new_list(&mut self) -> usize {
        self.id += 1;
        self.ids.insert(self.id, None);
        self.id
    }

    pub fn push_front(&mut self, id: usize, ele: T) {
        // todo
    }

    pub fn push_back(&mut self, id: usize, ele: T) {
        // todo
    }

    fn get(&self, pos: usize) -> Option<&ArenaNode<T>> {
        self.list.get(pos)
    }

    pub fn iter(&self, id: usize) -> impl Iterator<Item = &T> {
        ArenaListIter {
            list: self,
            cur: Some(id),
        }
    }
    
    // Delete node which match function [`f`]
    // Return the index of new head node.
    pub fn retain(&mut self, id: usize, mut f: impl FnMut(&T) -> bool) -> Option<usize> {
        todo!()
    }
}

struct ArenaListIter<'a, T> {
    list: &'a ArenaList<T>,
    cur: Option<usize>,
}

impl<'a, T> Iterator for ArenaListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cur {
            Some(pos) => {
                let node = self.list.get(pos);
                match node {
                    Some(ArenaNode::Occupied { 
                        data, 
                        next, 
                        .. 
                    }) => {
                        self.cur = *next;
                        Some(data)
                    },
                    _ => None,
                }
            }
            None => None,
        }
    }
}