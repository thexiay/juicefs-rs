use std::sync::Arc;
use std::ops::Deref;

#[derive(Debug)]
struct B {
    value: i32,
}

#[derive(Debug, Clone)]
struct A {
    inner: Arc<B>,
}

impl Deref for A {
    type Target = Arc<B>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn main() {
    let b = B { value: 42 };
    let arc_b = Arc::new(b);
    let a1 = A { inner: arc_b };

    // 调用 A 的 clone 方法
    let a2 = (*a1).clone();

    println!("a1: {:?}", a1);
    println!("a2: {:?}", a2);
}