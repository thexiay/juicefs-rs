
struct A {}

impl Drop for A {
    fn drop(&mut self) {
        let b = B {};
        panic!("111111")  
    }
}

struct B {}
impl Drop for B {
    fn drop(&mut self) {
        println!("222222")  // 验证double panic后会不会继续栈展开
    }
}

struct C {}
impl Drop for C {
    fn drop(&mut self) {
        println!("333333")  
    }
}

fn main() {
    let c = C {};  // 验证double panic后会不会继续栈展开
    panic_fn();
}

fn panic_fn() {
    let a = A {};
    std::mem::replace
    panic!("0000000");
}