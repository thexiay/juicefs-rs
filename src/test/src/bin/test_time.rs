use tokio::time::Instant;

fn hello() -> u32 {
    2
}

fn world() -> Enum {
    Enum::A { b: 1 }
}

enum Enum {
    A {
        b: u32,
    },
    B,
}

fn main() {
    let a = hello();
    if let Enum::A { b: 3 } = world() {
        println!("{:?}", Instant::now())
    }
}