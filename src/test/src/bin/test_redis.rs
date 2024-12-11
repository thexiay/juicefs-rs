use redis::Commands;

fn main() {
    let mut conn = redis::Client::open("redis://:mypassword@127.0.0.1:6379/").unwrap().get_connection().unwrap();
    conn.set::<_,_,()>("hhh", 0_u64).unwrap();
    conn.decr::<_,_,()>("hhh", 1).unwrap();

    let a = conn.get::<_, u64>("hhh").unwrap();
    println!("{:?}", a);
    
}