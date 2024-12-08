use redis::Commands;

fn main() {
    let mut conn = redis::Client::open("redis://127.0.0.1:6379/").unwrap().get_connection().unwrap();
    let a: Vec<String> = redis::pipe().del("key1").get("key2").get("key3").query(&mut conn).unwrap();
    println!("{:?}", a);
    
}