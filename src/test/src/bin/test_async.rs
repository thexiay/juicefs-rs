// 此特征用例列举了在某些场景下在async_trait中使用async fn会报错的情况
#![feature(async_closure)]
#![feature(async_fn_traits)]

use std::{future::Future, ops::AsyncFn, pin::Pin, time::Duration};
use tokio::time::sleep;

#[derive(Debug)]
struct Redis {}

impl Redis {
    // 这里想模拟一个事务操作,封装闭包然后包装到通用的事务机制中是常见的手法
    async fn txn<C: ToString, F: AsyncFn(&mut C) -> Option<String>>(c: &mut C, func: F) -> String {
        // 做一系列事务操作
        loop {
            let response: Option<String> = func(c).await;
            match response {
                None => {
                    continue;
                }
                Some(response) => {
                    return response;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let redis = Redis {};
    // 当在外部使用时没有任何问题，在async_trait中使用时会报错
    println!("111");
    let value = 100;
    let diff = 10;
    let name = "test";

    // 在非async trait
    Redis::txn(&mut "asdasd", async |c| {
        let a = format!("{}{}", c, "123");
        sleep(Duration::from_secs(1)).await;
        Some(a)
    })
    .await;
    println!("222");

    // 模拟#[async_trait]的封装,为什么在async_trait中使用异步闭包会有问题
    // 为什么如下这一段加上Send就会报错，不加Send就不会报错
    /*
    let b: Pin<Box<dyn Future<Output = ()> + Send + '_>> = Box::pin(async move {
        let () = {
            let value = 100;
            let diff = 10;
            let name = "test";
            Redis::txn(&mut "asdasd", async |c| {
                let a = format!("{}{}", c, "123");
                sleep(Duration::from_secs(1)).await;
                Some(a)
            })
            .await;
        };
    });
     */
}

fn is_send<T: Send>(a: T) {}
