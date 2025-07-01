use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::time::sleep;
use tracing::Level;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, layer::SubscriberExt};

fn start_tokio_console() {
    let mut layers = vec![];
    let (console_layer, server) = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .build();
    let console_layer = console_layer.with_filter(
        filter::Targets::new()
            .with_target("tokio", Level::TRACE)
            .with_target("runtime", Level::TRACE),
    );
    layers.push(console_layer.boxed());
    std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                println!("serving console subscriber");
                server.serve().await.unwrap();
            });
    });
    tracing_subscriber::registry().with(layers).init();
}

#[tokio::test(flavor = "current_thread")]
async fn test() {
    start_tokio_console();
    let lock = Arc::new(Mutex::new(0));

	// 任务1：尝试获取同一把锁（永远阻塞）
	let lock2 = lock.clone();
	tokio::spawn(async move {
		loop {
			println!("lock2");
			let guard = lock2.lock(); // 死锁点
			println!("lock2 end");
		}
	});

    // 任务2：持有锁后调用 await
    let lock1 = lock.clone();
	println!("lock1");
	let guard = lock1.lock();  
	sleep(Duration::from_secs(1)).await;  // 本质原因是：持有同步锁的情况下让出了调度权，这里其实不应该让出调度权。因为lock不可重入，条件1：一旦其他协程也去lock，条件2：阻塞了主调度线程，导致去释放这里的lock任务无资源去执行，那也就无法释放。同时满足这两个条件会导致死锁
    println!("lock1 end")
}