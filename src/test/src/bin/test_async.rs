use std::future::Future;

async fn for_each_city<F, Fut>(mut f: F)
where  // 这里特征边界必须分离成两个特征
    F: for<'c> FnMut(&'c str) -> Fut,
    Fut: Future<Output = ()>,
{
    for x in ["New York", "London", "Tokyo"] {
        f(x).await;
    }
}

#[tokio::main]
async fn main() {
    
}