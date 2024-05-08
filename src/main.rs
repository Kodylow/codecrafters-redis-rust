mod command;
mod redis;
mod utils;

use tracing::Level;
use tracing_subscriber::fmt::Subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = Subscriber::builder().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    const REDIS_SERVER_ADDRESS: &str = "127.0.0.1:6379";
    let redis = redis::Redis::new();
    redis.serve(REDIS_SERVER_ADDRESS).await
}
