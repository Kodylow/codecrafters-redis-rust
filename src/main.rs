mod command;
mod redis;
mod utils;

use clap::Parser;
use tracing::Level;
use tracing_subscriber::fmt::Subscriber;

#[derive(Parser)]
#[clap(version = "1.0", author = "Kody Low <kodylow7@gmail.com>")]
pub struct Cli {
    /// Hostname for the Redis server
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,
    /// Port number for the Redis server
    #[clap(long, default_value = "6379")]
    pub port: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = Subscriber::builder().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber)?;
    dotenv::dotenv().ok();
    let cli = Cli::parse();
    let redis = redis::Redis::new(cli.host, cli.port);
    redis.serve().await
}
