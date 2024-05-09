use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::{info, subscriber, Level};
use tracing_subscriber::FmtSubscriber;

mod command;
mod redis;
mod utils;

use crate::command::RedisCommandParser;
use crate::redis::{Redis, RedisRole};

#[derive(Parser)]
#[clap(version = "1.0", author = "Kody Low <kodylow7@gmail.com>")]
pub struct Cli {
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    #[clap(long, default_value = "6379")]
    pub port: String,

    #[clap(long, default_value = "master")]
    pub role: RedisRole,

    #[clap(long, default_value = "127.0.0.1")]
    pub master_host: String,

    #[clap(long, default_value = "6379")]
    pub master_port: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    subscriber::set_global_default(subscriber)?;
    dotenv::dotenv().ok();

    let cli = Cli::parse();
    let redis = Arc::new(Redis::new(
        &cli.host,
        &cli.port,
        cli.role,
        &cli.master_host,
        &cli.master_port,
    ));
    let listener = TcpListener::bind(&redis.address).await?;

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = vec![0; 1024];
            while let Ok(n) = stream.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                let buffer_str = std::str::from_utf8(&buffer).unwrap();
                let command = RedisCommandParser::parse(buffer_str).unwrap();
                let response = redis.handle_command(command).await.unwrap();
                stream.write_all(response.message.as_bytes()).await.unwrap();
                buffer.fill(0);
            }
        }
    }
}
