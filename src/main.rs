use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use redis::types::RedisRole;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::{info, Level};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, EnvFilter};

mod command;
mod redis;
mod utils;

use crate::command::RedisCommandParser;
use crate::redis::Redis;

#[derive(Parser)]
#[clap(version = "1.0", author = "Kody Low <kodylow7@gmail.com>")]
pub struct Cli {
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    #[clap(long, default_value = "6379")]
    pub port: String,

    #[clap(long, default_value = "master")]
    pub role: RedisRole,

    #[clap(long)]
    pub replicaof: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                EnvFilter::builder()
                    .with_default_directive(Level::INFO.into())
                    .from_env()
                    .context("Invalid log level")?,
            ),
        )
        .try_init()?;

    dotenv::dotenv().ok();

    let cli = Cli::parse();
    let role = match cli.replicaof {
        Some(_) => RedisRole::Slave,
        None => cli.role,
    };
    let (master_host, master_port) = if let Some(replica) = cli.replicaof {
        let parts: Vec<&str> = replica.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!(
                "Expected format for replicaof: <host>:<port>"
            ));
        }
        (parts[0].to_string(), parts[1].to_string())
    } else {
        (cli.host.clone(), cli.port.clone())
    };
    let redis = Arc::new(Redis::new(
        &cli.host,
        &cli.port,
        role,
        &master_host,
        &master_port,
    ));
    let listener = TcpListener::bind(&redis.address).await?;
    info!("Redis server listening on {}", redis.address);

    let redis_clone = redis.clone();
    tokio::spawn(async move {
        redis_clone.expiry_worker().await;
    });

    loop {
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buffer = vec![0; 1024];
                while let Ok(n) = stream.read(&mut buffer).await {
                    if n == 0 {
                        break;
                    }
                    let buffer_str = std::str::from_utf8(&buffer).context("Invalid UTF-8")?;
                    let command =
                        RedisCommandParser::parse(buffer_str).context("Invalid command")?;

                    if command.is_write_operation() {
                        redis.replicate_to_slaves(buffer_str).await?;
                    }

                    let response = redis
                        .handle_command(command)
                        .await
                        .context("Error handling command")?;
                    stream
                        .write_all(response.message.as_bytes())
                        .await
                        .context("Error writing response")?;
                    buffer.fill(0);
                }
            }
        }
    }
}
