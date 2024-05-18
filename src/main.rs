use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use redis::types::RedisRole;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
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
        let parts: Vec<&str> = replica.split(' ').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!(
                "Expected format for replicaof: \"<host> <port>\""
            ));
        }
        (parts[0].to_string(), parts[1].to_string())
    } else {
        (cli.host.clone(), cli.port.clone())
    };
    let redis = Arc::new(Mutex::new(Redis::new(
        &cli.host,
        &cli.port,
        role,
        &master_host,
        &master_port,
    )));
    let listener = TcpListener::bind(&redis.lock().await.address).await?;
    info!("Redis server listening on {}", redis.lock().await.address);

    let redis_clone = redis.lock().await.clone();
    tokio::spawn(async move {
        redis_clone.expiry_worker().await;
    });

    loop {
        let (mut stream, _) = listener.accept().await?;
        let redis_clone = redis.clone();

        tokio::spawn(async move {
            let mut buffer = vec![0; 1024];
            while let Ok(n) = stream.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                let buffer_str = match std::str::from_utf8(&buffer) {
                    Ok(s) => s,
                    Err(_) => {
                        eprintln!("Invalid UTF-8 sequence");
                        continue;
                    }
                };
                let command = match RedisCommandParser::parse(buffer_str) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        eprintln!("Invalid command: {:?}", e);
                        continue;
                    }
                };

                if command.is_write_operation() {
                    if let Err(e) = redis_clone
                        .lock()
                        .await
                        .replicate_to_slaves(buffer_str)
                        .await
                    {
                        eprintln!("Error replicating to slaves: {:?}", e);
                        continue;
                    }
                }

                let response = match redis_clone.lock().await.handle_command(command).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        eprintln!("Error handling command: {:?}", e);
                        continue;
                    }
                };
                if let Err(e) = stream.write_all(response.message.as_bytes()).await {
                    eprintln!("Error writing response: {:?}", e);
                    continue;
                }
                buffer.fill(0);
            }
        });
    }
}
