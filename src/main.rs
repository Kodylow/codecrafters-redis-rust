use std::sync::Arc;

pub mod cli;
pub mod command;
pub mod parser;
pub mod redis;
pub mod server;
pub mod utils;

use crate::cli::Cli;
use crate::redis::{master::Master, slave::Slave};
use anyhow::{Context, Result};
use clap::Parser;
use redis::types::RedisRole;
use server::{start_master_server, start_slave_server};
use tokio::sync::Mutex;
use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

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
    let role = cli.determine_role();
    let (master_host, master_port) = cli.get_master_info()?;

    match role {
        RedisRole::Master => {
            let redis = Arc::new(Mutex::new(Master::new(&cli.host, &cli.port)));
            start_master_server(redis).await
        }
        RedisRole::Slave => {
            let redis = Arc::new(Mutex::new(Slave::new(
                &cli.host,
                &cli.port,
                &master_host,
                &master_port,
            )));
            start_slave_server(redis).await
        }
    }
}
