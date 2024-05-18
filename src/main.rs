use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::Mutex;
use tracing::Level;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, EnvFilter};

mod cli;
mod command;
mod redis;
mod server;
mod utils;

use crate::cli::Cli;
use crate::redis::Redis;
use crate::server::start_server;

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

    let redis = Arc::new(Mutex::new(Redis::new(
        &cli.host,
        &cli.port,
        role,
        &master_host,
        &master_port,
    )));

    start_server(redis).await
}
