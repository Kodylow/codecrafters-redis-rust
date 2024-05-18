use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{net::TcpListener, sync::Mutex};
use tracing::info;

use crate::redis::{master::Master, slave::Slave};
use crate::{
    command::{RedisCommand, RedisCommandParser},
    redis::base::RedisServer,
};

pub async fn start_master_server(redis: Arc<Mutex<Master>>) -> Result<()> {
    let redis_info = redis.lock().await.base.info.clone();
    let address = format!("{}:{}", redis_info.master_host, redis_info.master_port);
    let listener = TcpListener::bind(&address).await?;
    info!("Redis server listening on {}", address);

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

pub async fn start_slave_server(redis: Arc<Mutex<Slave>>) -> Result<()> {
    let redis_info = redis.lock().await.base.info.clone();
    let address = format!("{}:{}", redis_info.master_host, redis_info.master_port);
    let listener = TcpListener::bind(&address).await?;
    info!("Redis server listening on {}", address);

    // Initiate handshake by sending PING to master
    let redis_clone = redis.lock().await.clone();
    tokio::spawn(async move {
        if let Err(e) = redis_clone.send_command_to_master(RedisCommand::Ping).await {
            eprintln!("Error sending PING to master: {:?}", e);
        }
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
