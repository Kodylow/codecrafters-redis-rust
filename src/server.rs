use crate::redis::{base::RedisServer, slave::Slave};
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};
use tracing::{error, info};

use crate::{parser::RedisCommandParser, redis::master::Master};

pub async fn start_master_server(redis: Arc<Mutex<Master>>) -> Result<()> {
    let listener = TcpListener::bind(&redis.lock().await.base.address).await?;
    let address = listener.local_addr()?;
    info!("Redis master server listening on {}", address);

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
                        error!("Invalid UTF-8 sequence");
                        continue;
                    }
                }
                .to_string();
                buffer.fill(0);

                let command = match RedisCommandParser::parse(&buffer_str) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        error!("Invalid command: {:?}", e);
                        continue;
                    }
                };

                if command.is_write_operation() {
                    if let Err(e) = redis_clone
                        .lock()
                        .await
                        .replicate_to_slaves(&buffer_str)
                        .await
                    {
                        error!("Error replicating to slaves: {:?}", e);
                        continue;
                    }
                }

                let response = match redis_clone.lock().await.handle_command(command).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!("Error handling command: {:?}", e);
                        continue;
                    }
                };
                info!("Sending response: {:?}", response);
                if let Err(e) = stream.write_all(response.message.as_bytes()).await {
                    error!("Error writing response: {:?}", e);
                    continue;
                }
            }
        });
    }
}

pub async fn start_slave_server(redis: Arc<Mutex<Slave>>) -> Result<()> {
    let listener = TcpListener::bind(&redis.lock().await.base.address).await?;
    let address = listener.local_addr()?;
    info!("Redis slave server listening on {}", address);

    // Handshake with master
    let redis_clone = redis.lock().await.clone();
    tokio::spawn(async move {
        if let Err(e) = redis_clone.handshake().await {
            error!("Error handshaking with master: {:?}", e);
            return;
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
                let buffer_str = match std::str::from_utf8(&buffer[..n]) {
                    Ok(s) => s,
                    Err(_) => {
                        eprintln!("Invalid UTF-8 sequence");
                        continue;
                    }
                };
                info!("Received buffer: {:?}", buffer_str);

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
                info!("Sending response: {:?}", response);
                if let Err(e) = stream.write_all(response.message.as_bytes()).await {
                    eprintln!("Error writing response: {:?}", e);
                    continue;
                }
                buffer.fill(0);
            }
        });
    }
}
