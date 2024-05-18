use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};
use tracing::info;

use crate::command::RedisCommandParser;
use crate::redis::Redis;

pub async fn start_server(redis: Arc<Mutex<Redis>>) -> Result<()> {
    let listener = TcpListener::bind(&redis.lock().await.address).await?;
    info!("Redis server listening on {}", redis.lock().await.address);

    let redis_clone = redis.lock().await.clone();
    tokio::spawn(async move {
        redis_clone.expiry_worker().await;
    });

    // Initiate handshake by sending PING to master
    let redis_clone = redis.lock().await.clone();
    if redis_clone.is_slave() {
        tokio::spawn(async move {
            if let Err(e) = redis_clone.send_ping_to_master().await {
                eprintln!("Error sending PING to master: {:?}", e);
            }
        });
    }

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
