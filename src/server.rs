use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::redis::{RedisCommand, RedisCommandHandler};

pub async fn start_redis_server(address: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(address).await?;
    info!("Redis Server listening on {}", address);

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream).await {
                error!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_client(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buffer = vec![0; 1024];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        let command = RedisCommand::from_buffer(&buffer)?;
        info!("Received command: {}", command);
        let response = RedisCommandHandler::handle_command(command);
        stream.write_all(response.message.as_bytes()).await?;
        info!("Sent response: {}", response.message);
        buffer.fill(0);
    }
    Ok(())
}
