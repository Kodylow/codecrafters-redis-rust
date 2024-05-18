use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::command::{RedisCommand, RedisCommandResponse};

use super::{store::RedisStore, types::RedisInfo};

/// A trait for Redis server implementations.
#[async_trait::async_trait]
pub trait RedisServer {
    async fn handle_command(
        &mut self,
        command: RedisCommand,
    ) -> Result<RedisCommandResponse, anyhow::Error>;
}

/// A base struct for common Redis server functionality.
#[derive(Debug, Clone)]
pub struct BaseServer {
    pub info: RedisInfo,
    pub address: String,
    pub store: RedisStore,
}

impl BaseServer {
    pub async fn send_command(
        &self,
        address: &str,
        command: &str,
    ) -> Result<String, anyhow::Error> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(command.as_bytes()).await?;

        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).await?;
        let response = std::str::from_utf8(&buffer[..n])?.to_string();

        Ok(response)
    }
}
