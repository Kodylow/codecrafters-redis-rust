use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tracing::info;

// Enum for Redis commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisCommand {
    Ping,
    Pong,
    Echo(String),
    Get(String),
    Set(String, String),
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisCommand::Ping => write!(f, "PING"),
            RedisCommand::Pong => write!(f, "PONG"),
            RedisCommand::Echo(s) => write!(f, "ECHO {}", s),
            RedisCommand::Get(s) => write!(f, "GET {}", s),
            RedisCommand::Set(k, v) => write!(f, "SET {} {}", k, v),
        }
    }
}

impl RedisCommand {
    pub fn parse(buffer: &[u8]) -> Result<Self, anyhow::Error> {
        let buffer_str = String::from_utf8(buffer.to_vec())?;
        let mut lines = buffer_str.split("\r\n").filter(|line| !line.is_empty());

        // Helper function to extract the next line with a specific prefix
        fn extract_line<'a>(
            lines: &mut impl Iterator<Item = &'a str>,
            prefix: char,
        ) -> Result<&'a str, anyhow::Error> {
            lines
                .next()
                .ok_or_else(|| anyhow::anyhow!("Invalid protocol format"))
                .and_then(|line| {
                    if line.starts_with(prefix) {
                        Ok(line.trim_start_matches(prefix))
                    } else {
                        Err(anyhow::anyhow!(
                            "Expected line to start with '{}', found: {}",
                            prefix,
                            line
                        ))
                    }
                })
        }

        let array_length_str = extract_line(&mut lines, '*')?;
        let array_length = array_length_str
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("Invalid array length"))?;

        if array_length < 1 {
            return Err(anyhow::anyhow!(
                "Command array must have at least one element"
            ));
        }

        let _command_length = extract_line(&mut lines, '$')?;
        let command = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Command not found"))?
            .to_lowercase();

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => {
                if array_length < 2 {
                    return Err(anyhow::anyhow!("ECHO command requires an argument"));
                }
                let _arg_length = extract_line(&mut lines, '$')?;
                let argument = lines
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Argument not found"))?;
                Ok(RedisCommand::Echo(argument.to_string()))
            }
            "set" => {
                if array_length < 3 {
                    return Err(anyhow::anyhow!("SET command requires two arguments"));
                }
                let _key_length = extract_line(&mut lines, '$')?;
                let key = lines
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Key not found"))?;
                let _value_length = extract_line(&mut lines, '$')?;
                let value = lines
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Value not found"))?;
                Ok(RedisCommand::Set(key.to_string(), value.to_string()))
            }
            "get" => {
                if array_length < 2 {
                    return Err(anyhow::anyhow!("GET command requires one argument"));
                }
                let _key_length = extract_line(&mut lines, '$')?;
                let key = lines
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Key not found"))?;
                Ok(RedisCommand::Get(key.to_string()))
            }
            _ => Err(anyhow::anyhow!("Unknown Redis command")),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisCommandResponse {
    pub message: String,
}

impl RedisCommandResponse {
    pub fn new(message: String) -> Self {
        RedisCommandResponse {
            message: format!("+{}\r\n", message),
        }
    }
}

// Command handler to process commands and generate responses
#[derive(Debug, Clone)]
pub struct Redis {
    store: Arc<Mutex<BTreeMap<String, String>>>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            store: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn serve(self, address: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(address).await?;
        info!("Redis Server listening on {}", address);
        loop {
            let (stream, _) = listener.accept().await?;
            tokio::spawn(self.clone().process_client_req(stream));
        }
    }

    async fn process_client_req(self, mut stream: TcpStream) -> anyhow::Result<()> {
        let mut buffer = vec![0; 1024];
        loop {
            let n = stream.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            let command = RedisCommand::parse(&buffer)?;
            info!("Received command: {}", command);
            let response = self.clone().handle_command(command).await;
            stream.write_all(response.message.as_bytes()).await?;
            info!("Sent response: {}", response.message);
            buffer.fill(0);
        }
        Ok(())
    }

    pub async fn handle_command(self, command: RedisCommand) -> RedisCommandResponse {
        match command {
            RedisCommand::Ping => RedisCommandResponse::new("PONG".to_string()),
            RedisCommand::Pong => RedisCommandResponse::new("PING".to_string()),
            RedisCommand::Echo(s) => RedisCommandResponse::new(s),
            RedisCommand::Set(key, value) => {
                let mut store = self.store.lock().await;
                store.insert(key, value);
                RedisCommandResponse::new("OK".to_string())
            }
            RedisCommand::Get(key) => {
                let store = self.store.lock().await;
                match store.get(&key) {
                    Some(value) => RedisCommandResponse::new(value.clone()),
                    None => RedisCommandResponse::new("(nil)".to_string()),
                }
            }
        }
    }
}
