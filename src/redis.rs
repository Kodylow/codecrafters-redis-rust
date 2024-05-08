use serde::{Deserialize, Serialize};
use tracing::info;

// Enum for Redis commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisCommand {
    Ping,
    Pong,
    // Add other commands here
}

// Implement parsing from string to RedisCommand
impl RedisCommand {
    pub fn from_buffer(buffer: &[u8]) -> Result<Self, anyhow::Error> {
        // Assuming the buffer is a valid UTF-8 string and the command starts after the first line break
        let buffer_str = String::from_utf8(buffer.to_vec())?;
        let lines: Vec<&str> = buffer_str.split('\n').collect();
        if lines.len() < 3 {
            return Err(anyhow::anyhow!("Buffer format is incorrect"));
        }
        let command = lines[2].trim(); // Assuming the command is on the third line
        info!("Parsed command from buffer: {}", command);
        match command.to_lowercase().as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            _ => Err(anyhow::anyhow!("Unknown Redis command")),
        }
    }
}

// Struct for Redis command responses
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
pub struct RedisCommandHandler;

impl RedisCommandHandler {
    pub fn handle_command(command: RedisCommand) -> RedisCommandResponse {
        match command {
            RedisCommand::Ping => RedisCommandResponse::new("PONG".to_string()),
            RedisCommand::Pong => RedisCommandResponse::new("PING".to_string()),
        }
    }
}
