use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

// Enum for Redis commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisCommand {
    Ping,
    Pong,
    Echo(String),
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisCommand::Ping => write!(f, "PING"),
            RedisCommand::Pong => write!(f, "PONG"),
            RedisCommand::Echo(s) => write!(f, "ECHO {}", s),
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
pub struct RedisCommandHandler;

impl RedisCommandHandler {
    pub fn handle_command(command: RedisCommand) -> RedisCommandResponse {
        match command {
            RedisCommand::Ping => RedisCommandResponse::new("PONG".to_string()),
            RedisCommand::Pong => RedisCommandResponse::new("PING".to_string()),
            RedisCommand::Echo(s) => RedisCommandResponse::new(s),
        }
    }
}
