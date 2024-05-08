use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use tracing::info;

// Enum for Redis commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisCommand {
    Ping,
    Pong,
    Echo(String),
    Get(String),
    Set(String, String, Option<u64>),
}

impl Display for RedisCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisCommand::Ping => write!(f, "PING"),
            RedisCommand::Pong => write!(f, "PONG"),
            RedisCommand::Echo(s) => write!(f, "ECHO {}", s),
            RedisCommand::Get(s) => write!(f, "GET {}", s),
            RedisCommand::Set(key, value, expiry) => {
                if let Some(expiry) = expiry {
                    write!(f, "SET {} {} PX {}", key, value, expiry)
                } else {
                    write!(f, "SET {} {}", key, value)
                }
            }
        }
    }
}

impl RedisCommand {
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
    pub fn parse(buffer: &[u8]) -> Result<Self, anyhow::Error> {
        let buffer_str = String::from_utf8(buffer.to_vec())?;
        let lines = buffer_str.split("\r\n");
        let mut lines = lines.filter(|line| !line.is_empty());

        let array_length_str = RedisCommand::extract_line(&mut lines, '*')?;
        let array_length = array_length_str
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("Invalid array length"))?;

        if array_length < 1 {
            return Err(anyhow::anyhow!(
                "Command array must have at least one element"
            ));
        }

        let _command_length = RedisCommand::extract_line(&mut lines, '$')?;
        let command = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Command not found"))?
            .to_lowercase();

        info!("Parsed command: {:?}", command);

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => {
                if array_length < 2 {
                    return Err(anyhow::anyhow!("ECHO command requires an argument"));
                }
                let _arg_length = RedisCommand::extract_line(&mut lines, '$')?;
                let argument = lines
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Argument not found"))?;
                Ok(RedisCommand::Echo(argument.to_string()))
            }
            "set" => RedisCommand::parse_set_command(&mut lines, array_length),
            "get" => {
                if array_length < 2 {
                    return Err(anyhow::anyhow!("GET command requires one argument"));
                }
                let _key_length = RedisCommand::extract_line(&mut lines, '$')?;
                let key = lines
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Key not found"))?;
                Ok(RedisCommand::Get(key.to_string()))
            }
            _ => Err(anyhow::anyhow!("Unknown Redis command")),
        }
    }

    pub fn parse_set_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<Self, anyhow::Error> {
        if array_length < 3 {
            return Err(anyhow::anyhow!(
                "SET command requires at least two arguments"
            ));
        }
        let _key_length = RedisCommand::extract_line(lines, '$')?;
        let key = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Key not found"))?;
        let _value_length = RedisCommand::extract_line(lines, '$')?;
        let value = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Value not found"))?;

        let expiry = if array_length == 5 {
            let _expiry_prefix_length = RedisCommand::extract_line(lines, '$')?;
            let expiry_prefix = lines
                .next()
                .ok_or_else(|| anyhow::anyhow!("Expiry prefix not found"))?;
            info!("Expiry prefix: {}", expiry_prefix);
            let _expiry_value_length = RedisCommand::extract_line(lines, '$')?;
            let expiry_str = lines
                .next()
                .ok_or_else(|| anyhow::anyhow!("Expiry value not found"))?;
            info!("Expiry value: {}", expiry_str);
            Some(
                expiry_str
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("Invalid expiry format"))?,
            )
        } else {
            None
        };

        Ok(RedisCommand::Set(
            key.to_string(),
            value.to_string(),
            expiry,
        ))
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
