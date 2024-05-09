use std::fmt::{Display, Formatter};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Enum for supportedRedis commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisCommand {
    Ping,
    Pong,
    Echo(String),
    Get(String),
    Set(String, String, Option<u64>),
    Info(String),
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
            RedisCommand::Info(section) => write!(f, "INFO {}", section),
        }
    }
}

impl RedisCommand {
    /// Helper function to extract the next line with a specific prefix
    fn extract_line<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        prefix: char,
    ) -> Result<&'a str, anyhow::Error> {
        lines
            .next()
            .context("Invalid protocol format")?
            .strip_prefix(prefix)
            .ok_or_else(|| anyhow::anyhow!("Expected line to start with '{}'", prefix))
    }

    /// Parses a Redis command into the RedisCommand enum.
    pub fn parse(buffer_str: &str) -> Result<Self, anyhow::Error> {
        let mut lines = buffer_str.split("\r\n").filter(|line| !line.is_empty());

        let array_length = Self::parse_array_length(&mut lines)?;

        let command = lines.next().context("Command not found")?.to_lowercase();

        info!("Parsed command: {}", command);

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => Self::handle_echo_command(&mut lines, array_length),
            "set" => Self::parse_set_command(&mut lines, array_length),
            "get" => Self::parse_get_command(&mut lines, array_length),
            "info" => Self::parse_info_command(&mut lines, array_length),
            _ => Err(anyhow::anyhow!("Unknown Redis command")),
        }
    }

    fn parse_array_length<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
    ) -> Result<usize, anyhow::Error> {
        let array_length_str = Self::extract_line(lines, '*')?;
        let array_length = array_length_str
            .parse::<usize>()
            .context("Invalid array length")?;

        if array_length < 1 {
            anyhow::bail!("Command array must have at least one element");
        }

        Ok(array_length)
    }

    fn handle_echo_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<Self, anyhow::Error> {
        if array_length < 2 {
            anyhow::bail!("ECHO command requires an argument");
        }
        let argument = Self::parse_argument(lines, "Argument")?;
        Ok(RedisCommand::Echo(argument))
    }

    fn parse_get_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<Self, anyhow::Error> {
        if array_length < 2 {
            anyhow::bail!("GET command requires one argument");
        }
        let key = Self::parse_argument(lines, "Key")?;
        Ok(RedisCommand::Get(key))
    }

    fn parse_set_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<Self, anyhow::Error> {
        if array_length < 3 {
            anyhow::bail!("SET command requires at least two arguments");
        }

        let key = Self::parse_argument(lines, "Key")?;
        let value = Self::parse_argument(lines, "Value")?;

        let expiry = if array_length == 5 {
            Some(Self::parse_expiry(lines)?)
        } else {
            None
        };
        Ok(RedisCommand::Set(key, value, expiry))
    }

    fn parse_info_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<Self, anyhow::Error> {
        if array_length < 2 {
            anyhow::bail!("INFO command requires a section argument");
        }
        let section = Self::parse_argument(lines, "Section")?;
        Ok(RedisCommand::Info(section))
    }

    fn parse_argument<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        name: &str,
    ) -> Result<String, anyhow::Error> {
        Self::extract_line(lines, '$')?;
        lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("{} not found", name))
            .map(String::from)
    }

    fn parse_expiry<'a>(lines: &mut impl Iterator<Item = &'a str>) -> Result<u64, anyhow::Error> {
        Self::extract_line(lines, '$')?;
        let expiry_str = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expiry value not found"))?;
        expiry_str.parse::<u64>().context("Invalid expiry format")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisCommandResponse {
    pub message: String,
}

impl RedisCommandResponse {
    pub fn new(message: String) -> Self {
        let formatted_message = if message.is_empty() {
            "$-1\r\n".to_string()
        } else {
            format!("${}\r\n{}\r\n", message.len(), message)
        };
        RedisCommandResponse {
            message: formatted_message,
        }
    }

    pub fn null() -> Self {
        RedisCommandResponse {
            message: "$-1\r\n".to_string(),
        }
    }

    pub fn _error(message: String) -> Self {
        RedisCommandResponse {
            message: format!("-{}\r\n", message),
        }
    }
}
