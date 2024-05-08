use std::fmt::{Display, Formatter};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::utils::millis_to_timestamp_from_now;

/// Enum for supportedRedis commands
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
    /// Helper function to extract the next line with a specific prefix
    fn extract_line<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        prefix: char,
    ) -> Result<&'a str, anyhow::Error> {
        let line = lines.next().context("Invalid protocol format")?;
        line.starts_with(prefix)
            .then(|| line.trim_start_matches(prefix))
            .ok_or_else(|| {
                anyhow::anyhow!("Expected line to start with '{}', found: {}", prefix, line)
            })
    }

    /// Parses a Redis command into my RedisCommand enum.
    pub fn parse(buffer: &[u8]) -> Result<Self, anyhow::Error> {
        let buffer_str = std::str::from_utf8(buffer)?;
        let mut lines = buffer_str.split("\r\n").filter(|line| !line.is_empty());

        // Parse the array length for the command and check if it's valid
        let array_length_str = Self::extract_line(&mut lines, '*')?;
        let array_length = array_length_str
            .parse::<usize>()
            .context("Invalid array length")?;

        if array_length < 1 {
            anyhow::bail!("Command array must have at least one element");
        }

        // Parse the command and extract the arguments
        let _command_length = Self::extract_line(&mut lines, '$')?;
        let command = lines.next().context("Command not found")?.to_lowercase();

        info!("Parsed command: {}", command);

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => Self::handle_echo_command(&mut lines, array_length),
            "set" => Self::parse_set_command(&mut lines, array_length),
            "get" => {
                if array_length < 2 {
                    anyhow::bail!("GET command requires one argument");
                }
                let _key_length = Self::extract_line(&mut lines, '$')?;
                let key = lines.next().context("Key not found")?;
                Ok(RedisCommand::Get(key.to_string()))
            }
            _ => Err(anyhow::anyhow!("Unknown Redis command")),
        }
    }

    /// Handles an ECHO command.
    pub fn handle_echo_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<Self, anyhow::Error> {
        if array_length < 2 {
            anyhow::bail!("ECHO command requires an argument");
        }
        let _arg_length = Self::extract_line(lines, '$')?;
        let argument = lines.next().context("Argument not found")?;
        Ok(RedisCommand::Echo(argument.to_string()))
    }

    /// Parses a SET command.
    /// Parses the key, value and optional expiry and returns a RedisCommand.
    pub fn parse_set_command<'a>(
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

    /// Helper function to parse an argument from a redis command.
    fn parse_argument<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        name: &str,
    ) -> Result<String, anyhow::Error> {
        let _length = Self::extract_line(lines, '$')?;
        lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("{} not found", name))
            .map(String::from)
    }

    /// Parses an expiry from a redis command.
    /// Parses the expiry prefix, value and returns the expiry in milliseconds from now.
    fn parse_expiry<'a>(lines: &mut impl Iterator<Item = &'a str>) -> Result<u64, anyhow::Error> {
        let _expiry_prefix_length = Self::extract_line(lines, '$')?;
        let expiry_prefix = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expiry prefix not found"))?;
        info!("Expiry prefix: {}", expiry_prefix);

        let _expiry_value_length = Self::extract_line(lines, '$')?;
        let expiry_str = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expiry value not found"))?;
        info!("Expiry value: {}", expiry_str);

        let expiry = expiry_str
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid expiry format"))?;
        millis_to_timestamp_from_now(expiry)
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
