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
    Info(Option<String>),
    Replicate(String),
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
            RedisCommand::Replicate(s) => write!(f, "REPLICATE {}", s),
            RedisCommand::Info(section) => match section {
                Some(section) => write!(f, "INFO {}", section),
                None => write!(f, "INFO"),
            },
        }
    }
}

impl RedisCommand {
    pub fn is_write_operation(&self) -> bool {
        match self {
            RedisCommand::Set(_, _, _) => true,
            _ => false,
        }
    }
}

pub struct RedisCommandParser;

impl RedisCommandParser {
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
    pub fn parse(buffer_str: &str) -> Result<RedisCommand, anyhow::Error> {
        let mut lines = buffer_str.split("\r\n").filter(|line| !line.is_empty());

        let array_length = Self::parse_array_length(&mut lines)?;

        // Skip the length line for the command itself
        let _ = Self::extract_line(&mut lines, '$')?;

        let command = lines.next().context("Command not found")?.to_lowercase();

        info!("Parsed command: {}", command);

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => handle_echo_command(&mut lines, array_length),
            "set" => handle_set_command(&mut lines, array_length),
            "get" => handle_get_command(&mut lines, array_length),
            "info" => handle_info_command(&mut lines, array_length),
            "replicate" => handle_replicate_command(&mut lines, array_length),
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
        let expiry_millis = expiry_str.parse::<u64>().context("Invalid expiry format")?;
        Ok(millis_to_timestamp_from_now(expiry_millis)?)
    }
}

fn handle_echo_command<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    array_length: usize,
) -> Result<RedisCommand, anyhow::Error> {
    if array_length < 2 {
        anyhow::bail!("ECHO command requires an argument");
    }
    let argument = RedisCommandParser::parse_argument(lines, "Argument")?;
    Ok(RedisCommand::Echo(argument))
}

fn handle_get_command<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    array_length: usize,
) -> Result<RedisCommand, anyhow::Error> {
    if array_length < 2 {
        anyhow::bail!("GET command requires one argument");
    }
    let key = RedisCommandParser::parse_argument(lines, "Key")?;
    Ok(RedisCommand::Get(key))
}

fn handle_set_command<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    array_length: usize,
) -> Result<RedisCommand, anyhow::Error> {
    if array_length < 3 {
        anyhow::bail!("SET command requires at least two arguments");
    }

    let key = RedisCommandParser::parse_argument(lines, "Key")?;
    let value = RedisCommandParser::parse_argument(lines, "Value")?;

    let expiry = if array_length >= 5 {
        // Check if the fourth argument is "PX"
        let px_indicator = RedisCommandParser::parse_argument(lines, "PX Indicator")?;
        if px_indicator.to_lowercase() == "px" {
            Some(RedisCommandParser::parse_expiry(lines)?)
        } else {
            None
        }
    } else {
        None
    };

    Ok(RedisCommand::Set(key, value, expiry))
}

fn handle_info_command<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    array_length: usize,
) -> Result<RedisCommand, anyhow::Error> {
    if array_length < 2 {
        anyhow::bail!("INFO command requires a section argument");
    }
    let section = RedisCommandParser::parse_argument(lines, "Section")?;
    Ok(RedisCommand::Info(Some(section)))
}

fn handle_replicate_command<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    array_length: usize,
) -> Result<RedisCommand, anyhow::Error> {
    if array_length < 2 {
        anyhow::bail!("REPLICATE command requires data argument");
    }
    let data = RedisCommandParser::parse_argument(lines, "Data")?;
    Ok(RedisCommand::Replicate(data))
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
