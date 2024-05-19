use anyhow::Context;

use crate::command::{AdminCommand, RedisCommand};
use crate::utils::millis_to_timestamp_from_now;

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
        // Remove null characters from the buffer
        let sanitized_buffer: String = buffer_str.chars().filter(|&c| c != '\0').collect();
        let mut lines = sanitized_buffer
            .split("\r\n")
            .filter(|line| !line.is_empty())
            .peekable();

        // Peek at the first character to determine the type of the command
        let first_char = lines
            .peek()
            .context("Empty buffer")?
            .chars()
            .next()
            .context("Empty line")?;

        match first_char {
            '*' => Self::parse_array_command(&mut lines),
            '$' => Self::parse_bulk_string_command(&mut lines),
            _ => Err(anyhow::anyhow!("Invalid protocol format")),
        }
    }

    fn parse_array_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
    ) -> Result<RedisCommand, anyhow::Error> {
        let array_length = Self::parse_array_length(lines)?;

        // Skip the length line for the command itself
        let _ = Self::extract_line(lines, '$')?;

        let command = lines.next().context("Command not found")?.to_lowercase();

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => Self::handle_echo_command(lines, array_length),
            "set" => Self::handle_set_command(lines, array_length),
            "get" => Self::handle_get_command(lines, array_length),
            "info" => Self::handle_info_command(lines, array_length),
            "replconf" => Self::handle_replconf_command(lines, array_length),
            "replicate" | "addslave" => Self::handle_admin_command(lines, array_length),
            _ => Err(anyhow::anyhow!("Unknown Redis command")),
        }
    }

    fn parse_bulk_string_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
    ) -> Result<RedisCommand, anyhow::Error> {
        // Extract the length of the bulk string
        let length_str = Self::extract_line(lines, '$')?;
        let _length: usize = length_str.parse().context("Invalid bulk string length")?;

        // Extract the actual command string
        let command = lines.next().context("Command not found")?;

        match command.to_lowercase().as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "pong" => Ok(RedisCommand::Pong),
            "echo" => Self::handle_echo_command(lines, 2), // Assuming ECHO has 1 argument
            "set" => Self::handle_set_command(lines, 3),   // Assuming SET has 2 arguments
            "get" => Self::handle_get_command(lines, 2),   // Assuming GET has 1 argument
            "info" => Self::handle_info_command(lines, 2), // Assuming INFO has 1 argument
            "replconf" => Self::handle_replconf_command(lines, 2), // Assuming REPLCONF has 1 argument
            "replicate" | "addslave" => Self::handle_admin_command(lines, 2), // Assuming admin commands have 1 argument
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

    fn handle_echo_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<RedisCommand, anyhow::Error> {
        if array_length < 2 {
            anyhow::bail!("ECHO command requires an argument");
        }
        let argument = Self::parse_argument(lines, "Argument")?;
        Ok(RedisCommand::Echo(argument))
    }

    fn handle_get_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<RedisCommand, anyhow::Error> {
        if array_length < 2 {
            anyhow::bail!("GET command requires one argument");
        }
        let key = Self::parse_argument(lines, "Key")?;
        Ok(RedisCommand::Get(key))
    }

    fn handle_set_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<RedisCommand, anyhow::Error> {
        if array_length < 3 {
            anyhow::bail!("SET command requires at least two arguments");
        }

        let key = Self::parse_argument(lines, "Key")?;
        let value = Self::parse_argument(lines, "Value")?;

        let expiry = if array_length >= 5 {
            // Check if the fourth argument is "PX"
            let px_indicator = Self::parse_argument(lines, "PX Indicator")?;
            if px_indicator.to_lowercase() == "px" {
                Some(Self::parse_expiry(lines)?)
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
        let section = if array_length > 1 {
            Some(Self::parse_argument(lines, "Section")?)
        } else {
            None
        };
        Ok(RedisCommand::Info(section))
    }

    fn handle_admin_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        _array_length: usize,
    ) -> Result<RedisCommand, anyhow::Error> {
        let command_type = Self::parse_argument(lines, "Command Type")?;
        let data = Self::parse_argument(lines, "Data")?;
        match command_type.as_str() {
            "replicate" => Ok(RedisCommand::Admin(AdminCommand::Replicate(data))),
            "addslave" => Ok(RedisCommand::Admin(AdminCommand::AddSlave(data))),
            _ => Err(anyhow::anyhow!("Unknown admin command")),
        }
    }

    fn handle_replconf_command<'a>(
        lines: &mut impl Iterator<Item = &'a str>,
        array_length: usize,
    ) -> Result<RedisCommand, anyhow::Error> {
        let args = (0..array_length - 1)
            .map(|_| Self::parse_argument(lines, "Argument"))
            .collect::<Result<Vec<String>, anyhow::Error>>()?;
        Ok(RedisCommand::Replconf(args))
    }
}
