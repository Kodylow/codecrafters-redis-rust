use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

/// Enum for administrative commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AdminCommand {
    Replicate(String),
    AddSlave(String),
}

/// Enum for supported Redis protocol commands
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisCommand {
    Ping,
    Pong,
    Echo(String),
    Get(String),
    Set(String, String, Option<u64>),
    Info(Option<String>),
    Admin(AdminCommand),
    Replconf(Vec<String>),
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
            RedisCommand::Info(section) => match section {
                Some(section) => write!(f, "INFO {}", section),
                None => write!(f, "INFO"),
            },
            RedisCommand::Admin(command) => match command {
                AdminCommand::Replicate(data) => write!(f, "REPLICATE {}", data),
                AdminCommand::AddSlave(data) => write!(f, "ADDSLAVE {}", data),
            },
            RedisCommand::Replconf(data) => write!(f, "REPLCONF {}", data.join(" ")),
        }
    }
}

impl RedisCommand {
    pub fn is_write_operation(&self) -> bool {
        matches!(self, RedisCommand::Set(_, _, _))
    }

    pub fn to_resp2(&self) -> String {
        let command_str = self.to_string();
        let parts: Vec<&str> = command_str.split_whitespace().collect();
        let mut resp2 = format!("*{}\r\n", parts.len());
        for part in parts {
            resp2.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
        }
        resp2
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
