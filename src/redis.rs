use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display, str::FromStr, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

use crate::{
    command::{RedisCommand, RedisCommandResponse},
    utils::now_millis,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisRole {
    Master,
    Slave,
}

impl Display for RedisRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl FromStr for RedisRole {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "master" => Ok(RedisRole::Master),
            "slave" => Ok(RedisRole::Slave),
            _ => Err(anyhow::anyhow!("Invalid Redis role")),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisInfo {
    pub role: RedisRole,
    pub master_host: String,
    pub master_port: String,
}

#[derive(Debug, Clone)]
pub struct RedisStore {
    store: Arc<Mutex<BTreeMap<String, (String, Option<u64>)>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        RedisStore {
            store: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let store = self.store.lock().await;
        let value = store.get(key)?;
        if let Some(expiry) = value.1 {
            if now_millis() >= expiry {
                self.remove(key).await;
                return None;
            }
        }
        Some(value.0.clone())
    }

    pub async fn set(&self, key: &str, value: &str, expiry: Option<u64>) {
        let mut store = self.store.lock().await;
        store.insert(key.to_string(), (value.to_string(), expiry));
    }

    pub async fn remove(&self, key: &str) {
        let mut store = self.store.lock().await;
        store.remove(key);
    }
}

/// A Redis server implementation.
#[derive(Debug, Clone)]
pub struct Redis {
    pub info: RedisInfo,
    pub address: String,
    store: RedisStore,
}

impl Redis {
    /// Creates a new Redis server. This function initializes an instance with provided parameters.
    pub fn new(
        host: &str,
        port: &str,
        role: RedisRole,
        master_host: &str,
        master_port: &str,
    ) -> Self {
        let address = format!("{}:{}", host, port);
        let redis = Redis {
            info: RedisInfo {
                role,
                master_host: master_host.to_string(),
                master_port: master_port.to_string(),
            },
            address,
            store: RedisStore::new(),
        };
        redis
    }

    /// Handles a Redis command.
    /// Parses the command, executes it and returns the response.
    pub async fn handle_command(
        &self,
        command: RedisCommand,
    ) -> Result<RedisCommandResponse, anyhow::Error> {
        info!("Handling command: {:?}", command);
        match command {
            RedisCommand::Ping => Ok(RedisCommandResponse::new("PONG".to_string())),
            RedisCommand::Pong => Ok(RedisCommandResponse::new("PING".to_string())),
            RedisCommand::Echo(s) => Ok(RedisCommandResponse::new(s)),
            RedisCommand::Set(key, value, expiry) => {
                self.store.set(&key, &value, expiry).await;
                Ok(RedisCommandResponse::new("OK".to_string()))
            }
            RedisCommand::Get(key) => match self.store.get(&key).await {
                Some(value) => Ok(RedisCommandResponse::new(value)),
                None => Ok(RedisCommandResponse::null()),
            },
            RedisCommand::Info(section) => Ok(self.info(&section)),
        }
    }

    /// Returns information about the server based on the requested section.
    pub fn info(&self, section: &str) -> RedisCommandResponse {
        match section {
            "replication" => {
                let info_message = format!(
                    "role:{}\r\nmaster_host:{}\r\nmaster_port:{}",
                    self.info.role, self.info.master_host, self.info.master_port
                );
                RedisCommandResponse::new(info_message)
            }
            _ => RedisCommandResponse::_error("Unsupported INFO section".to_string()),
        }
    }
}
