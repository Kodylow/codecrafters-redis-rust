use serde::{Deserialize, Serialize};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fmt::Display,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::RwLock, time::Instant};
use tracing::{debug, info};

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
        match self {
            RedisRole::Master => write!(f, "master"),
            RedisRole::Slave => write!(f, "slave"),
        }
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
    store: Arc<RwLock<BTreeMap<String, (String, Option<u64>)>>>,
    expirations: Arc<RwLock<BinaryHeap<Reverse<(u64, String)>>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        RedisStore {
            store: Arc::new(RwLock::new(BTreeMap::new())),
            expirations: Arc::new(RwLock::new(BinaryHeap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let store = self.store.read().await;
        let value = store.get(key)?;
        if let Some(expiry) = value.1 {
            if now_millis() >= expiry {
                drop(store);
                self.remove(key).await;
                return None;
            }
        }
        Some(value.0.clone())
    }

    pub async fn set(&self, key: &str, value: &str, expiry: Option<u64>) {
        let mut store = self.store.write().await;
        let mut expirations = self.expirations.write().await;
        if let Some(expiry_time) = expiry {
            expirations.push(Reverse((expiry_time, key.to_string())));
        }
        store.insert(key.to_string(), (value.to_string(), expiry));
    }

    pub async fn remove(&self, key: &str) {
        let mut store = self.store.write().await;
        store.remove(key);
    }

    pub async fn next_expiration(&self) -> Option<u64> {
        let expirations = self.expirations.read().await;
        expirations.peek().map(|exp| exp.0 .0)
    }

    pub async fn clean_expired_keys(&self) {
        info!("Cleaning expired keys");
        let mut store = self.store.write().await;
        let mut expirations = self.expirations.write().await;
        let now = now_millis();
        while let Some(Reverse((expiry_time, key))) = expirations.peek() {
            if *expiry_time <= now {
                info!("Removing expired key: {}", key);
                store.remove(key);
                expirations.pop();
            } else {
                break;
            }
        }
    }
}

/// A Redis server implementation.
#[derive(Debug, Clone)]
pub struct Redis {
    pub info: RedisInfo,
    pub address: String,
    pub store: RedisStore,
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

    /// Starts a background worker that cleans up expired keys in the store.
    pub async fn expiry_worker(&self) {
        loop {
            let next_expiry = self.store.next_expiration().await;
            match next_expiry {
                Some(expiry_time) => {
                    let now = now_millis();
                    if expiry_time > now {
                        // Sleep until the next expiry time or wake up earlier if a new earlier expiry is set
                        debug!("Sleeping until expiry time: {}", expiry_time);
                        tokio::time::sleep_until(
                            Instant::now() + Duration::from_millis(expiry_time - now),
                        )
                        .await;
                    }
                    // Clean expired keys after waking up
                    self.store.clean_expired_keys().await;
                }
                None => {
                    // If no expirations are set, sleep for a long duration (e.g., 60 secs) or until a new key is set
                    debug!("No expirations set, sleeping for 10 seconds");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
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
            RedisCommand::Get(key) => match self.store.get(&key).await {
                Some(value) => Ok(RedisCommandResponse::new(value)),
                None => Ok(RedisCommandResponse::null()),
            },
            RedisCommand::Info(section) => Ok(self.info(&section)),
            RedisCommand::Set(key, value, expiry) => {
                self.store.set(&key, &value, expiry).await;
                Ok(RedisCommandResponse::new("OK".to_string()))
            }
        }
    }
}
