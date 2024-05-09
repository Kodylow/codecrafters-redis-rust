use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, info};

use crate::{
    command::{RedisCommand, RedisCommandResponse},
    utils::now_millis,
};

use super::{store::RedisStore, types::RedisInfo, types::RedisRole};

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
