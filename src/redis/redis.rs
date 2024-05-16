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
    pub slaves: Vec<String>,
    pub reqwest_client: reqwest::Client,
}

impl Redis {
    /// Creates a new Redis server. This function initializes an instance with provided parameters.
    pub fn new(
        host: &str,
        port: &str,
        role: RedisRole, // This is now directly passed in, determined by the CLI args
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
            slaves: Vec::new(),
            reqwest_client: reqwest::Client::new(),
        };
        redis
    }

    pub async fn add_slave(&mut self, slave_address: String) {
        self.slaves.push(slave_address);
    }

    pub async fn replicate_to_slaves(&self, command: &str) -> Result<(), anyhow::Error> {
        for slave in &self.slaves {
            let command_to_send = format!("REPLICATE {}\r\n", command);
            let slave_address = format!("http://{}", slave);
            let send_result = self
                .reqwest_client
                .post(&slave_address)
                .body(command_to_send)
                .send()
                .await;
            match send_result {
                Ok(response) => {
                    if response.status().is_success() {
                        debug!("Command '{}' replicated to slave at {}", command, slave);
                    } else {
                        debug!(
                            "Failed to replicate command '{}' to slave at {}: Status {}",
                            command,
                            slave,
                            response.status()
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        "Error replicating command '{}' to slave at {}: {}",
                        command, slave, e
                    );
                }
            }
        }
        Ok(())
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
            RedisCommand::Info(section) => {
                match section.as_deref() {
                    // Use `as_deref` to convert Option<String> to Option<&str>
                    Some("replication") => {
                        let info_message = format!(
                            "role:{}\r\nmaster_host:{}\r\nmaster_port:{}",
                            self.info.role, self.info.master_host, self.info.master_port
                        );
                        Ok(RedisCommandResponse::new(info_message))
                    }
                    _ => {
                        // Handle unsupported sections
                        Ok(RedisCommandResponse::_error(
                            "Unsupported INFO section".to_string(),
                        ))
                    }
                }
            }
            RedisCommand::Set(key, value, expiry) => {
                self.store.set(&key, &value, expiry).await;
                Ok(RedisCommandResponse::new("OK".to_string()))
            }
            RedisCommand::Replicate(data) => {
                self.replicate_to_slaves(&data).await?;
                Ok(RedisCommandResponse::new("OK".to_string()))
            }
        }
    }
}
