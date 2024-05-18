use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, info};

use crate::{
    command::{AdminCommand, RedisCommand, RedisCommandResponse},
    utils::now_millis,
};

use super::{
    base::{BaseServer, RedisServer},
    store::RedisStore,
    types::{RedisInfo, RedisRole},
};

/// A Redis master server implementation.
#[derive(Debug, Clone)]
pub struct Master {
    pub base: BaseServer,
    pub slaves: Vec<String>,
}

impl Master {
    /// Creates a new Redis master server.
    pub fn new(host: &str, port: &str) -> Self {
        let address = format!("{}:{}", host, port);
        Master {
            base: BaseServer {
                info: RedisInfo::new(RedisRole::Master, "", ""),
                address,
                store: RedisStore::new(),
            },
            slaves: Vec::new(),
        }
    }

    pub async fn add_slave(&mut self, slave_address: String) -> Result<(), anyhow::Error> {
        self.slaves.push(slave_address);
        Ok(())
    }

    pub async fn replicate_to_slaves(&self, command: &str) -> Result<(), anyhow::Error> {
        for slave_address in &self.slaves {
            let command_to_send = format!(
                "*2\r\n$9\r\nREPLICATE\r\n${}\r\n{}\r\n",
                command.len(),
                command
            );

            if let Err(e) = self
                .base
                .send_command(&slave_address, &command_to_send)
                .await
            {
                debug!(
                    "Error replicating command to slave at {}: {}",
                    slave_address, e
                );
            }
        }
        Ok(())
    }

    /// Starts a background worker that cleans up expired keys in the store.
    pub async fn expiry_worker(&self) {
        loop {
            if let Some(expiry_time) = self.base.store.next_expiration().await {
                let now = now_millis();
                if expiry_time > now {
                    debug!("Sleeping until expiry time: {}", expiry_time);
                    tokio::time::sleep_until(
                        Instant::now() + Duration::from_millis(expiry_time - now),
                    )
                    .await;
                }
                self.base.store.clean_expired_keys().await;
            } else {
                debug!("No expirations set, sleeping for 10 seconds");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

#[async_trait::async_trait]
impl RedisServer for Master {
    /// Handles a Redis command.
    /// Parses the command, executes it and returns the response.
    async fn handle_command(
        &mut self,
        command: RedisCommand,
    ) -> Result<RedisCommandResponse, anyhow::Error> {
        info!("Handling command: {:?}", command);
        match command {
            RedisCommand::Ping => Ok(RedisCommandResponse::new("PONG".to_string())),
            RedisCommand::Pong => Ok(RedisCommandResponse::new("PING".to_string())),
            RedisCommand::Echo(s) => Ok(RedisCommandResponse::new(s)),
            RedisCommand::Get(key) => match self.base.store.get(&key).await {
                Some(value) => Ok(RedisCommandResponse::new(value)),
                None => Ok(RedisCommandResponse::null()),
            },
            RedisCommand::Info(section) => match section.as_deref() {
                Some("replication") => {
                    let info_message = format!(
                        "role:{}\r\nmaster_host:{}\r\nmaster_port:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                        self.base.info.role, self.base.info.master_host, self.base.info.master_port, self.base.info.master_replid, self.base.info.master_repl_offset
                    );
                    Ok(RedisCommandResponse::new(info_message))
                }
                _ => Ok(RedisCommandResponse::_error(
                    "Unsupported INFO section".to_string(),
                )),
            },
            RedisCommand::Set(key, value, expiry) => {
                self.base.store.set(&key, &value, expiry).await;
                Ok(RedisCommandResponse::new("OK".to_string()))
            }
            RedisCommand::Admin(command) => match command {
                AdminCommand::Replicate(data) => {
                    self.replicate_to_slaves(&data).await?;
                    Ok(RedisCommandResponse::new("OK".to_string()))
                }
                AdminCommand::AddSlave(data) => {
                    self.add_slave(data).await?;
                    Ok(RedisCommandResponse::new("OK".to_string()))
                }
            },
        }
    }
}
