use tracing::{debug, error, info};

use crate::command::{AdminCommand, RedisCommand, RedisCommandResponse};

use super::{
    base::{BaseServer, RedisServer},
    store::RedisStore,
    types::{RedisInfo, RedisRole},
};

/// A Redis slave server implementation.
#[derive(Debug, Clone)]
pub struct Slave {
    pub base: BaseServer,
}

impl Slave {
    /// Creates a new Redis slave server.
    pub fn new(host: &str, port: &str, master_host: &str, master_port: &str) -> Self {
        let address = format!("{}:{}", host, port);
        Slave {
            base: BaseServer {
                info: RedisInfo::new(RedisRole::Slave, master_host, master_port),
                address,
                store: RedisStore::new(),
            },
        }
    }

    /// Sends a command to the master.
    pub async fn send_command_to_master(
        &self,
        command: RedisCommand,
    ) -> Result<String, anyhow::Error> {
        let master_address = format!(
            "{}:{}",
            self.base.info.master_host, self.base.info.master_port
        );
        let command_str = command.to_resp2();

        let response = self
            .base
            .send_command(&master_address, &command_str)
            .await?;

        if response.starts_with("+") {
            info!(
                "Successfully sent command to master at {}: {}",
                master_address, response
            );
        } else {
            debug!(
                "Failed to send command to master at {}: Response {}",
                master_address, response
            );
        }
        Ok(response)
    }

    /// Performs the handshake with the master.
    pub async fn handshake_with_master(&self) -> Result<(), anyhow::Error> {
        // Send PING command
        let ping_response = self.send_command_to_master(RedisCommand::Ping).await?;
        if !ping_response.starts_with("+PONG") {
            return Err(anyhow::anyhow!("Failed to receive PONG from master"));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RedisServer for Slave {
    /// Handles a Redis command.
    /// Parses the command, executes it and returns the response.
    async fn handle_command(
        &mut self,
        command: RedisCommand,
    ) -> Result<RedisCommandResponse, anyhow::Error> {
        info!("Handling command: {:?}", command);
        match command {
            RedisCommand::Ping => Ok(RedisCommandResponse::new("PONG".to_string())),
            RedisCommand::Pong => Ok(RedisCommandResponse::new("REPLCONF".to_string())),
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
                AdminCommand::Replicate(_) => {
                    // Slaves should not handle replication commands
                    Ok(RedisCommandResponse::_error(
                        "Replication command not supported on slave".to_string(),
                    ))
                }
                AdminCommand::AddSlave(_) => {
                    // Slaves should not handle adding slaves
                    Ok(RedisCommandResponse::_error(
                        "AddSlave command not supported on slave".to_string(),
                    ))
                }
            },
            RedisCommand::Replconf(_data) => {
                error!("Slaves do not support REPLCONF");
                Ok(RedisCommandResponse::_error(
                    "REPLCONF command not supported on slave".to_string(),
                ))
            }
        }
    }
}
