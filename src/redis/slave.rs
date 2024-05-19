use anyhow::Context;
use tracing::{error, info};

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

        info!("Sending command to master: {}", command_str);

        let response = self
            .base
            .send_command(&master_address, &command_str)
            .await?;

        if response != RedisCommandResponse::new("OK".to_string()).to_string() {
            error!("Failed to send command to master, response: {}", response);
            return Err(anyhow::anyhow!("Failed to send command to master"));
        }

        Ok(response)
    }

    /// Performs the handshake with the master.
    pub async fn handshake(&self) -> Result<(), anyhow::Error> {
        // Send PING command to master
        info!("Sending PING command to master");
        let response = self.send_command_to_master(RedisCommand::Ping).await?;
        if response != RedisCommandResponse::new("PONG".to_string()).to_string() {
            return Err(anyhow::anyhow!("Failed to send PING command to master"));
        }

        Ok(())
    }

    /// Sends a REPLCONF command to the master.
    pub async fn replconf(&self) -> Result<String, anyhow::Error> {
        // Send REPLCONF listening-port <PORT>
        let port = self
            .base
            .info
            .master_port
            .parse::<u16>()
            .context("Invalid port number")?;
        let listening_port_command =
            RedisCommand::Replconf(vec!["listening-port".to_string(), port.to_string()]);
        self.send_command_to_master(listening_port_command).await?;

        let capa_psync2_command =
            RedisCommand::Replconf(vec!["capa".to_string(), "psync2".to_string()]);
        let _ = self.send_command_to_master(capa_psync2_command).await?;

        Ok("REPLCONF commands sent successfully".to_string())
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
            RedisCommand::Pong => {
                info!("Slave received PONG command");
                let replconf_response = self.replconf().await?;
                Ok(RedisCommandResponse::new(replconf_response))
            }
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
            RedisCommand::Ok => Ok(RedisCommandResponse::new("OK".to_string())),
        }
    }
}
