use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::Instant,
};
use tracing::{debug, info};

use crate::{
    command::{AdminCommand, RedisCommand, RedisCommandResponse},
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
        Redis {
            info: RedisInfo::new(role, master_host, master_port),
            address,
            store: RedisStore::new(),
            slaves: Vec::new(),
        }
    }

    pub async fn add_slave(&mut self, slave_address: String) -> Result<(), anyhow::Error> {
        self.slaves.push(slave_address);
        Ok(())
    }

    pub fn is_slave(&self) -> bool {
        self.info.role == RedisRole::Slave
    }

    pub async fn replicate_to_slaves(&self, command: &str) -> Result<(), anyhow::Error> {
        for slave in &self.slaves {
            let command_to_send = format!(
                "*2\r\n$9\r\nREPLICATE\r\n${}\r\n{}\r\n",
                command.len(),
                command
            );
            let slave_address = format!("{}:{}", slave, "6379"); // Assuming default Redis port 6379

            if let Err(e) = self
                .send_command_to_slave(&slave_address, &command_to_send)
                .await
            {
                debug!("Error replicating command to slave at {}: {}", slave, e);
            }
        }
        Ok(())
    }

    async fn send_command(&self, address: &str, command: &str) -> Result<String, anyhow::Error> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(command.as_bytes()).await?;

        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).await?;
        let response = std::str::from_utf8(&buffer[..n])?.to_string();

        Ok(response)
    }

    async fn send_command_to_slave(
        &self,
        slave_address: &str,
        command: &str,
    ) -> Result<(), anyhow::Error> {
        let response = self.send_command(slave_address, command).await?;

        if response.starts_with("+OK") {
            debug!("Command replicated to slave at {}", slave_address);
        } else {
            debug!(
                "Failed to replicate command to slave at {}: Response {}",
                slave_address, response
            );
        }
        Ok(())
    }

    /// Sends a command to the master.
    pub async fn send_command_to_master(
        &self,
        command: RedisCommand,
    ) -> Result<String, anyhow::Error> {
        let master_address = format!("{}:{}", self.info.master_host, self.info.master_port);
        let command_str = command.to_resp2();

        let response = self.send_command(&master_address, &command_str).await?;

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

    /// Starts a background worker that cleans up expired keys in the store.
    pub async fn expiry_worker(&self) {
        loop {
            if let Some(expiry_time) = self.store.next_expiration().await {
                let now = now_millis();
                if expiry_time > now {
                    debug!("Sleeping until expiry time: {}", expiry_time);
                    tokio::time::sleep_until(
                        Instant::now() + Duration::from_millis(expiry_time - now),
                    )
                    .await;
                }
                self.store.clean_expired_keys().await;
            } else {
                debug!("No expirations set, sleeping for 10 seconds");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }

    /// Handles a Redis command.
    /// Parses the command, executes it and returns the response.
    pub async fn handle_command(
        &mut self,
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
            RedisCommand::Info(section) => match section.as_deref() {
                Some("replication") => {
                    let info_message = format!(
                            "role:{}\r\nmaster_host:{}\r\nmaster_port:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                            self.info.role, self.info.master_host, self.info.master_port, self.info.master_replid, self.info.master_repl_offset
                        );
                    Ok(RedisCommandResponse::new(info_message))
                }
                _ => Ok(RedisCommandResponse::_error(
                    "Unsupported INFO section".to_string(),
                )),
            },
            RedisCommand::Set(key, value, expiry) => {
                self.store.set(&key, &value, expiry).await;
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
