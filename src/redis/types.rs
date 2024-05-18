use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisRole {
    Master,
    Slave,
}

impl RedisRole {
    pub fn as_str(&self) -> &str {
        match self {
            RedisRole::Master => "master",
            RedisRole::Slave => "slave",
        }
    }
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

use rand::{distributions::Alphanumeric, Rng};

#[derive(Debug, Clone)]
pub struct RedisInfo {
    pub role: RedisRole,
    pub master_host: String,
    pub master_port: String,
    pub master_replid: String,
    pub master_repl_offset: u64,
}

impl RedisInfo {
    pub fn new(role: RedisRole, master_host: &str, master_port: &str) -> Self {
        let master_replid: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();

        RedisInfo {
            role,
            master_host: master_host.to_string(),
            master_port: master_port.to_string(),
            master_replid,
            master_repl_offset: 0,
        }
    }
}
