use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

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
