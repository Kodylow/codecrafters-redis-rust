use crate::redis::types::RedisRole;
use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[clap(version = "1.0", author = "Kody Low <kodylow7@gmail.com>")]
pub struct Cli {
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    #[clap(long, default_value = "6379")]
    pub port: String,

    #[clap(long, default_value = "master")]
    pub role: RedisRole,

    #[clap(long)]
    pub replicaof: Option<String>,
}

impl Cli {
    pub fn determine_role(&self) -> RedisRole {
        match self.replicaof {
            Some(_) => RedisRole::Slave,
            None => self.role,
        }
    }

    pub fn get_master_info(&self) -> Result<(String, String)> {
        if let Some(replica) = &self.replicaof {
            let parts: Vec<&str> = replica.split(' ').collect();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Expected format for replicaof: \"<host> <port>\""
                ));
            }
            Ok((parts[0].to_string(), parts[1].to_string()))
        } else {
            Ok((self.host.clone(), self.port.clone()))
        }
    }
}
