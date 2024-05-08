use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tracing::info;

use crate::{
    command::{RedisCommand, RedisCommandResponse},
    utils::{millis_to_timestamp_from_now, now_millis},
};

/// A Redis server implementation.
#[derive(Debug, Clone)]
pub struct Redis {
    store: Arc<Mutex<BTreeMap<String, (String, Option<u64>)>>>,
    expiries: Arc<Mutex<BinaryHeap<Reverse<(u64, String)>>>>,
}

impl Redis {
    /// Creates a new Redis server.
    pub fn new() -> Self {
        Redis {
            store: Arc::new(Mutex::new(BTreeMap::new())),
            expiries: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    /// Starts the Redis server.
    /// Handles incoming connections and processes commands.
    pub async fn serve(self, address: &str) -> anyhow::Result<()> {
        self.clone().start_expiry_checker().await;
        let listener = TcpListener::bind(address).await?;
        info!("Redis Server listening on {}", address);
        loop {
            let (stream, _) = listener.accept().await?;
            let self_clone = self.clone();
            tokio::spawn(async move { self_clone.process_client_req(stream).await });
        }
    }

    /// Starts the expiry checker.
    /// Checks the expiry of keys and removes them from the store if they have expired.
    /// Uses a BinaryHeap to keep track of the expiry times in order of expiry.
    pub async fn start_expiry_checker(self) {
        info!("Starting expiry checker");
        let store = self.store.clone();
        let expiries = self.expiries.clone();
        tokio::spawn(async move {
            loop {
                let mut next_expiry = None;
                {
                    let expiries_lock = expiries.lock().await;
                    if let Some(Reverse((expiry_millis, _))) = expiries_lock.peek() {
                        next_expiry = Some(*expiry_millis);
                    }
                }
                match next_expiry {
                    Some(next_timestamp) => {
                        let now = now_millis();
                        if now >= next_timestamp {
                            let mut store = store.lock().await;
                            let mut expiries_lock = expiries.lock().await;
                            while let Some(Reverse((expiry_millis, key))) = expiries_lock.pop() {
                                if expiry_millis <= now {
                                    info!("Expiring key {}", key);
                                    store.remove(&key);
                                } else {
                                    expiries_lock.push(Reverse((expiry_millis, key)));
                                    break;
                                }
                            }
                        }
                        let next_duration =
                            std::time::Duration::from_millis(next_timestamp.saturating_sub(now));
                        tokio::time::sleep(next_duration).await;
                    }
                    None => tokio::time::sleep(Duration::from_secs(10)).await, // Sleep longer if no expiries are pending
                }
            }
        });
    }

    /// Processes a client request.
    /// Handles all Redis commands and sends the response back to the client.
    async fn process_client_req(self, mut stream: TcpStream) -> anyhow::Result<()> {
        let mut buffer = vec![0; 1024];
        loop {
            let n = stream.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            let command = RedisCommand::parse(&buffer)?;
            info!("Received command: {}", command);
            let response = self.clone().handle_command(command).await?;
            stream.write_all(response.message.as_bytes()).await?;
            info!("Sent response: {}", response.message);
            buffer.fill(0);
        }
        Ok(())
    }

    /// Handles a Redis command.
    /// Parses the command, executes it and returns the response.
    pub async fn handle_command(
        self,
        command: RedisCommand,
    ) -> Result<RedisCommandResponse, anyhow::Error> {
        info!("Handling command: {:?}", command);
        match command {
            RedisCommand::Ping => Ok(RedisCommandResponse::new("PONG".to_string())),
            RedisCommand::Pong => Ok(RedisCommandResponse::new("PING".to_string())),
            RedisCommand::Echo(s) => Ok(RedisCommandResponse::new(s)),
            RedisCommand::Set(key, value, expiry) => {
                self.insert_with_expiry(key, value, expiry).await?;
                Ok(RedisCommandResponse::new("OK".to_string()))
            }
            RedisCommand::Get(key) => match self.get(key).await {
                Some(value) => Ok(RedisCommandResponse::new(value)),
                None => Ok(RedisCommandResponse::null()),
            },
        }
    }

    /// Inserts a value into the store with an optional expiry.
    async fn insert_with_expiry(
        &self,
        key: String,
        value: String,
        expiry: Option<u64>,
    ) -> anyhow::Result<()> {
        let mut store = self.store.lock().await;
        store.insert(key.clone(), (value, expiry));
        if let Some(expiry) = expiry {
            let expiry_timestamp = millis_to_timestamp_from_now(expiry)?;
            self.expiries
                .lock()
                .await
                .push(Reverse((expiry_timestamp, key)));
        }
        Ok(())
    }

    /// Gets a value from the store and returns it.
    /// If the value is expired, returns None.
    pub async fn get(&self, key: String) -> Option<String> {
        let store = self.store.lock().await;
        let now = now_millis();
        match store.get(&key) {
            Some((value, expiry)) => {
                if let Some(expiry) = expiry {
                    if now >= *expiry {
                        return None;
                    }
                }
                Some(value.clone())
            }
            None => None,
        }
    }
}
