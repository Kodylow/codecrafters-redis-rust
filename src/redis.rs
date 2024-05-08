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
    time::Instant,
};
use tracing::info;

use crate::command::{RedisCommand, RedisCommandResponse};

#[derive(Debug, Clone)]
pub struct Redis {
    store: Arc<Mutex<BTreeMap<String, (String, Option<u64>)>>>,
    expiries: Arc<Mutex<BinaryHeap<Reverse<(Instant, String)>>>>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            store: Arc::new(Mutex::new(BTreeMap::new())),
            expiries: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

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

    pub async fn start_expiry_checker(self) {
        info!("Starting expiry checker");
        let store = self.store.clone();
        let expiries = self.expiries.clone();
        tokio::spawn(async move {
            loop {
                let mut next_expiry = None;
                {
                    let expiries_lock = expiries.lock().await;
                    if let Some(Reverse((time, _))) = expiries_lock.peek() {
                        next_expiry = Some(*time);
                    }
                }
                match next_expiry {
                    Some(next) => {
                        let now = Instant::now();
                        if now >= next {
                            let mut store = store.lock().await;
                            let mut expiries_lock = expiries.lock().await;
                            while let Some(Reverse((time, key))) = expiries_lock.pop() {
                                if time <= now {
                                    info!("Expiring key {}", key);
                                    store.remove(&key);
                                } else {
                                    expiries_lock.push(Reverse((time, key)));
                                    break;
                                }
                            }
                        }
                        tokio::time::sleep_until(next.into()).await;
                    }
                    None => tokio::time::sleep(Duration::from_secs(10)).await, // Sleep longer if no expiries are pending
                }
            }
        });
    }

    async fn process_client_req(self, mut stream: TcpStream) -> anyhow::Result<()> {
        let mut buffer = vec![0; 1024];
        loop {
            let n = stream.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            let command = RedisCommand::parse(&buffer)?;
            info!("Received command: {}", command);
            let response = self.clone().handle_command(command).await;
            stream.write_all(response.message.as_bytes()).await?;
            info!("Sent response: {}", response.message);
            buffer.fill(0);
        }
        Ok(())
    }

    pub async fn handle_command(self, command: RedisCommand) -> RedisCommandResponse {
        info!("Handling command: {:?}", command);
        match command {
            RedisCommand::Ping => RedisCommandResponse::new("PONG".to_string()),
            RedisCommand::Pong => RedisCommandResponse::new("PING".to_string()),
            RedisCommand::Echo(s) => RedisCommandResponse::new(s),
            RedisCommand::Set(key, value, expiry) => {
                let mut store = self.store.lock().await;
                if let Some(expiry) = expiry {
                    self.expiries.lock().await.push(Reverse((
                        Instant::now() + Duration::from_millis(expiry),
                        key.clone(),
                    )));
                }
                store.insert(key, (value, expiry));
                RedisCommandResponse::new("OK".to_string())
            }

            RedisCommand::Get(key) => {
                let store = self.store.lock().await;
                match store.get(&key) {
                    Some((value, _expiry)) => RedisCommandResponse::new(value.clone()),
                    None => RedisCommandResponse::new("(nil)".to_string()),
                }
            }
        }
    }
}
