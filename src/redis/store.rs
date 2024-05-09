use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::info;

use crate::utils::now_millis;

#[derive(Debug, Clone)]
pub struct RedisStore {
    store: Arc<RwLock<BTreeMap<String, (String, Option<u64>)>>>,
    expirations: Arc<RwLock<BinaryHeap<Reverse<(u64, String)>>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        RedisStore {
            store: Arc::new(RwLock::new(BTreeMap::new())),
            expirations: Arc::new(RwLock::new(BinaryHeap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let store = self.store.read().await;
        let value = store.get(key)?;
        if let Some(expiry) = value.1 {
            if now_millis() >= expiry {
                drop(store);
                self.remove(key).await;
                return None;
            }
        }
        Some(value.0.clone())
    }

    pub async fn set(&self, key: &str, value: &str, expiry: Option<u64>) {
        let mut store = self.store.write().await;
        let mut expirations = self.expirations.write().await;
        if let Some(expiry_time) = expiry {
            expirations.push(Reverse((expiry_time, key.to_string())));
        }
        store.insert(key.to_string(), (value.to_string(), expiry));
    }

    pub async fn remove(&self, key: &str) {
        let mut store = self.store.write().await;
        store.remove(key);
    }

    pub async fn next_expiration(&self) -> Option<u64> {
        let expirations = self.expirations.read().await;
        expirations.peek().map(|exp| exp.0 .0)
    }

    pub async fn clean_expired_keys(&self) {
        info!("Cleaning expired keys");
        let mut store = self.store.write().await;
        let mut expirations = self.expirations.write().await;
        let now = now_millis();
        while let Some(Reverse((expiry_time, key))) = expirations.peek() {
            if *expiry_time <= now {
                info!("Removing expired key: {}", key);
                store.remove(key);
                expirations.pop();
            } else {
                break;
            }
        }
    }
}
