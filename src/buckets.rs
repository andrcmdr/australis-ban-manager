use crate::de::Token;
use ethereum_types::Address;
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::IpAddr, time::SystemTime};

pub struct BucketPriorityQueue(PriorityQueue<BucketName, u64>);

impl Default for BucketPriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketPriorityQueue {
    pub fn new() -> Self {
        Self(PriorityQueue::new())
    }

    pub fn current_time() -> u64 {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_secs(),
            Err(e) => {
                tracing::error!("{e:?}");
                0
            }
        }
    }

    /// Update Bucket pruority to current time
    pub fn update(&mut self, bucket_name: &BucketName) {
        self.0.change_priority(bucket_name, Self::current_time());
    }

    /// Get current preority by key
    pub fn get_priority(&mut self, bucket_name: &BucketName) -> u64 {
        if let Some((_, priority)) = self.0.get(bucket_name) {
            *priority
        } else {
            tracing::error!("Priority queue key not found",);
            0
        }
    }

    ///  Returns the couple (item, priority) with the greatest priority
    /// in the queue, or None if it is empty.
    pub fn peek(&self) -> Option<(&BucketName, &u64)> {
        self.0.peek()
    }

    /// Removes the item with the greatest priority from the priority
    /// queue and returns the pair (item, priority), or None if the queue is empty.
    pub fn pop(&mut self) -> Option<(BucketName, u64)> {
        self.0.pop()
    }

    /// Insert the item-priority pair into the queue.
    /// If an element equal to item was already into the queue, it is updated and the old value of its priority returned in Some; otherwise, returns None.
    pub fn push(&mut self, bucket_name: BucketName) {
        self.0.push(bucket_name, Self::current_time());
    }

    pub fn remove(&mut self, bucket_name: &BucketName) -> Option<(BucketName, u64)> {
        self.0.remove(bucket_name)
    }

    /// Free priority queue
    pub fn retention_free(&mut self, retention_time: u64) -> Vec<BucketName> {
        let mut buckets = vec![];
        while let Some((bucket_name, value)) = self.peek() {
            if Self::current_time() - value > retention_time {
                buckets.push(bucket_name.clone());
                // Remove from queue
                self.pop();
            }
        }
        buckets
    }
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum BucketIdentity {
    IP,
    Address,
    Token,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum BucketNameValue {
    IP(IpAddr),
    Address(Address),
    Token(Token),
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum BucketErrorKind {
    IncorrectNonce,
    MaxGas,
    Reverts,
    UsedExcessiveGas,
    Custom(String),
}

#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct BucketName {
    kind: BucketIdentity,
    value: BucketNameValue,
    error: BucketErrorKind,
}

impl BucketName {
    pub fn new(kind: BucketIdentity, value: BucketNameValue, error: BucketErrorKind) -> Self {
        Self { kind, value, error }
    }
}

pub type BucketValue = u64;

/// Bucket data contain value kind
/// for specific bucket, and last bucket
/// update UNIX time in sec
#[derive(Debug, Hash, Clone)]
pub struct BucketData {
    pub value: BucketValue,
    pub last_update: u64,
}

pub struct LeakyBucket(HashMap<BucketName, BucketData>);

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct BucketConfig {
    pub base_size: u64,
    pub leak_rate: u64,
    pub overflow_size: u64,
    pub retention: u64,
}

impl Default for LeakyBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl LeakyBucket {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Calculate new fill value
    pub fn get_fill(&self, key: &BucketName, value: BucketValue) -> BucketValue {
        let old_value = if let Some(data) = self.0.get(key) {
            data.value
        } else {
            return value;
        };
        old_value + value
    }

    /// Update fill
    pub fn fill(&mut self, key: &BucketName, value: BucketValue) {
        let data = self.0.entry(key.clone()).or_insert(BucketData {
            value,
            last_update: BucketPriorityQueue::current_time(),
        });
        *data = BucketData {
            value,
            last_update: data.last_update,
        };
    }

    /// Leaky bucket
    pub fn leaky(&mut self, key: &BucketName, config: &BucketConfig) {
        use std::cmp::max;

        let bucket = if let Some(bucket) = self.0.get(key) {
            bucket
        } else {
            return;
        };
        let duration = max(86400 / config.leak_rate, 1);
        let current_time = BucketPriorityQueue::current_time();
        let leak_time_detla = current_time - bucket.last_update;
        if leak_time_detla < duration {
            // NOP
            return;
        }
        let leak_amount = config.leak_rate * leak_time_detla / 86400;

        let data = BucketData {
            value: bucket.value - leak_amount,
            last_update: BucketPriorityQueue::current_time(),
        };
        *self.0.entry(key.clone()).or_insert(data) = data.clone();
    }

    pub fn remove(&mut self, key: &BucketName) {
        self.0.remove(key);
    }
}
