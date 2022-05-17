//! # Buckets
//!
//! Contain Buckets and Leaky for buckets logic
use crate::de::deserialize_duration;
use crate::de::Token;
use ethereum_types::Address;
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::IpAddr, time::Duration, time::SystemTime};

/// Bucket priority queue where:
/// - key: bucket name
/// - value: last bucket update
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

    /// Get current time without panic
    pub fn current_time() -> u64 {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_secs(),
            Err(e) => {
                tracing::error!("{e:?}");
                0
            }
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

    /// Remove from prioirty queue
    pub fn remove(&mut self, bucket_name: &BucketName) -> Option<(BucketName, u64)> {
        self.0.remove(bucket_name)
    }

    /// Pop from priority queue data that should be freed
    /// and return Buckets list
    pub fn retention_free(&mut self, retention_time: u64) -> Vec<BucketName> {
        let mut buckets = vec![];
        // Check priority queue and if it's ready pop it
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

/// Bucket Identity - basic identifier for Bucket
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum BucketIdentity {
    IP,
    Address,
    Token,
}

/// Bucket name value - specific value for Identity
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum BucketNameValue {
    IP(IpAddr),
    Address(Address),
    Token(Token),
}

/// BUcket error kind - basic errors for ban event
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum BucketErrorKind {
    IncorrectNonce,
    MaxGas,
    Reverts,
    UsedExcessiveGas,
    Custom(String),
}

/// Bucket name represent bucket itself
/// Bucket is: bucket_name => bucket_data
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

/// Bucket value - basic type for all buckets
pub type BucketValue = u64;

/// Bucket data contain value kind
/// for specific bucket, and last bucket
/// update UNIX time in sec
#[derive(Debug, Hash, Clone)]
pub struct BucketData {
    pub value: BucketValue,
    pub last_update: u64,
}

/// Leaky bucket represent Map of key-value of
/// bucket name & bucket_data
pub struct LeakyBucket(HashMap<BucketName, BucketData>);

/// Basic bucket config
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct BucketConfig {
    pub base_size: u64,
    pub leak_rate: u64,
    pub overflow_size: u64,
    #[serde(deserialize_with = "deserialize_duration")]
    pub retention: Duration,
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

    /// Update fill with prepared value by bucket name
    pub fn fill(&mut self, key: &BucketName, value: BucketValue) {
        let bucket_data = BucketData {
            value,
            last_update: BucketPriorityQueue::current_time(),
        };
        *self.0.entry(key.clone()).or_insert(bucket_data) = bucket_data.clone();
    }

    /// Leaky bucket algorithm
    pub fn leaky(&mut self, key: &BucketName, config: &BucketConfig) {
        use std::cmp::max;

        // Get bucket
        let bucket = if let Some(bucket) = self.0.get(key) {
            bucket
        } else {
            return;
        };
        // Calculate leak amount
        let duration = max(86400 / config.leak_rate, 1);
        let current_time = BucketPriorityQueue::current_time();
        let leak_time_delta = current_time - bucket.last_update;
        if leak_time_delta < duration {
            // NOP
            return;
        }
        let leak_amount = config.leak_rate * leak_time_delta / 86400;
        // Fill value always >= 0
        let value = if bucket.value > leak_amount {
            bucket.value - leak_amount
        } else {
            0
        };

        // Decrease bucket value and set last update time
        let data = BucketData {
            value,
            last_update: BucketPriorityQueue::current_time(),
        };
        *self.0.entry(key.clone()).or_insert(data) = data.clone();
    }

    /// Remove leaky bucket  
    pub fn remove(&mut self, key: &BucketName) {
        self.0.remove(key);
    }
}
