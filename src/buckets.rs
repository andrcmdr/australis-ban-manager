use crate::de::Token;
use ethereum_types::Address;
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use std::{collections::HashMap, net::IpAddr, time::SystemTime};

pub struct BucketPriorityQueue(PriorityQueue<BucketName, u128>);

impl BucketPriorityQueue {
    pub fn new() -> Self {
        Self(PriorityQueue::new())
    }

    pub fn current_time() -> u128 {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_millis(),
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
    pub fn get_priority(&mut self, bucket_name: &BucketName) -> u128 {
        if let Some((_, priority)) = self.0.get(bucket_name) {
            *priority
        } else {
            tracing::error!("Priority queue key not found",);
            0
        }
    }

    ///  Returns the couple (item, priority) with the greatest priority
    /// in the queue, or None if it is empty.
    pub fn peek(&self) -> Option<(&BucketName, &u128)> {
        self.0.peek()
    }

    /// Removes the item with the greatest priority from the priority
    /// queue and returns the pair (item, priority), or None if the queue is empty.
    pub fn pop(&mut self) -> Option<(BucketName, u128)> {
        self.0.pop()
    }

    /// Insert the item-priority pair into the queue.
    /// If an element equal to item was already into the queue, it is updated and the old value of its priority returned in Some; otherwise, returns None.
    pub fn push(&mut self, bucket_name: BucketName) {
        self.0.push(bucket_name, Self::current_time());
    }

    pub fn remove(&mut self, bucket_name: &BucketName) -> Option<(BucketName, u128)> {
        self.0.remove(bucket_name)
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum BucketValue {
    IncorrectNonce(u32),
    MaxGas(u32),
    Reverts(u32),
    UsedExcessiveGas(u128),
    Custom(u32),
}

pub struct LeakyBucket(HashMap<BucketName, BucketValue>);

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct BucketConfig {
    pub base_size: u64,
    pub leak_rate: u64,
    pub overflow_size: u64,
    pub retention: u64,
}

impl Add for BucketValue {
    type Output = Self;

    fn add(self, value: Self) -> Self::Output {
        match self {
            BucketValue::IncorrectNonce(val_l) => {
                if let BucketValue::IncorrectNonce(val_r) = value.clone() {
                    BucketValue::IncorrectNonce(val_l + val_r)
                } else {
                    unimplemented!()
                }
            }
            BucketValue::MaxGas(val_l) => {
                if let BucketValue::MaxGas(val_r) = value.clone() {
                    BucketValue::MaxGas(val_l + val_r)
                } else {
                    unimplemented!()
                }
            }
            BucketValue::UsedExcessiveGas(val_l) => {
                if let BucketValue::UsedExcessiveGas(val_r) = value.clone() {
                    BucketValue::UsedExcessiveGas(val_l + val_r)
                } else {
                    unimplemented!()
                }
            }
            BucketValue::Reverts(val_l) => {
                if let BucketValue::Reverts(val_r) = value.clone() {
                    BucketValue::Reverts(val_l + val_r)
                } else {
                    unimplemented!()
                }
            }
            _ => todo!("Add case fot custom error"),
        }
    }
}

impl Sub for BucketValue {
    type Output = Self;

    /// If right values is greate or equal to left value
    /// just set zero value directly
    fn sub(self, value: Self) -> Self::Output {
        match self {
            BucketValue::IncorrectNonce(val_l) => {
                if let BucketValue::IncorrectNonce(val_r) = value.clone() {
                    if val_l > val_r {
                        BucketValue::IncorrectNonce(val_l - val_r)
                    } else {
                        BucketValue::IncorrectNonce(0)
                    }
                } else {
                    unimplemented!()
                }
            }
            BucketValue::MaxGas(val_l) => {
                if let BucketValue::MaxGas(val_r) = value.clone() {
                    if val_l > val_r {
                        BucketValue::MaxGas(val_l - val_r)
                    } else {
                        BucketValue::MaxGas(0)
                    }
                } else {
                    unimplemented!()
                }
            }
            BucketValue::UsedExcessiveGas(val_l) => {
                if let BucketValue::UsedExcessiveGas(val_r) = value.clone() {
                    if val_l > val_r {
                        BucketValue::UsedExcessiveGas(val_l - val_r)
                    } else {
                        BucketValue::UsedExcessiveGas(0)
                    }
                } else {
                    unimplemented!()
                }
            }
            BucketValue::Reverts(val_l) => {
                if let BucketValue::Reverts(val_r) = value.clone() {
                    if val_l > val_r {
                        BucketValue::Reverts(val_l - val_r)
                    } else {
                        BucketValue::Reverts(0)
                    }
                } else {
                    unimplemented!()
                }
            }
            _ => todo!("Add case fot custom error"),
        }
    }
}

impl LeakyBucket {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Calculate new fill value
    pub fn get_fill(&self, key: &BucketName, value: BucketValue) -> BucketValue {
        let old_value = if let Some(val) = self.0.get(&key) {
            val
        } else {
            return value;
        };
        match key.error {
            BucketErrorKind::IncorrectNonce => {
                if let BucketValue::IncorrectNonce(_) = value.clone() {
                    old_value.clone() + value
                } else {
                    todo!("Add error handling");
                }
            }
            BucketErrorKind::MaxGas => {
                if let BucketValue::MaxGas(_) = value.clone() {
                    old_value.clone() + value
                } else {
                    todo!("Add error handling");
                }
            }
            BucketErrorKind::UsedExcessiveGas => {
                if let BucketValue::UsedExcessiveGas(_) = value.clone() {
                    old_value.clone() + value
                } else {
                    todo!("Add error handling");
                }
            }
            BucketErrorKind::Reverts => {
                if let BucketValue::Reverts(_) = value.clone() {
                    old_value.clone() + value
                } else {
                    todo!("Add error handling");
                }
            }
            _ => todo!("Add case fot custom error"),
        }
    }

    /// Update fill
    pub fn fill(&mut self, key: BucketName, value: BucketValue) {
        *self.0.entry(key.clone()).or_insert(value.clone()) = value.clone();
    }

    /// Decrease bucket value
    pub fn decrease(&mut self, key: BucketName, value: BucketValue) {
        let old_value = if let Some(val) = self.0.get(&key) {
            val
        } else {
            return;
        };
        let new_value = match key.error {
            BucketErrorKind::IncorrectNonce => {
                if let BucketValue::IncorrectNonce(_) = value.clone() {
                    old_value.clone() - value
                } else {
                    todo!("Add error handling");
                }
            }
            BucketErrorKind::MaxGas => {
                if let BucketValue::MaxGas(_) = value.clone() {
                    old_value.clone() - value
                } else {
                    todo!("Add error handling");
                }
            }
            BucketErrorKind::UsedExcessiveGas => {
                if let BucketValue::UsedExcessiveGas(_) = value.clone() {
                    old_value.clone() - value
                } else {
                    todo!("Add error handling");
                }
            }
            BucketErrorKind::Reverts => {
                if let BucketValue::Reverts(_) = value.clone() {
                    old_value.clone() - value
                } else {
                    todo!("Add error handling");
                }
            }
            _ => todo!("Add case fot custom error"),
        };
        *self.0.entry(key.clone()).or_insert(new_value.clone()) = new_value.clone();
    }

    /// Leaky bucket
    pub fn leaky(&mut self, key: BucketName, value: BucketValue) {
        self.decrease(key, value)
    }
}

/*
#[derive
(Debug, Serialize, Deserialize, Clone)]
pub struct NamedBucketConfig {
    pub name: String,
    pub base_size: u64,
    pub leak_rate: u64,
    pub overflow_size: u64,
    pub retention: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BucketsConfig {
    pub near_gas: BucketConfig,
    pub eth_gas: BucketConfig,
    pub free_gas: BucketConfig,
    pub default_relayer_err: BucketConfig,
    pub default_engine_err: BucketConfig,
    pub default_evm_revert: BucketConfig,
    pub relayer_errors: Vec<NamedBucketConfig>,
    pub engine_errors: Vec<NamedBucketConfig>,
    pub evm_reverts: Vec<NamedBucketConfig>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq)]
pub struct Bucket {
    value: u64,
    #[serde(rename = "BaseSize")]
    base_size: u64,
    #[serde(rename = "LeakRate")]
    leak_rate: u64,
    #[serde(rename = "OverflowSize")]
    overflow_size: u64,
    #[serde(rename = "Retention")]
    retention: u64,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum BucketKind {
    /// Near gas unit is NEAR gas.
    NearGas(Bucket),
    /// ETH gas unit is ETH gas.
    EthGas(Bucket),
    /// Relayer error bucket values are per error.
    RelayerErrors(HashMap<String, Bucket>),
    /// Engine error bucket values are per error.
    EngineErrors(HashMap<String, Bucket>),
    /// Revert value is a single revert.
    Reverts(Bucket),
    /// Free gas value is a single transaction.
    FreeGas(Bucket),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Buckets(Vec<BucketKind>);

impl Buckets {
    pub fn new(config: BucketsConfig) -> Self {
        let buckets: Vec<BucketKind> = {
            let capacity = 6
                + config.relayer_errors.len()
                + config.engine_errors.len()
                + config.evm_reverts.len();
            Vec::with_capacity(capacity)
        };
        Buckets(buckets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thing() {
        let mut map = HashMap::new();
        let bucket = BucketConfig {
            base_size: 10,
            leak_rate: 1,
            overflow_size: 1,
            retention: 1000,
        };
        let named_bucket = NamedBucketConfig {
            name: "test".to_string(),
            base_size: 0,
            leak_rate: 0,
            overflow_size: 0,
            retention: 0,
        };
        map.insert("EXAMPLE_ERROR".to_string(), bucket);
        let buckets_config = BucketsConfig {
            near_gas: bucket,
            eth_gas: bucket,
            default_relayer_error: BucketConfig {
                base_size: 0,
                leak_rate: 0,
                overflow_size: 0,
                retention: 0,
            },
            default_engine_error: BucketConfig {
                base_size: 0,
                leak_rate: 0,
                overflow_size: 0,
                retention: 0,
            },
            default_evm_revert: BucketConfig {
                base_size: 0,
                leak_rate: 0,
                overflow_size: 0,
                retention: 0,
            },
            relayer_errors: vec![named_bucket.clone(), named_bucket.clone()],
            engine_errors: vec![named_bucket.clone(), named_bucket.clone()],
            evm_reverts: vec![named_bucket.clone(), named_bucket],
        };

        let toml = toml::to_string_pretty(&buckets_config).unwrap();
        println!("{}", toml);

        // let json = serde_json::to_string_pretty(&buckets).unwrap();
        // println!("{}", json);
    }
}
*/
