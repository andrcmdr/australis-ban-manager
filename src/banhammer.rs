//! # Banhummer
//! Contains basic logic for detecting ban event related by
//! Leaky Buckets algorithm.
//!
//! Bucket can:
//! - fill
//! - leak
//! - overflow
//! - remove
//! Tge "fill" is always >= 0
//!
//! Bucket name contains fields: Identiti + IdentityVAlie + ErrorKind
use crate::buckets::{
    BucketConfig, BucketErrorKind, BucketIdentity, BucketName, BucketNameValue,
    BucketPriorityQueue, LeakyBucket,
};
use crate::de::{RelayerMessage, TransactionError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

const NEAR_GAS_COUNTER: u64 = 202651902028573;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LeakyBucketConfig {
    pub identity: BucketIdentity,
    pub error_kind: BucketErrorKind,
    pub bucket: BucketConfig,
}

/// Banhammer configs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub incorrect_nonce_threshold: u64,
    pub max_gas_threshold: u64,
    pub revert_threshold: u64,
    pub excessive_gas_threshold: u64,
    pub token_multiplier: u64,
    pub leaky_buckets: Vec<LeakyBucketConfig>,
}

pub struct RetentionKey {}

/// Basic Banhammer data struct
pub struct Banhammer {
    next_retention_check: HashMap<String, Duration>,
    config: Config,
    leaky_buckets: LeakyBucket,
    bucket_pq: HashMap<String, BucketPriorityQueue>,
}

impl Banhammer {
    pub fn new(config: Config) -> Self {
        Self {
            next_retention_check: HashMap::new(),
            config,
            leaky_buckets: LeakyBucket::default(),
            bucket_pq: HashMap::new(),
        }
    }

    /// Check bucket by threshold and process
    /// actions: fill, leak, overflow.
    /// Return: ban event
    fn check_and_change_bucket(
        &mut self,
        bucket_identity: &BucketIdentity,
        bucket_value: &BucketNameValue,
        bucket_error_kind: BucketErrorKind,
        threshold: u64,
        fill: u64,
    ) -> Option<BucketName> {
        let mut ban_event = None;
        let bucket_name = BucketName::new(
            bucket_identity.clone(),
            bucket_value.clone(),
            bucket_error_kind,
        );
        let fill_result = self.leaky_buckets.get_fill(&bucket_name, fill);
        // Check overflow
        if fill_result >= threshold {
            ban_event = Some(bucket_name.clone());
            // Set leaky bucket ti base size after overflow
            self.leaky_buckets
                .fill(&bucket_name, self.config.leaky_buckets.base_size)
        } else {
            // Check leaky status and leak if it needed
            self.leaky_buckets
                .leaky(&bucket_name, &self.config.leaky_buckets);
            // Fill bucket
            self.leaky_buckets.fill(&bucket_name, fill_result)
        }
        // Set priority queue for bucket with
        // last_update field as current time
        self.bucket_pq.insert(bucket_name);
        ban_event
    }

    /// Process bucket live cycle    
    fn process_bucket(
        &mut self,
        bucket_identity: BucketIdentity,
        bucket_value: BucketNameValue,
        maybe_error: Option<&TransactionError>,
        near_gas: u64,
        token_exist: bool,
    ) -> Vec<BucketName> {
        let mut ban_events = vec![];
        let near_gas_threshold = {
            if token_exist {
                self.config.excessive_gas_threshold
                    * 1_000_000_000_000
                    * self.config.token_multiplier
            } else {
                self.config.excessive_gas_threshold * 1_000_000_000_000
            }
        };

        if let Some(ban_event) = self.check_and_change_bucket(
            &bucket_identity,
            &bucket_value,
            BucketErrorKind::UsedExcessiveGas,
            near_gas_threshold,
            near_gas,
        ) {
            ban_events.push(ban_event);
        }

        // if it's no errors - just return
        if maybe_error.is_none() {
            return ban_events;
        }

        match maybe_error.unwrap() {
            TransactionError::ErrIncorrectNonce | TransactionError::InvalidECDSA => {
                let threshold = {
                    if token_exist {
                        self.config.incorrect_nonce_threshold * self.config.token_multiplier
                    } else {
                        self.config.incorrect_nonce_threshold
                    }
                } as u64;

                if let Some(ban_event) = self.check_and_change_bucket(
                    &bucket_identity,
                    &bucket_value,
                    BucketErrorKind::IncorrectNonce,
                    threshold,
                    1,
                ) {
                    ban_events.push(ban_event);
                }
            }
            TransactionError::MaxGas => {
                let threshold = {
                    if token_exist {
                        self.config.max_gas_threshold * self.config.token_multiplier
                    } else {
                        self.config.max_gas_threshold
                    }
                } as u64;

                if let Some(ban_event) = self.check_and_change_bucket(
                    &bucket_identity,
                    &bucket_value,
                    BucketErrorKind::MaxGas,
                    threshold,
                    1,
                ) {
                    ban_events.push(ban_event);
                }
            }
            TransactionError::Revert(_) => {
                let threshold = {
                    if token_exist {
                        self.config.revert_threshold * self.config.token_multiplier
                    } else {
                        self.config.revert_threshold
                    }
                } as u64;

                if let Some(ban_event) = self.check_and_change_bucket(
                    &bucket_identity,
                    &bucket_value,
                    BucketErrorKind::Reverts,
                    threshold,
                    1,
                ) {
                    ban_events.push(ban_event);
                }
            }
            TransactionError::Relayer(_) => (),
        }
        ban_events
    }

    /// Tick for retention time for leaky bucket
    pub fn tick(&mut self, time: Instant) {
        for next_retention_check in self.next_retention_check {
            if time.elapsed() > self.next_retention_check {
                // Get buckets fpr remove.
                // Retention time in seconds
                let buckets_to_remove = self.bucket_pq.retention_free(60);
                for bucket in buckets_to_remove {
                    tracing::info!("bucket removed: {bucket:?}");
                    self.leaky_buckets.remove(&bucket);
                }
                self.next_retention_check += self.config.leaky_buckets.retention;
            }
        }
    }

    /// Read relayer input, process leaky bucket and return ban events list
    pub fn read_input(&mut self, input: &RelayerMessage) -> Vec<BucketName> {
        let mut ban_events = vec![];
        let maybe_error = input.error.as_ref();

        // Check is token exist
        let token_exist = input.token.is_some();

        // Process leaky buckets for Client IPs
        let mut events = self.process_bucket(
            BucketIdentity::IP,
            BucketNameValue::IP(input.client),
            maybe_error,
            NEAR_GAS_COUNTER,
            token_exist,
        );
        ban_events.append(&mut events);

        // Process leaky buckets for Client Eth Addresses
        let mut events = self.process_bucket(
            BucketIdentity::Address,
            BucketNameValue::Address(input.params.from),
            maybe_error,
            NEAR_GAS_COUNTER,
            token_exist,
        );
        ban_events.append(&mut events);

        // Process leaky buckets for Client API tokens
        if let Some(token) = input.token.clone() {
            let mut events = self.process_bucket(
                BucketIdentity::Token,
                BucketNameValue::Token(token),
                maybe_error,
                NEAR_GAS_COUNTER,
                token_exist,
            );
            ban_events.append(&mut events);
        }

        ban_events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_excessive_gas() {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let config = Config {
            incorrect_nonce_threshold: 10,
            max_gas_threshold: 2,
            revert_threshold: 10,
            excessive_gas_threshold: 3,
            token_multiplier: 1,
            leaky_buckets: vec![LeakyBucketConfig {
                identity: BucketIdentity::IP,
                error_kind: BucketErrorKind::UsedExcessiveGas,
                bucket: BucketConfig {
                    base_size: 1,
                    leak_rate: 100000,
                    overflow_size: 10,
                    retention: Duration::from_secs(10),
                },
            }],
        };
        let mut bh = Banhammer::new(config.clone());
        let events = bh.process_bucket(
            BucketIdentity::IP,
            BucketNameValue::IP(ip),
            Some(&TransactionError::ErrIncorrectNonce),
            1_000_000_000_000,
            false,
        );
        assert!(events.is_empty());
        let bucket_name = BucketName::new(
            BucketIdentity::IP,
            BucketNameValue::IP(ip),
            BucketErrorKind::UsedExcessiveGas,
        );
        let res = bh.leaky_buckets.get_fill(&bucket_name, 0);
        assert_eq!(1_000_000_000_000, res);

        let events = bh.process_bucket(
            BucketIdentity::IP,
            BucketNameValue::IP(ip),
            Some(&TransactionError::ErrIncorrectNonce),
            1_000_000_000_000,
            false,
        );
        assert!(events.is_empty());
        let res = bh.leaky_buckets.get_fill(&bucket_name, 0);
        assert_eq!(2_000_000_000_000, res);

        let events = bh.process_bucket(
            BucketIdentity::IP,
            BucketNameValue::IP(ip),
            Some(&TransactionError::ErrIncorrectNonce),
            1_000_000_000_100,
            false,
        );
        assert_eq!(events.len(), 1);
        let res = bh.leaky_buckets.get_fill(&bucket_name, 0);
        assert_eq!(config.leaky_buckets.base_size, res);
    }
}
