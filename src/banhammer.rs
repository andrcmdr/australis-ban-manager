use crate::buckets::{
    BucketConfig, BucketErrorKind, BucketIdentity, BucketName, BucketNameValue,
    BucketPriorityQueue, LeakyBucket,
};
use crate::de::{RelayerMessage, TransactionError};
use serde::{
    de::{self, Error, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{
    fmt::{self},
    time::{Duration, Instant},
};

const NEAR_GAS_COUNTER: u64 = 202651902028573;

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    struct DurationVisitor;

    impl<'de> Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("transaction as hex string")
        }

        fn visit_u8<E>(self, duration: u8) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_u64(duration as u64)
        }

        fn visit_u16<E>(self, duration: u16) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_u64(duration as u64)
        }

        fn visit_u64<E>(self, duration: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Duration::from_secs(duration))
        }

        fn visit_i8<E>(self, duration: i8) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_u64(duration as u64)
        }

        fn visit_i16<E>(self, duration: i16) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_u64(duration as u64)
        }

        fn visit_i32<E>(self, duration: i32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_u64(duration as u64)
        }

        fn visit_i64<E>(self, duration: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_u64(duration as u64)
        }
    }

    deserializer.deserialize_u64(DurationVisitor)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_duration")]
    pub timeframe: Duration,
    pub incorrect_nonce_threshold: u64,
    pub max_gas_threshold: u64,
    pub revert_threshold: u64,
    pub excessive_gas_threshold: u64,
    pub token_multiplier: u64,
    pub leaky_buckets: BucketConfig,
}

pub struct Banhammer {
    next_check: Duration,
    config: Config,
    leaky_buckets: LeakyBucket,
    bucket_pq: BucketPriorityQueue,
}

impl Banhammer {
    pub fn new(config: Config) -> Self {
        Self {
            next_check: config.timeframe,
            config,
            leaky_buckets: LeakyBucket::new(),
            bucket_pq: BucketPriorityQueue::new(),
        }
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

        let bucket_excessive_gas = BucketName::new(
            bucket_identity.clone(),
            bucket_value.clone(),
            BucketErrorKind::UsedExcessiveGas,
        );
        let fill_result = self.leaky_buckets.get_fill(&bucket_excessive_gas, near_gas);
        // Check overflow
        if fill_result >= near_gas_threshold {
            ban_events.push(bucket_excessive_gas.clone());
            self.leaky_buckets
                .fill(&bucket_excessive_gas, self.config.leaky_buckets.base_size)
        } else {
            self.leaky_buckets
                .leaky(&bucket_excessive_gas, &self.config.leaky_buckets);
            self.leaky_buckets.fill(&bucket_excessive_gas, fill_result)
        }
        self.bucket_pq.push(bucket_excessive_gas);

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

                let bucket_incorrect_nonce = BucketName::new(
                    bucket_identity,
                    bucket_value,
                    BucketErrorKind::IncorrectNonce,
                );
                let fill_result = self.leaky_buckets.get_fill(&bucket_incorrect_nonce, 1);

                // Check overflow
                if fill_result >= threshold {
                    ban_events.push(bucket_incorrect_nonce.clone());
                    self.leaky_buckets
                        .fill(&bucket_incorrect_nonce, self.config.leaky_buckets.base_size)
                } else {
                    self.leaky_buckets
                        .leaky(&bucket_incorrect_nonce, &self.config.leaky_buckets);
                    self.leaky_buckets
                        .fill(&bucket_incorrect_nonce, fill_result)
                }
                self.bucket_pq.push(bucket_incorrect_nonce);
            }
            TransactionError::MaxGas => {
                let threshold = {
                    if token_exist {
                        self.config.max_gas_threshold * self.config.token_multiplier
                    } else {
                        self.config.max_gas_threshold
                    }
                } as u64;
                let bucket_max_gas =
                    BucketName::new(bucket_identity, bucket_value, BucketErrorKind::MaxGas);
                let fill_result = self.leaky_buckets.get_fill(&bucket_max_gas, 1);

                if fill_result >= threshold {
                    ban_events.push(bucket_max_gas.clone());
                    self.leaky_buckets
                        .fill(&bucket_max_gas, self.config.leaky_buckets.base_size)
                } else {
                    self.leaky_buckets
                        .leaky(&bucket_max_gas, &self.config.leaky_buckets);
                    self.leaky_buckets.fill(&bucket_max_gas, fill_result)
                }
                self.bucket_pq.push(bucket_max_gas);
            }
            TransactionError::Revert(_) => {
                let threshold = {
                    if token_exist {
                        self.config.revert_threshold * self.config.token_multiplier
                    } else {
                        self.config.revert_threshold
                    }
                } as u64;
                let bucket_reverts =
                    BucketName::new(bucket_identity, bucket_value, BucketErrorKind::Reverts);
                let fill_result = self.leaky_buckets.get_fill(&bucket_reverts, 1);
                if fill_result >= threshold {
                    self.leaky_buckets
                        .fill(&bucket_reverts, self.config.leaky_buckets.base_size)
                } else {
                    self.leaky_buckets
                        .leaky(&bucket_reverts, &self.config.leaky_buckets);
                    self.leaky_buckets.fill(&bucket_reverts, fill_result)
                }
                self.bucket_pq.push(bucket_reverts);
            }
            TransactionError::Relayer(_) => (),
        }
        ban_events
    }

    /// Tick for retention time for leaky bucket
    pub fn tick(&mut self, time: Instant) {
        if time.elapsed() > self.next_check {
            // Retention time in seconds
            let buckets_to_remove = self.bucket_pq.retention_free(60);
            for bucket in buckets_to_remove {
                self.leaky_buckets.remove(&bucket);
            }
            self.next_check += self.config.timeframe;
        }
    }

    pub fn read_input(&mut self, input: &RelayerMessage) -> Vec<BucketName> {
        let mut ban_events = vec![];
        let maybe_error = input.error.as_ref();

        //let token_exist = user.token.clone().is_some();
        let token_exist = input.token.is_some();
        let mut events = self.process_bucket(
            BucketIdentity::IP,
            BucketNameValue::IP(input.client),
            maybe_error,
            NEAR_GAS_COUNTER,
            token_exist,
        );
        ban_events.append(&mut events);

        let mut events = self.process_bucket(
            BucketIdentity::Address,
            BucketNameValue::Address(input.params.from),
            maybe_error,
            NEAR_GAS_COUNTER,
            token_exist,
        );
        ban_events.append(&mut events);

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
