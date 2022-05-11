use serde::{
    de::{self, Error, Visitor},
    Deserializer,
};

use std::{
    fmt::{self},
    time::Duration,
};

mod relayer;

pub use relayer::{Params, RelayerMessage, Timestamp, Token, Transaction, TransactionError, Url};

pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
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
