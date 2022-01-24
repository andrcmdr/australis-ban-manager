use ethereum_types::Address;
use http::{StatusCode, Uri};
use serde::{
    de::{self, Error, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{
    fmt::{self, Formatter},
    net::IpAddr,
    time::Duration,
};

#[derive(Debug, PartialEq)]
pub struct Timestamp(Duration);

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TimestampVisitor;

        impl<'de> Visitor<'de> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("timestamp as u64")
            }

            fn visit_u64<E>(self, timestamp: u64) -> Result<Self::Value, E> {
                let duration = Duration::from_millis(timestamp);
                Ok(Timestamp(duration))
            }
        }

        deserializer.deserialize_u64(TimestampVisitor)
    }
}

#[derive(PartialEq)]
pub struct Transaction(Vec<u8>);

impl<'de> Deserialize<'de> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TransactionVisitor;

        impl<'de> Visitor<'de> for TransactionVisitor {
            type Value = Transaction;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("transaction as hex string")
            }

            fn visit_str<E>(self, tx_hex: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let tx_hex = tx_hex.trim_start_matches("0x");
                let hex = hex::decode(tx_hex).map_err(de::Error::custom)?;
                Ok(Transaction(hex))
            }
        }

        deserializer.deserialize_str(TransactionVisitor)
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let field = {
            let mut field = "0x".to_string();
            let field_hex = hex::encode(&self.0);
            field.push_str(&field_hex);
            field
        };
        f.debug_tuple("Transaction").field(&field).finish()
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let field = {
            let mut field = "0x".to_string();
            let field_hex = hex::encode(&self.0);
            field.push_str(&field_hex);
            field
        };
        f.write_str(&field)
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Params {
    pub from: Address,
    pub to: Address,
    pub eth_gas: u32,
    pub eth_nonce: u32,
    pub eth_value: String,
    pub tx: Transaction,
}

#[derive(Debug, PartialEq)]
pub struct Status(StatusCode);

impl<'de> Deserialize<'de> for Status {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StatusVisitor;

        impl<'de> Visitor<'de> for StatusVisitor {
            type Value = Status;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("expected status as u16")
            }

            fn visit_i8<E>(self, status: i8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }

            fn visit_i16<E>(self, status: i16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }

            fn visit_i32<E>(self, status: i32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }

            fn visit_i64<E>(self, status: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }

            fn visit_u8<E>(self, status: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }

            fn visit_u16<E>(self, status: u16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let status = StatusCode::from_u16(status).map_err(de::Error::custom)?;
                Ok(Status(status))
            }

            fn visit_u32<E>(self, status: u32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }

            fn visit_u64<E>(self, status: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                self.visit_u16(status as u16)
            }
        }

        deserializer.deserialize_any(StatusVisitor)
    }
}

#[derive(Debug, PartialEq)]
pub struct Url(Uri);

impl<'de> Deserialize<'de> for Url {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UrlVisitor;

        impl<'de> Visitor<'de> for UrlVisitor {
            type Value = Url;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("transaction as hex string")
            }

            fn visit_str<E>(self, url: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let url = url.parse::<Uri>().map_err(de::Error::custom)?;
                Ok(Url(url))
            }
        }

        deserializer.deserialize_str(UrlVisitor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionError {
    ErrIncorrectNonce,
    MaxGas,
    Revert(String),
    Relayer(String),
}

impl<'de> Deserialize<'de> for TransactionError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TransactionErrorVisitor;

        impl<'de> Visitor<'de> for TransactionErrorVisitor {
            type Value = TransactionError;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("transaction as hex string")
            }

            fn visit_str<E>(self, err: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if err.contains("<https://github.com/aurora-is-near/aurora-relayer/issues>") {
                    return Ok(TransactionError::Relayer(err.to_string()));
                }

                Ok(match err {
                    "ERR_INCORRECT_NONCE" => TransactionError::ErrIncorrectNonce,
                    "Exceeded the maximum amount of gas allowed to burn per contract." => {
                        TransactionError::MaxGas
                    }
                    _ => TransactionError::Revert(err.to_string()),
                })
            }
        }

        deserializer.deserialize_str(TransactionErrorVisitor)
    }
}

fn deserialize_error<'de, D>(deserializer: D) -> Result<Option<TransactionError>, D::Error>
where
    D: Deserializer<'de>,
{
    struct TransactionErrorVisitor;

    impl<'de> Visitor<'de> for TransactionErrorVisitor {
        type Value = Option<TransactionError>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("transaction as hex string")
        }

        fn visit_str<E>(self, err: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if err.is_empty() {
                return Ok(None);
            }

            if err.contains("<https://github.com/aurora-is-near/aurora-relayer/issues>") {
                return Ok(Some(TransactionError::Relayer(err.to_string())));
            }

            Ok(Some(match err {
                "ERR_INCORRECT_NONCE" => TransactionError::ErrIncorrectNonce,
                "Exceeded the maximum amount of gas allowed to burn per contract." => {
                    TransactionError::MaxGas
                }
                _ => TransactionError::Revert(err.to_string()),
            }))
        }
    }

    deserializer.deserialize_str(TransactionErrorVisitor)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct Token(String);

fn deserialize_token<'de, D>(deserializer: D) -> Result<Option<Token>, D::Error>
where
    D: Deserializer<'de>,
{
    struct TokenVisitor;

    impl<'de> Visitor<'de> for TokenVisitor {
        type Value = Option<Token>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("transaction as hex string")
        }

        fn visit_str<E>(self, token: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if token.is_empty() {
                Ok(None)
            } else if token.len() != 44 && token.len() != 43 {
                Err(de::Error::custom(
                    "token length needs to be 44 characters long",
                ))
            } else {
                Ok(Some(Token(token.to_string())))
            }
        }
    }

    deserializer.deserialize_str(TokenVisitor)
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct RelayerInput {
    pub host: Url,
    pub timestamp: Timestamp,
    pub status: Status,
    pub client: IpAddr,
    pub response_time: f32,
    // pub has_error: bool, // skipped
    // pub has_token: bool, // skipped
    #[serde(deserialize_with = "deserialize_error")]
    pub error: Option<TransactionError>,
    #[serde(deserialize_with = "deserialize_token")]
    pub token: Option<Token>,
    pub method: String,
    pub params: Params,
}

#[cfg(test)]
mod tests {
    use super::{Params, RelayerInput, Status, Timestamp, Transaction, Url};
    use ethereum_types::Address;
    use http::{StatusCode, Uri};
    use std::time::Duration;

    #[test]
    fn test_deserialize() {
        let input = r#"
{
  "host": "westcoast002.relayers.aurora.dev",
  "timestamp": 1642679283605128700,
  "status": 200,
  "client": "52.180.67.69",
  "response_time": 6.146,
  "hasError": false,
  "hasToken": false,
  "error": "",
  "token": "",
  "method": "eth_sendrawtransaction",
  "params": {
    "from": "0xd85dea6093118b54318e10c6654a2b2e1a6b40c6",
    "to": "0x73cd7b6b17836e4ed282e5b9e6e01fbfb966b442",
    "eth_gas": 400000,
    "eth_nonce": 23118,
    "eth_value": "0",
    "tx": "0xf9020c825a4e8083061a809473cd7b6b17836e4ed282e5b9e6e01fbfb966b44280b901a45b42abc9000000000000000000000000000000000000000000000000215c9f93ceee2b8d000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000004c42c30ac6cc15fac9bd938618bcaa1a1fae8501d180000042a99168543b200fcc4bdd27c33ec7daa6fcfd8532ddb524bf4038096120010aa6550b34b69531c9cc9bdeed33cd01541e1eed10f90519d2c06fe3feb1242a991685479899ea824dbfa94348467f64d5a457f75f8bc40495d33c65aba18a6550b35bc3364c2f4c612000000000000000000000000000000000000000000000000000000000000000424f6c59747e4aceb3dba365df77d68c2a3aa4fb126fc0000215c9f93ceee2b8d747f3861eb4b98e61682da7687aa140e373b698e26f200857276181c5c5d8f87d263fc38d75928c965ed3507ceb2ce60fce4001626f015c9f93cecf771290b6484b123875f0f36b966d0b6ca14b31121bd9676ad2ea5276181cd92ff99f1da46849c8a82c8a0e55e5767cc04c5fa7b26759c236c90be4125056aff5faeb00a0e3fc64a949e66a00b9f4c971ab138d624ae8691d40e6ad1fff3983ac6c6e6ad2dff7ac8d186a9b7"
  }
}
"#;
        let header: RelayerInput = serde_json::from_str(input).unwrap();
        let tx_bytes = hex::decode("f9020c825a4e8083061a809473cd7b6b17836e4ed282e5b9e6e01fbfb966b44280b901a45b42abc9000000000000000000000000000000000000000000000000215c9f93ceee2b8d000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000004c42c30ac6cc15fac9bd938618bcaa1a1fae8501d180000042a99168543b200fcc4bdd27c33ec7daa6fcfd8532ddb524bf4038096120010aa6550b34b69531c9cc9bdeed33cd01541e1eed10f90519d2c06fe3feb1242a991685479899ea824dbfa94348467f64d5a457f75f8bc40495d33c65aba18a6550b35bc3364c2f4c612000000000000000000000000000000000000000000000000000000000000000424f6c59747e4aceb3dba365df77d68c2a3aa4fb126fc0000215c9f93ceee2b8d747f3861eb4b98e61682da7687aa140e373b698e26f200857276181c5c5d8f87d263fc38d75928c965ed3507ceb2ce60fce4001626f015c9f93cecf771290b6484b123875f0f36b966d0b6ca14b31121bd9676ad2ea5276181cd92ff99f1da46849c8a82c8a0e55e5767cc04c5fa7b26759c236c90be4125056aff5faeb00a0e3fc64a949e66a00b9f4c971ab138d624ae8691d40e6ad1fff3983ac6c6e6ad2dff7ac8d186a9b7").unwrap();
        let from_address_bytes = hex::decode("d85dea6093118b54318e10c6654a2b2e1a6b40c6").unwrap();
        let to_address_bytes = hex::decode("73cd7b6b17836e4ed282e5b9e6e01fbfb966b442").unwrap();
        let expected = RelayerInput {
            host: Url("westcoast002.relayers.aurora.dev".parse::<Uri>().unwrap()),
            timestamp: Timestamp(Duration::from_millis(1642679283605128700)),
            status: Status(StatusCode::OK),
            client: "52.180.67.69".parse().unwrap(),
            response_time: 6.146,
            error: None,
            token: None,
            method: "eth_sendrawtransaction".to_string(),
            params: Params {
                from: Address::from_slice(&from_address_bytes),
                to: Address::from_slice(&to_address_bytes),
                eth_gas: 400000,
                eth_nonce: 23118,
                eth_value: "0".to_string(),
                tx: Transaction(tx_bytes),
            },
        };
        assert_eq!(header, expected);
    }
}
