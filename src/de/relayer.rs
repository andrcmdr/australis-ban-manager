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

#[derive(PartialEq)]
// TODO: Deserialize into actual result.
pub struct EvmResult(Vec<u8>);

impl fmt::Debug for EvmResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let field = {
            let mut field = "0x".to_string();
            let field_hex = hex::encode(&self.0);
            field.push_str(&field_hex);
            field
        };
        f.debug_tuple("EvmResult").field(&field).finish()
    }
}

impl fmt::Display for EvmResult {
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

#[derive(Debug, PartialEq)]
pub enum SignatureVersion {
    Legacy,
    Eip2930,
    Eip1559,
}

fn deserialize_signature_version<'de, D>(deserializer: D) -> Result<SignatureVersion, D::Error>
    where
        D: Deserializer<'de>,
{
    struct SignatureVersionVisitor;

    impl<'de> Visitor<'de> for SignatureVersionVisitor {
        type Value = SignatureVersion;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("signature version as str")
        }

        fn visit_str<E>(self, signature_version: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
        {
            match signature_version {
                "Berlin" => Ok(SignatureVersion::Eip2930),
                "London" => Ok(SignatureVersion::Eip1559),
                _ => Err(de::Error::custom(format!("Unknown signature version: {signature_version}"))),
            }
        }
    }

    deserializer.deserialize_str(SignatureVersionVisitor)
}

fn deserialize_evm_result<'de, D>(deserializer: D) -> Result<Option<EvmResult>, D::Error>
where
    D: Deserializer<'de>,
{
    struct EvmResultVisitor;

    impl<'de> Visitor<'de> for EvmResultVisitor {
        type Value = Option<EvmResult>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("expected no hex or hex")
        }

        fn visit_str<E>(self, evm_result: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if evm_result.is_empty() {
                return Ok(None);
            }

            let result_hex = evm_result.trim_start_matches("0x");
            let hex = hex::decode(result_hex).map_err(de::Error::custom)?;
            Ok(Some(EvmResult(hex)))
        }
    }

    deserializer.deserialize_str(EvmResultVisitor)
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Params {
    pub from: Address,
    #[serde(rename = "sigversion")]
    #[serde(deserialize_with = "deserialize_signature_version")]
    pub signature_version: SignatureVersion,
    #[serde(rename = "aurora-result")]
    #[serde(deserialize_with = "deserialize_evm_result")]
    pub evm_result: Option<EvmResult>,
    #[serde(rename = "near-gas-burned")]
    pub near_gas_burned: u64,
    #[serde(rename = "near-tx-id")]
    pub near_tx: String,
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
pub struct RelayerMessage {
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
    #[serde(rename = "Method")]
    pub method: String,
    pub params: Params,
}

#[cfg(test)]
mod tests {
    use super::{Params, RelayerMessage, Status, Timestamp, Transaction, Url, SignatureVersion, TransactionError, EvmResult};
    use ethereum_types::Address;
    use http::{StatusCode, Uri};
    use std::time::Duration;

    #[test]
    fn test_deserialize() {
        let input = r#"
{
  "host": "eastcoast002.relayers.aurora.dev",
  "timestamp": 1643055002125066200,
  "status": 200,
  "client": "2001:41d0:401:3000::6131",
  "response_time": 9.016,
  "hasError": true,
  "hasToken": false,
  "error": "ERR_INCORRECT_NONCE",
  "token": "",
  "Method": "eth_sendrawtransaction",
  "params": {
    "from": "0x432963a481e1aa7f09e9ea878d4a596eee6eb63b",
    "sigversion": "London",
    "aurora-result": "",
    "near-gas-burned": 2428826553270,
    "near-tx-id": "EhSE72mE1Bj2czjRFPbHpF6ZbN7WYpvW3RiJCqRQEpWi",
    "to": "0xa3a1ef5ae6561572023363862e238afa84c72ef5",
    "eth_gas": 196566,
    "eth_nonce": 1109,
    "eth_value": "0",
    "tx": "0xf9018c820455808302ffd694a3a1ef5ae6561572023363862e238afa84c72ef580b9012438ed173900000000000000000000000000000000000000000000000000000000064794160000000000000000000000000000000000000000000830038c7a58c18000000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000432963a481e1aa7f09e9ea878d4a596eee6eb63b0000000000000000000000000000000000000000000000000000000061ef079a00000000000000000000000000000000000000000000000000000000000000030000000000000000000000004988a896b1227218e4a686fde5eabdcabd91571f000000000000000000000000fa94348467f64d5a457f75f8bc40495d33c65abb000000000000000000000000c42c30ac6cc15fac9bd938618bcaa1a1fae8501d849c8a82c7a0c2e6e27f406606698c5363b043e8d736e7a89f2fde612d473f41564262138aa6a03e00b96b99a2033ab8ef5f356b16514542eaa0de2e8356f0045a712ca3ec3501"
  }
}
"#;
        let header: RelayerMessage = serde_json::from_str(input).unwrap();
        let tx_bytes = hex::decode("f9018c820455808302ffd694a3a1ef5ae6561572023363862e238afa84c72ef580b9012438ed173900000000000000000000000000000000000000000000000000000000064794160000000000000000000000000000000000000000000830038c7a58c18000000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000432963a481e1aa7f09e9ea878d4a596eee6eb63b0000000000000000000000000000000000000000000000000000000061ef079a00000000000000000000000000000000000000000000000000000000000000030000000000000000000000004988a896b1227218e4a686fde5eabdcabd91571f000000000000000000000000fa94348467f64d5a457f75f8bc40495d33c65abb000000000000000000000000c42c30ac6cc15fac9bd938618bcaa1a1fae8501d849c8a82c7a0c2e6e27f406606698c5363b043e8d736e7a89f2fde612d473f41564262138aa6a03e00b96b99a2033ab8ef5f356b16514542eaa0de2e8356f0045a712ca3ec3501").unwrap();
        let from_address_bytes = hex::decode("432963a481e1aa7f09e9ea878d4a596eee6eb63b").unwrap();
        let to_address_bytes = hex::decode("a3a1ef5ae6561572023363862e238afa84c72ef5").unwrap();
        let expected = RelayerMessage {
            host: Url("eastcoast002.relayers.aurora.dev".parse::<Uri>().unwrap()),
            timestamp: Timestamp(Duration::from_millis(1643055002125066200)),
            status: Status(StatusCode::OK),
            client: "2001:41d0:401:3000::6131".parse().unwrap(),
            response_time: 9.016,
            error: Some(TransactionError::ErrIncorrectNonce),
            token: None,
            method: "eth_sendrawtransaction".to_string(),
            params: Params {
                from: Address::from_slice(&from_address_bytes),
                signature_version: SignatureVersion::Eip1559,
                evm_result: None,
                near_gas_burned: 2428826553270,
                near_tx: "EhSE72mE1Bj2czjRFPbHpF6ZbN7WYpvW3RiJCqRQEpWi".to_string(),
                to: Address::from_slice(&to_address_bytes),
                eth_gas: 196566,
                eth_nonce: 1109,
                eth_value: "0".to_string(),
                tx: Transaction(tx_bytes),
            },
        };
        assert_eq!(header, expected);
    }
}
