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

const RELAYER_ERR_PATTERN: &str = "httpsgithub.comaurora-is-nearaurora-relayerissues";

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
                _ => Err(de::Error::custom(format!(
                    "Unknown signature version: {signature_version}"
                ))),
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

fn deserialize_to<'de, D>(deserializer: D) -> Result<Option<Address>, D::Error>
where
    D: Deserializer<'de>,
{
    struct AddressVisitor;

    impl<'de> Visitor<'de> for AddressVisitor {
        type Value = Option<Address>;

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
            Ok(Some(Address::from_slice(&hex)))
        }
    }

    deserializer.deserialize_str(AddressVisitor)
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Params {
    pub from: Address,
    #[serde(rename = "sigver")]
    #[serde(deserialize_with = "deserialize_signature_version")]
    pub signature_version: SignatureVersion,
    #[serde(deserialize_with = "deserialize_evm_result")]
    #[serde(rename = "aurora_result")]
    pub evm_result: Option<EvmResult>,
    pub near_gas: u128,
    // pub near_txid: String,
    #[serde(deserialize_with = "deserialize_to")]
    pub to: Option<Address>,
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
    InvalidECDSA,
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
                if err.contains(RELAYER_ERR_PATTERN) {
                    return Ok(TransactionError::Relayer(err.to_string()));
                }

                Ok(match err {
                    "ERR_INCORRECT_NONCE" => TransactionError::ErrIncorrectNonce,
                    "Exceeded the maximum amount of gas allowed to burn per contract." => {
                        TransactionError::MaxGas
                    }
                    "ERR_INVALID_ECDSA_SIGNATURE" => TransactionError::InvalidECDSA,
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

            if err.contains("httpsgithub.comaurora-is-nearaurora-relayerissues") {
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
    pub method: String,
    pub params: Params,
}

#[cfg(test)]
mod tests {
    use super::{
        EvmResult, Params, RelayerMessage, SignatureVersion, Status, Timestamp, Transaction, Url,
    };
    use ethereum_types::Address;
    use http::{StatusCode, Uri};
    use std::time::Duration;

    #[test]
    fn test_deserialize() {
        let input = r#"
{
  "host": "westcoast004.relayers.aurora.dev",
  "timestamp": 1644082737464356000,
  "status": 200,
  "client": "197.251.253.48",
  "response_time": 8.747,
  "hasError": false,
  "hasToken": false,
  "error": "",
  "token": "",
  "method": "eth_sendrawtransaction",
  "params": {
    "from": "0xb845796ae42f5061c65717e3e29ff33495b1652d",
    "sigver": "London",
    "aurora_result": "0x6fa5f6cd64bd7510a7c67e68f0bbe87a580d22a175b342d50eb9698800b9992a",
    "near_gas": 0,
    "near_txid": "",
    "to": "",
    "eth_gas": 6721975,
    "eth_nonce": 10,
    "eth_value": "0",
    "tx": "0xf904e90a80836691b78080b9049760c060405234801561001057600080fd5b506040516104573803806104578339818101604052604081101561003357600080fd5b5080516020909101516001600160a01b03821661004f57600080fd5b6001600160601b0319606083901b166080526001600160a01b03811661007457600080fd5b606081811b6001600160601b03191660a052608051901c91506001600160a01b031661038d6100ca6000398060e2528061019f52806103355250806093528061016e528061023d52806102d1525061038d6000f3fe608060405234801561001057600080fd5b50600436106100415760003560e01c80634cf088d914610046578063a694fc3a1461006a578063a6c41fec14610089575b600080fd5b61004e610091565b604080516001600160a01b039092168252519081900360200190f35b6100876004803603602081101561008057600080fd5b50356100b5565b005b61004e610333565b7f000000000000000000000000000000000000000000000000000000000000000081565b604080516323b872dd60e01b81523360048201523060248201526044810183905290516001600160a01b037f000000000000000000000000000000000000000000000000000000000000000016916323b872dd9160648083019260209291908290030181600087803b15801561012a57600080fd5b505af115801561013e573d6000803e3d6000fd5b505050506040513d602081101561015457600080fd5b50506040805163095ea7b360e01b81526001600160a01b037f0000000000000000000000000000000000000000000000000000000000000000811660048301526024820184905291517f00000000000000000000000000000000000000000000000000000000000000009092169163095ea7b3916044808201926020929091908290030181600087803b1580156101ea57600080fd5b505af11580156101fe573d6000803e3d6000fd5b505050506040513d602081101561021457600080fd5b505060408051637acb775760e01b81526004810183905233602482015290516001600160a01b037f00000000000000000000000000000000000000000000000000000000000000001691637acb77579160448083019260209291908290030181600087803b15801561028557600080fd5b505af1158015610299573d6000803e3d6000fd5b505050506040513d60208110156102af57600080fd5b505060408051630f41a04d60e11b815233600482015290516001600160a01b037f00000000000000000000000000000000000000000000000000000000000000001691631e83409a91602480830192600092919082900301818387803b15801561031857600080fd5b505af115801561032c573d6000803e3d6000fd5b5050505050565b7f00000000000000000000000000000000000000000000000000000000000000008156fea26469706673582212205b01c55f5a17ed9a5ecb2fbeb6e08982b207e457e8542c13b952616ddebaee5664736f6c634300070500330000000000000000000000009469380b2fdc401a83735353745fbee26a6ace020000000000000000000000003d352c41273dd54844df4f5e92256283d46229bf849c8a82c8a0d7a42931d9faff43abb5422d5b17246e6efddb69ac51ffb418ba796d81d53f3ba029e9f7bd3bd4a70b1c7519f108b8de8aa9503b5b41caa51e17bfadfe53896e76"
  }
}
        "#;
        let header: RelayerMessage = serde_json::from_str(input).unwrap();
        let tx_bytes = hex::decode("f904e90a80836691b78080b9049760c060405234801561001057600080fd5b506040516104573803806104578339818101604052604081101561003357600080fd5b5080516020909101516001600160a01b03821661004f57600080fd5b6001600160601b0319606083901b166080526001600160a01b03811661007457600080fd5b606081811b6001600160601b03191660a052608051901c91506001600160a01b031661038d6100ca6000398060e2528061019f52806103355250806093528061016e528061023d52806102d1525061038d6000f3fe608060405234801561001057600080fd5b50600436106100415760003560e01c80634cf088d914610046578063a694fc3a1461006a578063a6c41fec14610089575b600080fd5b61004e610091565b604080516001600160a01b039092168252519081900360200190f35b6100876004803603602081101561008057600080fd5b50356100b5565b005b61004e610333565b7f000000000000000000000000000000000000000000000000000000000000000081565b604080516323b872dd60e01b81523360048201523060248201526044810183905290516001600160a01b037f000000000000000000000000000000000000000000000000000000000000000016916323b872dd9160648083019260209291908290030181600087803b15801561012a57600080fd5b505af115801561013e573d6000803e3d6000fd5b505050506040513d602081101561015457600080fd5b50506040805163095ea7b360e01b81526001600160a01b037f0000000000000000000000000000000000000000000000000000000000000000811660048301526024820184905291517f00000000000000000000000000000000000000000000000000000000000000009092169163095ea7b3916044808201926020929091908290030181600087803b1580156101ea57600080fd5b505af11580156101fe573d6000803e3d6000fd5b505050506040513d602081101561021457600080fd5b505060408051637acb775760e01b81526004810183905233602482015290516001600160a01b037f00000000000000000000000000000000000000000000000000000000000000001691637acb77579160448083019260209291908290030181600087803b15801561028557600080fd5b505af1158015610299573d6000803e3d6000fd5b505050506040513d60208110156102af57600080fd5b505060408051630f41a04d60e11b815233600482015290516001600160a01b037f00000000000000000000000000000000000000000000000000000000000000001691631e83409a91602480830192600092919082900301818387803b15801561031857600080fd5b505af115801561032c573d6000803e3d6000fd5b5050505050565b7f00000000000000000000000000000000000000000000000000000000000000008156fea26469706673582212205b01c55f5a17ed9a5ecb2fbeb6e08982b207e457e8542c13b952616ddebaee5664736f6c634300070500330000000000000000000000009469380b2fdc401a83735353745fbee26a6ace020000000000000000000000003d352c41273dd54844df4f5e92256283d46229bf849c8a82c8a0d7a42931d9faff43abb5422d5b17246e6efddb69ac51ffb418ba796d81d53f3ba029e9f7bd3bd4a70b1c7519f108b8de8aa9503b5b41caa51e17bfadfe53896e76").unwrap();
        let from_address_bytes = hex::decode("b845796ae42f5061c65717e3e29ff33495b1652d").unwrap();
        let to_address_bytes = hex::decode("a3a1ef5ae6561572023363862e238afa84c72ef5").unwrap();
        let evm_result_bytes =
            hex::decode("6fa5f6cd64bd7510a7c67e68f0bbe87a580d22a175b342d50eb9698800b9992a")
                .unwrap();
        let expected = RelayerMessage {
            host: Url("westcoast004.relayers.aurora.dev".parse::<Uri>().unwrap()),
            timestamp: Timestamp(Duration::from_millis(1644082737464356000)),
            status: Status(StatusCode::OK),
            client: "197.251.253.48".parse().unwrap(),
            response_time: 8.747,
            error: None,
            token: None,
            method: "eth_sendrawtransaction".to_string(),
            params: Params {
                from: Address::from_slice(&from_address_bytes),
                signature_version: SignatureVersion::Eip1559,
                evm_result: Some(EvmResult(evm_result_bytes)),
                near_gas: 0,
                // near_txid: "EhSE72mE1Bj2czjRFPbHpF6ZbN7WYpvW3RiJCqRQEpWi".to_string(),
                to: None,
                eth_gas: 6721975,
                eth_nonce: 10,
                eth_value: "0".to_string(),
                tx: Transaction(tx_bytes),
            },
        };
        assert_eq!(header, expected);
    }
}
