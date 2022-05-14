use crate::buckets::{
    BucketErrorKind, BucketIdentity, BucketName, BucketNameValue, BucketValue, LeakyBucket,
};
use crate::de::{RelayerMessage, Token, TransactionError};
use ethereum_types::Address;
use serde::{
    de::{self, Error, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{
    collections::HashMap,
    fmt::{self},
    net::IpAddr,
    time::{Duration, Instant, SystemTime},
};

const NEAR_GAS_COUNTER: u128 = 202651902028573;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct BanProgress {
    incorrect_nonce: u32,
    max_gas: u32,
    revert: Vec<String>,
    excessive_gas: u128,
}

impl BanProgress {
    pub fn reset(&mut self) {
        self.incorrect_nonce = 0;
        self.max_gas = 0;
        self.revert = Vec::new();
        self.excessive_gas = 0;
    }
}

#[derive(Debug, Default, Serialize)]
pub struct BanList {
    pub clients: HashMap<IpAddr, UserClient>,
    pub tokens: HashMap<Token, UserToken>,
    pub addresses: HashMap<Address, UserAddress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum BanReason {
    TooManyIncorrectNonce,
    TooManyMaxGas,
    TooManyReverts,
    UsedExcessiveGas,
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BanKind {
    IncorrectNonce,
    MaxGas,
    Revert(String),
    ExcessiveGas(u32),
}

// TODO: Generalise to the same struct. Move relationship data to another lib.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UserClient {
    tokens: Vec<Token>,
    addresses: Vec<Address>,
    transaction_count: u64,
    ban_progress: BanProgress,
    banned: Option<BanReason>,
    updated: u128,
}

impl UserClient {
    fn update(&mut self) {
        self.updated = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_millis(),
            Err(e) => {
                tracing::error!("{e:?}");
                0
            }
        }
    }

    fn push_token(&mut self, token: Token) {
        self.update();
        self.tokens.push(token);
    }

    fn push_address(&mut self, address: Address) {
        self.update();
        self.addresses.push(address);
    }

    fn increment_transaction_count(&mut self) {
        self.update();
        self.transaction_count += 1;
    }

    fn ban_progress_mut(&mut self) -> &mut BanProgress {
        self.update();
        &mut self.ban_progress
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UserAddress {
    clients: Vec<IpAddr>,
    tokens: Vec<Token>,
    transaction_count: u64,
    ban_progress: BanProgress,
    banned: Option<BanReason>,
    updated: u128,
}

impl UserAddress {
    fn updated(&mut self) {
        self.updated = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_millis(),
            Err(e) => {
                tracing::error!("{e:?}");
                0
            }
        }
    }

    fn push_token(&mut self, token: Token) {
        self.updated();
        self.tokens.push(token);
    }

    fn push_client(&mut self, client: IpAddr) {
        self.updated();
        self.clients.push(client);
    }

    fn increment_transaction_count(&mut self) {
        self.updated();
        self.transaction_count += 1;
    }

    fn ban_progress_mut(&mut self) -> &mut BanProgress {
        self.updated();
        &mut self.ban_progress
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UserToken {
    clients: Vec<IpAddr>,
    addresses: Vec<Address>,
    transaction_count: u64,
    ban_progress: BanProgress,
    banned: Option<BanReason>,
    updated: u128,
}

impl UserToken {
    fn updated(&mut self) {
        self.updated = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_millis(),
            Err(e) => {
                tracing::error!("{e:?}");
                0
            }
        }
    }

    fn push_address(&mut self, address: Address) {
        self.updated();
        self.addresses.push(address);
    }

    fn push_client(&mut self, client: IpAddr) {
        self.updated();
        self.clients.push(client);
    }

    fn increment_transaction_count(&mut self) {
        self.updated();
        self.transaction_count += 1;
    }

    fn ban_progress_mut(&mut self) -> &mut BanProgress {
        self.updated();
        &mut self.ban_progress
    }
}

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

struct UserDetails {
    client: IpAddr,
    address: Address,
    token: Option<Token>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_duration")]
    pub timeframe: Duration,
    pub incorrect_nonce_threshold: u32,
    pub max_gas_threshold: u32,
    pub revert_threshold: u32,
    pub excessive_gas_threshold: u32,
    pub token_multiplier: u32,
}

pub struct Banhammer {
    next_check: Duration,
    user_clients: HashMap<IpAddr, UserClient>,
    user_addresses: HashMap<Address, UserAddress>,
    user_tokens: HashMap<Token, UserToken>,
    ban_list: BanList,
    config: Config,
    leaky_buckets: LeakyBucket,
}

fn check_ban(
    ban_progress: &mut BanProgress,
    config: &Config,
    token: Option<&Token>,
    maybe_error: Option<&TransactionError>,
    near_gas: u128,
) -> Option<BanReason> {
    let near_gas_threshold = {
        if token.is_some() {
            config.excessive_gas_threshold as u128
                * 1_000_000_000_000
                * config.token_multiplier as u128
        } else {
            config.excessive_gas_threshold as u128 * 1_000_000_000_000
        }
    };
    // tracing::debug!("near_gas: {}", near_gas);
    ban_progress.excessive_gas += near_gas;
    if ban_progress.excessive_gas > near_gas_threshold {
        return Some(BanReason::UsedExcessiveGas);
    }

    let error = maybe_error?;
    match error {
        TransactionError::ErrIncorrectNonce => {
            let threshold = {
                if token.is_some() {
                    config.incorrect_nonce_threshold * config.token_multiplier
                } else {
                    config.incorrect_nonce_threshold
                }
            };
            ban_progress.incorrect_nonce += 1;
            if ban_progress.incorrect_nonce >= threshold {
                Some(BanReason::TooManyIncorrectNonce)
            } else {
                None
            }
        }
        TransactionError::InvalidECDSA => {
            let threshold = {
                if token.is_some() {
                    config.incorrect_nonce_threshold * config.token_multiplier
                } else {
                    config.incorrect_nonce_threshold
                }
            };
            ban_progress.incorrect_nonce += 1;
            if ban_progress.incorrect_nonce >= threshold {
                Some(BanReason::TooManyIncorrectNonce)
            } else {
                None
            }
        }
        TransactionError::MaxGas => {
            let threshold = {
                if token.is_some() {
                    config.max_gas_threshold * config.token_multiplier
                } else {
                    config.max_gas_threshold
                }
            };
            ban_progress.max_gas += 1;
            if ban_progress.max_gas >= threshold {
                Some(BanReason::TooManyMaxGas)
            } else {
                None
            }
        }
        TransactionError::Revert(msg) => {
            let threshold = {
                if token.is_some() {
                    config.revert_threshold * config.token_multiplier
                } else {
                    config.revert_threshold
                }
            };
            ban_progress.revert.push(msg.clone());
            if ban_progress.revert.len() as u32 >= threshold {
                Some(BanReason::TooManyReverts)
            } else {
                None
            }
        }
        TransactionError::Relayer(_) => None,
    }
}

impl Banhammer {
    pub fn new(config: Config) -> Self {
        Self {
            next_check: config.timeframe,
            user_clients: HashMap::default(),
            user_addresses: HashMap::default(),
            user_tokens: HashMap::default(),
            ban_list: BanList::default(),
            config,
            leaky_buckets: LeakyBucket::new(),
        }
    }

    /// Process bucket live cycle    
    fn process_bucket(
        &mut self,
        bucket_identity: BucketIdentity,
        bucket_value: BucketNameValue,
        maybe_error: Option<&TransactionError>,
        near_gas: u128,
        token_exist: bool,
    ) {
        let bucket_excessive_gas = BucketName::new(
            bucket_identity.clone(),
            bucket_value.clone(),
            BucketErrorKind::UsedExcessiveGas,
        );
        let _x = self.leaky_buckets.get_fill(
            &bucket_excessive_gas,
            BucketValue::UsedExcessiveGas(near_gas),
        );

        // if no errors - just return
        if maybe_error.is_none() {
            return;
        }
        match maybe_error.unwrap() {
            TransactionError::ErrIncorrectNonce | TransactionError::InvalidECDSA => {
                let threshold = {
                    if token_exist {
                        self.config.incorrect_nonce_threshold * self.config.token_multiplier
                    } else {
                        self.config.incorrect_nonce_threshold
                    }
                };

                let bucket_incorrect_nonce = BucketName::new(
                    bucket_identity,
                    bucket_value,
                    BucketErrorKind::IncorrectNonce,
                );
                let fill_result = self
                    .leaky_buckets
                    .get_fill(&bucket_incorrect_nonce, BucketValue::IncorrectNonce(1));

                if fill_result > BucketValue::Reverts(threshold) {
                    self.leaky_buckets
                        .decrease(bucket_incorrect_nonce, fill_result)
                } else {
                    self.leaky_buckets
                        .fill(bucket_incorrect_nonce, BucketValue::Reverts(1))
                }
            }
            TransactionError::MaxGas => {
                let threshold = {
                    if token_exist {
                        self.config.max_gas_threshold * self.config.token_multiplier
                    } else {
                        self.config.max_gas_threshold
                    }
                };
                let bucket_max_gas =
                    BucketName::new(bucket_identity, bucket_value, BucketErrorKind::MaxGas);
                let fill_result = self
                    .leaky_buckets
                    .get_fill(&bucket_max_gas, BucketValue::MaxGas(1));

                if fill_result > BucketValue::Reverts(threshold) {
                    self.leaky_buckets.decrease(bucket_max_gas, fill_result)
                } else {
                    self.leaky_buckets
                        .fill(bucket_max_gas, BucketValue::Reverts(1))
                }
            }
            TransactionError::Revert(_) => {
                let threshold = {
                    if token_exist {
                        self.config.revert_threshold * self.config.token_multiplier
                    } else {
                        self.config.revert_threshold
                    }
                };
                let bucket_reverts =
                    BucketName::new(bucket_identity, bucket_value, BucketErrorKind::Reverts);
                let fill_result = self
                    .leaky_buckets
                    .get_fill(&bucket_reverts, BucketValue::Reverts(1));
                if fill_result > BucketValue::Reverts(threshold) {
                    self.leaky_buckets.decrease(bucket_reverts, fill_result)
                } else {
                    self.leaky_buckets
                        .fill(bucket_reverts, BucketValue::Reverts(1))
                }
            }
            TransactionError::Relayer(_) => (),
        }
    }

    pub fn tick(&mut self, time: Instant) {
        if time.elapsed() > self.next_check {
            for (_, client) in self.user_clients.iter_mut() {
                client.ban_progress.reset();
            }
            for (_, address) in self.user_addresses.iter_mut() {
                address.ban_progress.reset();
            }
            for (_, token) in self.user_tokens.iter_mut() {
                token.ban_progress.reset();
            }
            self.next_check += self.config.timeframe;
        }
    }

    fn ban_progression(
        &mut self,
        user: &UserDetails,
        token: Option<&Token>,
        maybe_error: Option<&TransactionError>,
        near_gas: u128,
    ) -> (Option<BanReason>, Option<BanReason>, Option<BanReason>) {
        // TODO excessive gas
        let maybe_client_banned = if !self.ban_list.clients.contains_key(&user.client) {
            let client_progress = &mut self
                .user_clients
                .get_mut(&user.client)
                .expect("`UserClient` missing.")
                .ban_progress_mut();
            check_ban(client_progress, &self.config, token, maybe_error, near_gas)
        } else {
            None
        };

        let maybe_address_banned = if !self.ban_list.addresses.contains_key(&user.address) {
            let address_progress = &mut self
                .user_addresses
                .get_mut(&user.address)
                .expect("`UserAddress' missing.")
                .ban_progress_mut();
            check_ban(address_progress, &self.config, token, maybe_error, near_gas)
        } else {
            None
        };

        let maybe_token_banned = {
            if let Some(token) = token {
                if !self.ban_list.tokens.contains_key(token) {
                    let token_progress = &mut self
                        .user_tokens
                        .get_mut(token)
                        .expect("'UserToken' missing.")
                        .ban_progress_mut();
                    // .ban_progress;
                    check_ban(
                        token_progress,
                        &self.config,
                        Some(token),
                        maybe_error,
                        near_gas,
                    )
                } else {
                    None
                }
            } else {
                None
            }
        };

        (
            maybe_client_banned,
            maybe_address_banned,
            maybe_token_banned,
        )
    }

    fn associate_with_user_client(
        &mut self,
        client: IpAddr,
        address: Address,
        maybe_token: Option<Token>,
    ) {
        let user_client = self
            .user_clients
            .entry(client)
            .or_insert_with(UserClient::default);

        if !user_client.addresses.contains(&address) {
            user_client.push_address(address);
        }

        if let Some(token) = maybe_token {
            if !user_client.tokens.contains(&token) {
                user_client.push_token(token);
            }
        }
    }

    fn associate_with_user_address(
        &mut self,
        address: Address,
        client: IpAddr,
        maybe_token: Option<Token>,
    ) {
        let user_address = self
            .user_addresses
            .entry(address)
            .or_insert_with(UserAddress::default);

        if !user_address.clients.contains(&client) {
            user_address.push_client(client);
        }

        if let Some(token) = maybe_token {
            if !user_address.tokens.contains(&token) {
                user_address.push_token(token);
            }
        }
    }

    fn associate_with_user_token(&mut self, token: Token, client: IpAddr, address: Address) {
        let user_token = self
            .user_tokens
            .entry(token)
            .or_insert_with(UserToken::default);

        if !user_token.clients.contains(&client) {
            user_token.push_client(client);
        }

        if !user_token.addresses.contains(&address) {
            user_token.push_address(address);
        }
    }

    fn increment_transaction_count(&mut self, user: &UserDetails) {
        let user_client = self
            .user_clients
            .get_mut(&user.client)
            .expect("'UserClient' missing.");
        user_client.increment_transaction_count();

        let user_address = self
            .user_addresses
            .get_mut(&user.address)
            .expect("'UserAddress' missing");
        user_address.increment_transaction_count();

        if let Some(token) = &user.token {
            let user_token = self
                .user_tokens
                .get_mut(token)
                .expect("'UserToken' missing");
            user_token.increment_transaction_count();
        }
    }

    pub fn read_input(&mut self, input: &RelayerMessage) -> Vec<BanReason> {
        let mut ban_reasons = vec![];
        let maybe_error = input.error.as_ref();
        let user = UserDetails {
            client: input.client,
            address: input.params.from,
            token: input.token.clone(),
        };

        self.associate_with_user_client(user.client, user.address, user.token.clone());
        self.associate_with_user_address(user.address, user.client, user.token.clone());
        if let Some(token) = user.token.clone() {
            self.associate_with_user_token(token, user.client, user.address);
        }
        let token_exist = user.token.clone().is_some();
        self.process_bucket(
            BucketIdentity::IP,
            BucketNameValue::IP(input.client),
            maybe_error,
            NEAR_GAS_COUNTER,
            token_exist,
        );
        self.process_bucket(
            BucketIdentity::Address,
            BucketNameValue::Address(input.params.from),
            maybe_error,
            NEAR_GAS_COUNTER,
            token_exist,
        );
        if let Some(token) = input.token.clone() {
            self.process_bucket(
                BucketIdentity::Token,
                BucketNameValue::Token(token),
                maybe_error,
                NEAR_GAS_COUNTER,
                token_exist,
            );
        }

        self.increment_transaction_count(&user);

        let (maybe_client_banned, maybe_address_banned, maybe_token_banned) =
            self.ban_progression(&user, user.token.as_ref(), maybe_error, NEAR_GAS_COUNTER); // TODO: add from relayer message when available

        if let Some(ban_reason) = maybe_client_banned {
            tracing::info!("BANNED client: {}, reason: {:?}", user.client, ban_reason);
            ban_reasons.push(ban_reason.clone());

            let mut user_client = self
                .user_clients
                .remove(&user.client)
                .expect("`UserClient` missing.");
            user_client.banned = Some(ban_reason);
            self.ban_list.clients.insert(user.client, user_client);
        }
        if let Some(ban_reason) = maybe_address_banned {
            tracing::info!(
                "BANNED address: {:?}, reason: {:?}",
                user.address,
                ban_reason
            );
            ban_reasons.push(ban_reason.clone());

            let mut user_address = self
                .user_addresses
                .remove(&user.address)
                .expect("`UserAddress` missing.");
            user_address.banned = Some(ban_reason);
            self.ban_list.addresses.insert(user.address, user_address);
        }
        if let Some(ban_reason) = maybe_token_banned {
            let token = user.token.expect("'Token' missing.");
            tracing::info!("BANNED token: {:?}, reason: {:?}", token, ban_reason);
            ban_reasons.push(ban_reason.clone());

            let mut user_token = self
                .user_tokens
                .remove(&token)
                .expect("`UserToken` missing.");
            user_token.banned = Some(ban_reason);
            self.ban_list.tokens.insert(token, user_token);
        }

        ban_reasons
    }

    pub fn user_clients(&self) -> &HashMap<IpAddr, UserClient> {
        &self.user_clients
    }

    pub fn user_addresses(&self) -> &HashMap<Address, UserAddress> {
        &self.user_addresses
    }

    pub fn user_tokens(&self) -> &HashMap<Token, UserToken> {
        &self.user_tokens
    }

    pub fn bans(&self) -> &BanList {
        &self.ban_list
    }
}
