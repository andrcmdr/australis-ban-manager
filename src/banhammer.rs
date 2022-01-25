use crate::de::{RelayerInput, Token, TransactionError};
use ethereum_types::Address;
use serde::{
    de::{self, Error, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::{
    collections::HashMap,
    fmt::{self},
    net::IpAddr,
    time::{Duration, Instant},
};
use tracing::info;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct BanProgress {
    incorrect_nonce: u32,
    max_gas: u32,
    revert: Vec<String>,
    excessive_gas: u32,
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

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UserClient {
    tokens: Vec<Token>,
    addresses: Vec<Address>,
    transaction_count: u64,
    ban_progress: BanProgress,
    banned: Option<BanReason>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UserAddress {
    clients: Vec<IpAddr>,
    tokens: Vec<Token>,
    transaction_count: u64,
    ban_progress: BanProgress,
    banned: Option<BanReason>,
    // last_update: SystemTime,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct UserToken {
    clients: Vec<IpAddr>,
    addresses: Vec<Address>,
    transaction_count: u64,
    ban_progress: BanProgress,
    banned: Option<BanReason>,
    // last_update: SystemTime,
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

struct User {
    client: IpAddr,
    address: Address,
    token: Option<Token>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_duration")]
    timeframe: Duration,
    incorrect_nonce_threshold: u32,
    max_gas_threshold: u32,
    revert_threshold: u32,
    // excessive_gas_threshold: u32, // TODO
    token_multiplier: u32,
}

#[derive(Debug, Serialize)]
pub struct Banhammer {
    next_check: Duration,
    user_clients: HashMap<IpAddr, UserClient>,
    user_addresses: HashMap<Address, UserAddress>,
    user_tokens: HashMap<Token, UserToken>,
    ban_list: BanList,
    config: Config,
}

fn check_error_ban(
    ban_progress: &mut BanProgress,
    config: &Config,
    token: Option<&Token>,
    maybe_error: Option<&TransactionError>,
) -> Option<BanReason> {
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
        }
    }

    pub fn tick(&mut self, time: Instant) {
        if time.elapsed() > self.next_check {
            for (_, client) in self.user_clients.iter_mut() {
                client.ban_progress.excessive_gas = 0;
                client.ban_progress.max_gas = 0;
                client.ban_progress.incorrect_nonce = 0;
                client.ban_progress.revert = Vec::new();
            }
            // TODO user_tokens / user_froms
            self.next_check += self.config.timeframe;
        }
    }

    fn ban_progression(
        &mut self,
        user: &User,
        token: Option<&Token>,
        maybe_error: Option<&TransactionError>,
        // gas: // TODO when added to relayer
    ) -> (Option<BanReason>, Option<BanReason>, Option<BanReason>) {
        // TODO excessive gas
        let maybe_client_banned = if !self.ban_list.clients.contains_key(&user.client) {
            let client_progress = &mut self
                .user_clients
                .get_mut(&user.client)
                .expect("`UserClient` missing.")
                .ban_progress;
            check_error_ban(client_progress, &self.config, token, maybe_error)
        } else {
            None
        };

        let maybe_address_banned = if !self.ban_list.addresses.contains_key(&user.address) {
            let address_progress = &mut self
                .user_addresses
                .get_mut(&user.address)
                .expect("`UserAddress' missing.")
                .ban_progress;
            check_error_ban(address_progress, &self.config, token, maybe_error)
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
                        .ban_progress;
                    check_error_ban(token_progress, &self.config, Some(token), maybe_error)
                } else {
                    None
                }
            } else {
                None
            }
        };

        (maybe_client_banned, maybe_address_banned, maybe_token_banned)
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
            user_client.addresses.push(address)
        }

        if let Some(token) = maybe_token {
            if !user_client.tokens.contains(&token) {
                user_client.tokens.push(token);
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
            user_address.clients.push(client);
        }

        if let Some(token) = maybe_token {
            if !user_address.tokens.contains(&token) {
                user_address.tokens.push(token);
            }
        }
    }

    fn associate_with_user_token(&mut self, token: Token, client: IpAddr, address: Address) {
        let user_token = self
            .user_tokens
            .entry(token)
            .or_insert_with(UserToken::default);

        if !user_token.clients.contains(&client) {
            user_token.clients.push(client);
        }

        if !user_token.addresses.contains(&address) {
            user_token.addresses.push(address);
        }
    }

    fn increment_transaction_count(&mut self, user: &User) {
        let user_client = self
            .user_clients
            .get_mut(&user.client)
            .expect("'UserClient' missing.");
        user_client.transaction_count += 1;

        let user_address = self
            .user_addresses
            .get_mut(&user.address)
            .expect("'UserAddress' missing");
        user_address.transaction_count += 1;

        if let Some(token) = &user.token {
            let user_token = self
                .user_tokens
                .get_mut(token)
                .expect("'UserToken' missing");
            user_token.transaction_count += 1;
            // user_token.last_update = SystemTime::now();
        }
    }

    pub fn read_input(&mut self, input: &RelayerInput) {
        let maybe_error = input.error.as_ref();
        let user = User {
            client: input.client,
            address: input.params.from,
            token: input.token.clone(),
        };

        self.associate_with_user_client(user.client, user.address, user.token.clone());
        self.associate_with_user_address(user.address, user.client, user.token.clone());
        if let Some(token) = user.token.clone() {
            self.associate_with_user_token(token, user.client, user.address);
        }

        self.increment_transaction_count(&user);

        let (maybe_client_banned, maybe_address_banned, maybe_token_banned) =
            self.ban_progression(&user, user.token.as_ref(), maybe_error);

        if let Some(ban_reason) = maybe_client_banned {
            info!(
                "BANNED client: {}, reason: {:?}",
                user.client,
                maybe_error.expect("Error expected")
            );
            let mut user_client = self
                .user_clients
                .remove(&user.client)
                .expect("`UserClient` missing.");
            user_client.banned = Some(ban_reason);
            self.ban_list.clients.insert(user.client, user_client);
        }
        if let Some(ban_reason) = maybe_address_banned {
            info!(
                "BANNED address: {:?}, reason: {:?}",
                user.address,
                maybe_error.expect("Error expected")
            );
            let mut user_address = self
                .user_addresses
                .remove(&user.address)
                .expect("`UserAddress` missing.");
            user_address.banned = Some(ban_reason);
            self.ban_list.addresses.insert(user.address, user_address);
        }
        if let Some(ban_reason) = maybe_token_banned {
            let token = user.token.expect("'Token' missing.");
            info!(
                "BANNED token: {:?}, reason: {:?}",
                token,
                maybe_error.expect("Error expected")
            );
            let mut user_token = self
                .user_tokens
                .remove(&token)
                .expect("`UserToken` missing.");
            user_token.banned = Some(ban_reason);
            self.ban_list.tokens.insert(token, user_token);
        }
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
