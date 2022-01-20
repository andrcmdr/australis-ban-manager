use crate::de::{RelayerInput, Token, TransactionError};
use ethereum_types::Address;
use serde::{
    de::{self, Error, Visitor},
    Deserialize, Deserializer,
};
use std::{
    collections::HashMap,
    fmt::{self},
    net::IpAddr,
    time::{Duration, Instant},
};

#[derive(Debug, Default, Clone)]
pub struct BanCount {
    incorrect_nonce: u32,
    max_gas: u32,
    revert: Vec<String>,
    excessive_gas: u32,
    // banned_reason: Option<BanKind>,
}

#[derive(Debug, Default)]
pub struct BanList {
    pub ip: HashMap<IpAddr, BanCount>,
    pub token: HashMap<Token, BanCount>,
    pub from: HashMap<Address, BanCount>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BanKind {
    IncorrectNonce,
    MaxGas,
    Revert(String),
    ExcessiveGas(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UserKind {
    Client(IpAddr),
    From(Address),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BannedUserKind {
    Client(IpAddr),
    Token(Token),
    From(Address),
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

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_duration")]
    timeframe: Duration,
    incorrect_nonce_threshold: u32,
    max_gas_threshold: u32,
    revert_threshold: u32,
    excessive_gas_threshold: u32,
    token_multiplier: u32,
}

#[derive(Debug)]
pub struct Banhammer {
    next_check: Duration,
    users: HashMap<UserKind, BanCount>,
    ban_list: BanList,
    config: Config,
}

impl Banhammer {
    pub fn new(config: Config) -> Self {
        Self {
            next_check: config.timeframe,
            users: HashMap::default(),
            ban_list: BanList::default(),
            config,
        }
    }

    pub fn tick(&mut self, time: Instant) {
        if time.elapsed() > self.next_check {
            for (_user, ban_count) in self.users.iter_mut() {
                ban_count.excessive_gas = 0;
                ban_count.max_gas = 0;
                ban_count.incorrect_nonce = 0;
                ban_count.revert = Vec::new();
            }
            self.next_check += self.config.timeframe;
        }
    }

    pub fn ban_progression(
        &mut self,
        user: UserKind,
        token: Option<&Token>,
        ban_kind: BanKind,
    ) -> bool {
        let ban_count = self
            .users
            .entry(user.clone())
            .or_insert_with(|| BanCount::default());

        let banned = match ban_kind {
            BanKind::IncorrectNonce => {
                let threshold = {
                    if token.is_some() {
                        self.config.incorrect_nonce_threshold * self.config.token_multiplier
                    } else {
                        self.config.incorrect_nonce_threshold
                    }
                };
                ban_count.incorrect_nonce += 1;
                ban_count.incorrect_nonce >= threshold
            }
            BanKind::MaxGas => {
                let threshold = {
                    if token.is_some() {
                        self.config.max_gas_threshold * self.config.token_multiplier
                    } else {
                        self.config.max_gas_threshold
                    }
                };
                ban_count.max_gas += 1;
                ban_count.max_gas >= threshold
            }
            BanKind::Revert(msg) => {
                let threshold = {
                    if token.is_some() {
                        self.config.revert_threshold * self.config.token_multiplier
                    } else {
                        self.config.revert_threshold
                    }
                };
                ban_count.revert.push(msg);
                ban_count.max_gas >= threshold
            }
            BanKind::ExcessiveGas(gas) => {
                let threshold = {
                    if token.is_some() {
                        self.config.excessive_gas_threshold * self.config.token_multiplier
                    } else {
                        self.config.excessive_gas_threshold
                    }
                };
                ban_count.max_gas += gas;
                ban_count.max_gas >= threshold
            }
        };

        if banned {
            if let Some(token) = token.cloned() {
                self.ban_list.token.insert(token, ban_count.clone());
            }
            match user {
                UserKind::Client(ip) => {
                    self.ban_list.ip.insert(ip, ban_count.clone());
                }
                UserKind::From(addr) => {
                    self.ban_list.from.insert(addr, ban_count.clone());
                }
            }
        }

        banned
    }

    pub fn read_input(&mut self, input: &RelayerInput) -> Vec<(UserKind, BanKind)> {
        let users = {
            let ip = UserKind::Client(input.client);
            let from = UserKind::From(input.params.from);
            vec![ip, from]
        };
        let token = input.token.as_ref();

        let mut bans: Vec<(UserKind, BanKind)> = Vec::with_capacity(3);

        match &input.error {
            Some(TransactionError::ErrIncorrectNonce) => {
                for user_kind in &users {
                    if self.ban_progression(user_kind.clone(), token, BanKind::IncorrectNonce) {
                        bans.push((user_kind.clone(), BanKind::IncorrectNonce));
                    }
                }
            }
            Some(TransactionError::MaxGas) => {
                for user_kind in &users {
                    if self.ban_progression(user_kind.clone(), token, BanKind::MaxGas) {
                        bans.push((user_kind.clone(), BanKind::MaxGas))
                    }
                }
            }
            Some(TransactionError::Revert(msg)) => {
                for user_kind in &users {
                    if self.ban_progression(user_kind.clone(), token, BanKind::Revert(msg.clone()))
                    {
                        bans.push((user_kind.clone(), BanKind::Revert(msg.clone())))
                    }
                }
            }
            _ => {}
        }

        bans
    }

    pub fn ban_list(&self) -> &BanList {
        &self.ban_list
    }
}
