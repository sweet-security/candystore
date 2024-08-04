#![feature(btree_cursors)]

mod hashing;
mod shard;
mod store;

pub use hashing::SecretKey;
use std::fmt::{Display, Formatter};
pub use store::{Stats, VickyStore};

#[derive(Debug)]
pub enum Error {
    WrongSecretKeyLength,
    KeyTooLong,
    ValueTooLong,
    KeyNotFound,
    IOError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Error::WrongSecretKeyLength => write!(f, "wrong secret length"),
            Error::KeyTooLong => write!(f, "key too long"),
            Error::KeyNotFound => write!(f, "key not found"),
            Error::ValueTooLong => write!(f, "value too long"),
            Error::IOError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// The configuration options for VickyStore. Comes with sane defaults, feel free to use them
#[derive(Debug, Clone)]
pub struct Config {
    pub max_shard_size: u32, // we don't want huge shards, because splitting would be expensive
    pub min_compaction_threashold: u32, // should be ~10% of max_shard_size
    pub secret_key: SecretKey, // just some entropy, not so important unless you fear DoS
    pub expected_number_of_keys: usize, // hint for creating number of shards accordingly)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_shard_size: 64 * 1024 * 1024,
            min_compaction_threashold: 8 * 1024 * 1024,
            secret_key: SecretKey::new(b"kOYLu0xvq2WtzcKJ").unwrap(),
            expected_number_of_keys: 0,
        }
    }
}
