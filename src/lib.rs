#![feature(btree_cursors)]

mod hashing;
mod insertion;
mod shard;
mod store;
mod typed;

pub use hashing::SecretKey;
pub use insertion::{GetOrCreateStatus, ReplaceStatus, SetStatus};
use std::fmt::{Display, Formatter};
pub use store::{Stats, VickyStore};
pub use typed::{VickyTypedKey, VickyTypedStore};

#[derive(Debug)]
pub enum VickyError {
    WrongSecretKeyLength,
    KeyTooLong,
    ValueTooLong,
    KeyNotFound,
}

impl Display for VickyError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::WrongSecretKeyLength => write!(f, "wrong secret length"),
            Self::KeyTooLong => write!(f, "key too long"),
            Self::KeyNotFound => write!(f, "key not found"),
            Self::ValueTooLong => write!(f, "value too long"),
        }
    }
}

impl std::error::Error for VickyError {}

/// It is an alias for a boxed [std::error::Error].
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

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
