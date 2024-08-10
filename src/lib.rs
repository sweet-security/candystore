#![feature(btree_cursors)]

mod collections;
mod hashing;
mod insertion;
mod shard;
mod store;
mod typed;

pub use hashing::HashSeed;
pub use insertion::{GetOrCreateStatus, ModifyStatus, ReplaceStatus, SetStatus};
use std::fmt::{Display, Formatter};
pub use store::{Stats, VickyStore};
pub use typed::{VickyTypedCollection, VickyTypedKey, VickyTypedStore};

#[derive(Debug)]
pub enum VickyError {
    WrongHashSeedLength,
    KeyTooLong,
    ValueTooLong,
    KeyNotFound,
    CompactionFailed(String),
    SplitFailed(String),
    LoadingFailed(String),
    CorruptedLinkedList(String),
}

impl Display for VickyError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::WrongHashSeedLength => write!(f, "wrong hash seed length"),
            Self::KeyTooLong => write!(f, "key too long"),
            Self::KeyNotFound => write!(f, "key not found"),
            Self::ValueTooLong => write!(f, "value too long"),
            Self::CorruptedLinkedList(s) => write!(f, "corrupted linked list: {s}"),
            Self::CompactionFailed(s) => write!(f, "shard compaction failed: {s}"),
            Self::LoadingFailed(s) => write!(f, "loading store failed: {s}"),
            Self::SplitFailed(s) => write!(f, "shard split failed: {s}"),
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
    pub hash_seed: HashSeed, // just some entropy, not so important unless you fear DoS
    pub expected_number_of_keys: usize, // hint for creating number of shards accordingly)
    pub merge_small_shards: bool, // whether or not to merge small shards when items are removed
    pub max_concurrent_collection_ops: u32, // number of keyed locks for concurrent collection ops
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_shard_size: 64 * 1024 * 1024,
            min_compaction_threashold: 8 * 1024 * 1024,
            hash_seed: HashSeed::new(b"kOYLu0xvq2WtzcKJ").unwrap(),
            expected_number_of_keys: 0,
            merge_small_shards: false,
            max_concurrent_collection_ops: 64,
        }
    }
}

pub(crate) const MAX_TOTAL_KEY_SIZE: usize = 0x3fff; // 14 bits
pub(crate) const NAMESPACING_RESERVED_SIZE: usize = 0xff;
pub const MAX_KEY_SIZE: usize = MAX_TOTAL_KEY_SIZE - NAMESPACING_RESERVED_SIZE;
pub const MAX_VALUE_SIZE: usize = 0xffff;

const _: () = assert!(MAX_KEY_SIZE <= u16::MAX as usize);
const _: () = assert!(MAX_VALUE_SIZE <= u16::MAX as usize);
