//! A fast (*blazingly*, of course), persistent, in-process key-value store that relies on a novel sharding
//! algorithm. Since Candy does not rely on log-structured merge (LSM) trees or B-Trees, no journal/WAL is needed
//! and IOs go directly to file.
//!
//! The algorithm can be thought of as a "zero-overhead" extension to a hash table that's stored over files,
//! as it's designed to minimizes disk IO operations. Most operations add an overhead of 1-2 microseconds
//! to the disk IO latency, and operations generally require 1-4 disk IOs.
//!
//! The algorithm, for the most part, is crash-safe. That is, you can crash at any point and still be in a consistent
//! state. You might lose the ongoing operation, but we consider this acceptable.
//!
//! Candy is designed to consume very little memory: entries are written directly to the shard-file, and only a
//! table of ~380KB is kept `mmap`-ed (it is also file-backed, so can be evicted if needed). A shard-file can
//! hold around 30K entries, and more shard-files are created as needed.
//!
//! A unique feature of Candy is the support of *lists*, which allow creating cheap collections.
//!
//! Notes:
//! * the file format is not yet stable!
//! * nightly is required for `simd_itertools`
//!
//! Example:
//! ```
//! use candystore::{CandyStore, Config, Result};
//!
//! fn main() -> Result<()> {
//!     let db = CandyStore::open("/tmp/candy-dir", Config::default())?;
//!     db.set("hello", "world")?;
//!     assert_eq!(db.get("hello")?, Some("world".into()));
//!     db.remove("hello")?;
//!     assert_eq!(db.get("hello")?, None);
//!
//!     // lists
//!     db.set_in_list("italian", "bye", "arrivederci")?;
//!     db.set_in_list("italian", "thanks", "grazie")?;
//!     assert_eq!(db.get_from_list("italian", "bye")?, Some("arrivederci".into()));
//!
//!     db.set_in_list("spanish", "bye", "adios")?;
//!     db.set_in_list("spanish", "thanks", "gracias")?;
//!
//!     let items = db.iter_list("spanish").map(|res| res.unwrap()).collect::<Vec<_>>();
//!     assert_eq!(items, vec![("bye".into(), "adios".into()), ("thanks".into(), "gracias".into())]);
//!
//!     Ok(())
//! }
//! ```

mod encodable;
mod hashing;
mod lists;
mod router;
mod shard;
mod store;
mod typed;

pub use hashing::HashSeed;
pub use lists::{ListCompactionParams, ListIterator};
use std::fmt::{Display, Formatter};
pub use store::{
    CandyStore, CoarseHistogram, GetOrCreateStatus, ReplaceStatus, SetStatus, SizeHistogram, Stats,
};
pub use typed::{CandyTypedDeque, CandyTypedKey, CandyTypedList, CandyTypedStore};

#[cfg(feature = "whitebox_testing")]
pub use hashing::HASH_BITS_TO_KEEP;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CandyError {
    WrongHashSeedLength,
    KeyTooLong(usize),
    ValueTooLong(usize),
    EntryCannotFitInShard(usize, usize),
    KeyNotFound,
    CompactionFailed(String),
    SplitFailed(String),
    LoadingFailed(String),
}

impl Display for CandyError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::WrongHashSeedLength => write!(f, "wrong hash seed length"),
            Self::KeyTooLong(sz) => write!(f, "key too long {sz}"),
            Self::KeyNotFound => write!(f, "key not found"),
            Self::ValueTooLong(sz) => write!(f, "value too long {sz}"),
            Self::EntryCannotFitInShard(sz, max) => {
                write!(f, "entry too big ({sz}) for a single shard file ({max})")
            }
            Self::CompactionFailed(s) => write!(f, "shard compaction failed: {s}"),
            Self::LoadingFailed(s) => write!(f, "loading store failed: {s}"),
            Self::SplitFailed(s) => write!(f, "shard split failed: {s}"),
        }
    }
}

impl std::error::Error for CandyError {}

pub type Result<T> = anyhow::Result<T>;

/// The configuration options for CandyStore. Comes with sane defaults, feel free to use them
#[derive(Debug, Clone)]
pub struct Config {
    pub max_shard_size: u32, // we don't want huge shards, because splitting would be expensive
    pub min_compaction_threashold: u32, // should be ~10% of max_shard_size
    pub hash_seed: HashSeed, // just some entropy, not so important unless you fear DoS
    pub expected_number_of_keys: usize, // hint for creating number of shards accordingly)
    pub merge_small_shards: bool, // whether or not to merge small shards when items are removed
    pub max_concurrent_list_ops: u32, // number of keyed locks for concurrent list ops
    pub truncate_up: bool, // whether or not to truncate up shard files to their max size (spare files)
    pub clear_on_unsupported_version: bool, // whether or not to clear the DB if the version is unsupported
    pub mlock_headers: bool, // whether or not to mlock the shard headers to RAM (POSIX only)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_shard_size: 64 * 1024 * 1024,
            min_compaction_threashold: 8 * 1024 * 1024,
            hash_seed: HashSeed::from_buf(b"kOYLu0xvq2WtzcKJ").unwrap(),
            expected_number_of_keys: 0,
            merge_small_shards: false,
            max_concurrent_list_ops: 64,
            truncate_up: true,
            clear_on_unsupported_version: false,
            mlock_headers: false,
        }
    }
}

pub(crate) const MAX_TOTAL_KEY_SIZE: usize = 0x3fff; // 14 bits
pub(crate) const NAMESPACING_RESERVED_SIZE: usize = 0xff;
pub const MAX_KEY_SIZE: usize = MAX_TOTAL_KEY_SIZE - NAMESPACING_RESERVED_SIZE;
pub const MAX_VALUE_SIZE: usize = 0xffff;

const _: () = assert!(MAX_KEY_SIZE <= u16::MAX as usize);
const _: () = assert!(MAX_VALUE_SIZE <= u16::MAX as usize);
