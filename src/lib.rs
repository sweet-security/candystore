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
mod stats;
mod store;
mod typed;

pub use hashing::HashSeed;
pub use lists::{ListCompactionParams, ListIterator};
pub use stats::Stats;
pub use store::{CandyStore, GetOrCreateStatus, ReplaceStatus, SetStatus};
pub use typed::{CandyTypedDeque, CandyTypedKey, CandyTypedList, CandyTypedStore};

use std::fmt::{Display, Formatter};

#[cfg(feature = "whitebox_testing")]
pub use hashing::HASH_BITS_TO_KEEP;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CandyError {
    KeyTooLong(usize),
    ValueTooLong(usize),
    EntryCannotFitInShard(usize, usize),
    KeyAlreadyExists(Vec<u8>, u64),
}

impl Display for CandyError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::KeyTooLong(sz) => write!(f, "key too long {sz}"),
            Self::ValueTooLong(sz) => write!(f, "value too long {sz}"),
            Self::KeyAlreadyExists(key, ph) => {
                write!(f, "key {key:?} already exists (0x{ph:016x})")
            }
            Self::EntryCannotFitInShard(sz, max) => {
                write!(f, "entry too big ({sz}) for a single shard file ({max})")
            }
        }
    }
}

impl std::error::Error for CandyError {}

pub type Result<T> = anyhow::Result<T>;

/// The configuration options for CandyStore. Comes with sane defaults, feel free to use them
#[derive(Debug, Clone)]
pub struct Config {
    /// we don't want huge shards, because splitting would be expensive
    pub max_shard_size: u32,
    /// should be ~10% of max_shard_size
    pub min_compaction_threashold: u32,
    /// just some entropy, not so important unless you fear DoS
    pub hash_seed: HashSeed,
    /// hint for creating number of shards accordingly)
    pub expected_number_of_keys: usize,
    /// number of keyed locks for concurrent list ops
    pub max_concurrent_list_ops: u32,
    /// whether or not to truncate up shard files to their max size (spare files)
    pub truncate_up: bool,
    /// whether or not to clear the DB if the version is unsupported
    pub clear_on_unsupported_version: bool,
    /// whether or not to mlock the shard headers to RAM (POSIX only)
    pub mlock_headers: bool,
    /// optionally delay modifying operations before for the given duration before flushing data to disk,
    /// to ensure reboot consistency
    #[cfg(feature = "flush_aggregation")]
    pub flush_aggregation_delay: Option<std::time::Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_shard_size: 64 * 1024 * 1024,
            min_compaction_threashold: 8 * 1024 * 1024,
            hash_seed: *b"kOYLu0xvq2WtzcKJ",
            expected_number_of_keys: 0,
            max_concurrent_list_ops: 64,
            truncate_up: true,
            clear_on_unsupported_version: false,
            mlock_headers: false,
            #[cfg(feature = "flush_aggregation")]
            flush_aggregation_delay: None,
        }
    }
}

pub(crate) const MAX_TOTAL_KEY_SIZE: usize = 0x3fff; // 14 bits
pub(crate) const NAMESPACING_RESERVED_SIZE: usize = 0xff;
pub const MAX_KEY_SIZE: usize = MAX_TOTAL_KEY_SIZE - NAMESPACING_RESERVED_SIZE;
pub const MAX_VALUE_SIZE: usize = 0xffff;

const _: () = assert!(MAX_KEY_SIZE <= u16::MAX as usize);
const _: () = assert!(MAX_VALUE_SIZE <= u16::MAX as usize);
