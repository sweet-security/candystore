/// Maximum supported data-file size after internal encoding overhead limits.
pub const MAX_FILE_SIZE: usize = (1 << 30) - (1 << 24);
/// Maximum supported user key length in bytes.
pub const MAX_USER_KEY_SIZE: usize = crate::internal::MAX_INTERNAL_KEY_SIZE - 16;
/// Maximum supported inline value length in bytes.
pub const MAX_USER_VALUE_SIZE: usize = crate::internal::MAX_INTERNAL_VALUE_SIZE - 64;

pub(crate) const ROW_WIDTH: usize = crate::internal::ROW_WIDTH;
pub(crate) const INITIAL_DATA_FILE_ORDINAL: u64 = 0x00bd_38a0_2a35_1cdf;

use crate::internal::MIN_INITIAL_ROWS;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
/// Runtime configuration for opening a store.
pub struct Config {
    /// SipHash keys used for row selection and signatures.
    ///
    /// When a store is created or fully reset, this key is written into the
    /// index header. Reopening an existing store reuses the persisted hash key
    /// from disk even if a different value is provided here.
    pub hash_key: (u64, u64),
    /// Whether to try to lock index mmaps into memory.
    pub mlock_index: bool,
    /// Growth factor used when remapping index structures.
    pub remap_scaler: u8,
    /// Initial target capacity in number of key/value entries.
    pub initial_capacity: usize,
    /// Maximum size of a single data file in bytes.
    pub max_data_file_size: u32,
    /// Minimum per-file waste threshold before background compaction considers it.
    pub compaction_min_threshold: u32,
    /// Maximum logical concurrency used to size internal lock tables, defaults to num_cpus*2
    pub max_concurrency: usize,
    /// Reset the database if opening encounters invalid on-disk data.
    pub reset_on_invalid_data: bool,
    /// Target background compaction throughput in bytes per second.
    pub compaction_throughput_bytes_per_sec: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hash_key: (0x7c2b_23a8_12c2_005f, 0x1f6a_4035_386e_c891),
            mlock_index: false,
            remap_scaler: 1,
            initial_capacity: MIN_INITIAL_ROWS * ROW_WIDTH,
            max_data_file_size: 64 * 1024 * 1024,
            compaction_min_threshold: 24 * 1024 * 1024,
            max_concurrency: (2 * num_cpus::get()).clamp(16, 64),
            reset_on_invalid_data: false,
            compaction_throughput_bytes_per_sec: 4 * 1024 * 1024,
        }
    }
}

#[derive(thiserror::Error, Debug)]
/// Errors returned by store operations and open/recovery flows.
pub enum Error {
    #[error("IO error: {0}")]
    IOError(std::io::Error),

    #[error("Missing data file: {0}")]
    MissingDataFile(u16),

    #[error("Data file {0} reached size limit")]
    RotateDataFile(u16),

    #[error("Row needs splitting at split level {0}")]
    SplitRow(u64),

    #[error("Too many data files")]
    TooManyDataFiles,

    #[error("Lockfile {0} is taken by {1}")]
    LockfileTaken(std::path::PathBuf, String),

    #[error("Payload {0} too large")]
    PayloadTooLarge(usize),
}

/// Convenience result type used by the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome of a conditional replace operation.
pub enum ReplaceStatus {
    /// The key existed and the previous value was replaced.
    PrevValue(Vec<u8>),
    /// The key existed, but its current value did not match the expected value.
    WrongValue(Vec<u8>),
    /// The key did not exist.
    DoesNotExist,
}

impl ReplaceStatus {
    /// Returns `true` when the value was replaced.
    pub fn was_replaced(&self) -> bool {
        matches!(self, Self::PrevValue(_))
    }

    /// Returns `true` when the replace operation did not update the value.
    pub fn failed(&self) -> bool {
        !self.was_replaced()
    }

    /// Returns `true` when the target key was missing.
    pub fn is_key_missing(&self) -> bool {
        matches!(self, Self::DoesNotExist)
    }

    /// Returns `true` when the expected value check failed.
    pub fn is_wrong_value(&self) -> bool {
        matches!(self, Self::WrongValue(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome of a set operation.
pub enum SetStatus {
    /// The key existed and the previous value was returned.
    PrevValue(Vec<u8>),
    /// The key was newly inserted.
    CreatedNew,
}

impl SetStatus {
    /// Returns `true` when the key did not previously exist.
    pub fn was_created(&self) -> bool {
        matches!(self, Self::CreatedNew)
    }

    /// Returns `true` when the key previously existed and was overwritten.
    pub fn was_replaced(&self) -> bool {
        matches!(self, Self::PrevValue(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome of a get-or-create operation.
pub enum GetOrCreateStatus {
    /// The key already existed and its current value was returned.
    ExistingValue(Vec<u8>),
    /// The key was created with the provided default value.
    CreatedNew(Vec<u8>),
}

impl GetOrCreateStatus {
    /// Returns `true` when the key was inserted by the operation.
    pub fn was_created(&self) -> bool {
        matches!(self, Self::CreatedNew(_))
    }

    /// Returns `true` when the key already existed.
    pub fn already_exists(&self) -> bool {
        matches!(self, Self::ExistingValue(_))
    }

    /// Returns the resulting value regardless of whether it was created or already existed.
    pub fn value(self) -> Vec<u8> {
        match self {
            Self::ExistingValue(value) | Self::CreatedNew(value) => value,
        }
    }
}

#[derive(Debug, Clone, Copy)]
/// Heuristics controlling list compaction.
pub struct ListCompactionParams {
    /// Minimum list span length before compaction is considered.
    pub min_length: u64,
    /// Minimum hole ratio required to trigger compaction.
    pub min_holes_ratio: f64,
}

impl Default for ListCompactionParams {
    fn default() -> Self {
        Self {
            min_length: 100,
            min_holes_ratio: 0.25,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Snapshot of store-level counters and size statistics.
pub struct Stats {
    /// Number of allocated index rows.
    pub num_rows: u64,
    /// Theoretical maximum number of entries at the current row count.
    pub capacity: u64,
    /// Number of currently live entries.
    pub num_items: u64,
    /// Total bytes occupied by index metadata files.
    pub index_size_bytes: u64,
    /// Number of completed background compactions.
    pub num_compactions: u64,
    /// Total time spent in compaction, in milliseconds.
    pub compaction_time_ms: u64,
    /// Number of data files currently present.
    pub num_data_files: u64,
    /// Number of successful key lookups.
    pub num_positive_lookups: u64,
    /// Number of failed key lookups.
    pub num_negative_lookups: u64,
    /// Number of probes that had to inspect a second matching index entry.
    pub num_collisions: u64,
    /// Time spent in the most recent grow remap operation.
    pub last_remap_dur: Duration,
    /// Time spent in the most recent successful file compaction.
    pub last_compaction_dur: Duration,
    /// Bytes reclaimed by the most recent successful file compaction.
    pub last_compaction_reclaimed_bytes: u32,
    /// Bytes rewritten by the most recent successful file compaction.
    pub last_compaction_moved_bytes: u32,
    /// Number of read operations performed against data files.
    pub num_read_ops: u64,
    /// Total bytes read from data files.
    pub num_read_bytes: u64,
    /// Number of write operations performed against data files.
    pub num_write_ops: u64,
    /// Total bytes written to data files.
    pub num_write_bytes: u64,
    /// Number of entry creations recorded since open.
    pub num_created: u64,
    /// Number of entry removals recorded since open.
    pub num_removed: u64,
    /// Number of entry replacements recorded since open.
    pub num_replaced: u64,
    /// Total logical entry bytes written since open.
    pub written_bytes: u64,
    /// Total bytes currently occupied by live entries.
    pub data_bytes: u64,
    /// Total bytes currently accounted as unreclaimed waste.
    pub waste_bytes: u64,
    /// Approximate histogram bucket for entries under 64 bytes since open.
    pub entries_under_64: u64,
    /// Approximate histogram bucket for entries under 256 bytes since open.
    pub entries_under_256: u64,
    /// Approximate histogram bucket for entries under 1024 bytes since open.
    pub entries_under_1024: u64,
    /// Approximate histogram bucket for entries under 4096 bytes since open.
    pub entries_under_4096: u64,
    /// Approximate histogram bucket for entries under 16384 bytes since open.
    pub entries_under_16384: u64,
    /// Approximate histogram bucket for entries of 16384 bytes or larger since open.
    pub entries_over_16384: u64,
}

impl Stats {
    /// Returns the fraction of the current index capacity occupied by live entries.
    pub fn fill_level(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        self.num_items as f64 / self.capacity as f64
    }

    /// Returns the number of live entries.
    pub fn num_entries(&self) -> u64 {
        self.num_items
    }

    /// Returns the current unreclaimed waste in bytes.
    pub fn current_waste(&self) -> u64 {
        self.waste_bytes
    }

    /// Returns bytes currently occupied by live data.
    pub fn data_bytes(&self) -> u64 {
        self.data_bytes
    }

    /// Returns bytes currently occupied by live data.
    pub fn occupied_bytes(&self) -> u64 {
        self.data_bytes()
    }

    /// Returns current unreclaimed waste in bytes.
    pub fn wasted_bytes(&self) -> u64 {
        self.current_waste()
    }

    /// Returns the number of inserted entries.
    pub fn num_inserts(&self) -> u64 {
        self.num_created
    }

    /// Returns the number of updated entries.
    pub fn num_updates(&self) -> u64 {
        self.num_replaced
    }

    /// Returns the number of removed entries.
    pub fn num_removals(&self) -> u64 {
        self.num_removed
    }
}
