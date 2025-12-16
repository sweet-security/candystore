use siphasher::sip128::{Hasher128, SipHasher13};
use std::hash::Hasher;
use std::time::Duration;
use thiserror::Error;

pub(crate) const ROW_WIDTH: usize = 64 * 9;
pub(crate) const PAGE_SIZE: usize = 4096;
pub(crate) const ROW_SELECTOR_BITS: usize = 24;
pub(crate) const ROW_SELECTOR_SHIFT: usize = 32 - ROW_SELECTOR_BITS;
pub(crate) const ROW_SELECTOR_MASK: u32 = (1 << ROW_SELECTOR_BITS) - 1;
pub(crate) const SIZE_HINT_UNIT: u32 = 512;
pub(crate) const KEY_LEN_BITS: usize = 14;
pub const MAX_KEY_LEN: usize = (1 << KEY_LEN_BITS) - 1;
pub(crate) const KEY_LEN_MASK: u16 = MAX_KEY_LEN as u16;
pub(crate) const MAX_VALUE_LEN_INTERNAL: usize = u16::MAX as usize;
pub const MAX_VALUE_LEN: usize = MAX_VALUE_LEN_INTERNAL - 0xff;
pub(crate) const INDEX_FILE_MAGIC: &[u8; 8] = b"CANDYKV2";
pub(crate) const INDEX_FILE_VERSION: u32 = 1;
pub(crate) const DATA_FILE_MAGIC: &[u8; 8] = b"CANDYkv2";
pub(crate) const DATA_FILE_VERSION: u32 = 1;
pub(crate) const INVALID_FILE_ID: u16 = 0xffff;
pub(crate) const SPECIAL_FILE_ID_BASE: u16 = 0xff00;
pub(crate) const MAX_FILE_ID: u16 = SPECIAL_FILE_ID_BASE - 1;
pub(crate) const MAX_DATA_FILE_SIZE: u32 = 0xF000_0000; // reserve some space at the end for races

const _: () = assert!(MAX_KEY_LEN <= u16::MAX as usize);
const _: () = assert!(MAX_VALUE_LEN <= u16::MAX as usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Do not perform automatic recovery.
    FailIfCorrupted,
    /// Perform automatic recovery on startup.
    RebuildIndexIfCorrupted,
    /// Reset the database (delete all data) on corruption and start afresh.
    ClearAllIfCorrupted,
}

#[derive(Debug, Clone)]
/// Configuration for the CandyStore.
pub struct Config {
    /// The 128-bit key used for SipHash13.
    pub hash_key: (u64, u64),
    /// Initial number of rows in the index.
    pub initial_capacity: usize,
    /// Maximum size of a single data file in bytes.
    pub max_data_file_size: u32,
    /// Maximum number of concurrent operations (used for lock sharding).
    pub max_concurrency: usize,
    /// Factor for resizing the index (0 means default).
    pub remapping_scaler: u8,
    /// Whether to mlock the index file in memory.
    pub mlock_index: bool,
    /// Whether to automatically recover from corruption.
    pub recovery_mode: RecoveryMode,
    /// Interval for flushing data to disk. If None, flushing is disabled.
    pub flush_interval: Option<Duration>,
    /// Interval for compacting data files.
    pub compaction_interval: Duration,
    /// Only compact data files larger than this size (in bytes).
    pub compaction_min_file_size: u32,
    /// Threshold of wasted space ratio to trigger compaction.
    pub compaction_min_waste_threshold: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hash_key: (0xb047_a3ef_b334_9804, 0x807d_3135_878e_9b27),
            initial_capacity: 1024,
            max_data_file_size: 64 * 1024 * 1024,
            remapping_scaler: 0,
            max_concurrency: 16,
            mlock_index: true,
            recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
            flush_interval: Some(Duration::from_secs(10)),
            compaction_interval: Duration::from_secs(10),
            compaction_min_file_size: 8 * 1024 * 1024,
            compaction_min_waste_threshold: 0.3,
        }
    }
}

#[derive(Debug, Error)]
/// Errors that can occur in CandyStore operations.
pub enum CandyError {
    #[error("Internal: split row required")]
    SplitRow,
    #[error("Internal: current data file too large; new data file required")]
    RotateDataFile,
    #[error("Missing data file ID {0}")]
    MissingDataFile(u16),
    #[error("Max data files limit reached")]
    MaxDataFilesReached,
    #[error("Database directory is locked by {0}")]
    DatabaseLocked(String),
    #[error("Key too large: {0} bytes (max is 16383)")]
    KeyTooLarge(usize),
    #[error("Value too large: {0} bytes (max is 65535)")]
    ValueTooLarge(usize),
    #[error("Data corruption: {0}")]
    DataCorruption(String),
    #[error("IO error: {0}")]
    IOError(std::io::Error),
}

pub type Result<T> = std::result::Result<T, CandyError>;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyNamespace {
    _Invalid = 0,
    User = 1,
    Typed = 2,

    QueueMeta = 10,
    QueueData = 11,
    TypedQueueMeta = 12,
    TypedQueueData = 13,
    BigMeta = 14,
    BigData = 15,
    TypedBigMeta = 16,
    TypedBigData = 17,

    ListMeta = 20,
    ListIndex = 21,
    ListData = 22,
    TypedListMeta = 23,
    TypedListIndex = 24,
    TypedListData = 25,

    StatsWastedBytes = 253,
}

impl KeyNamespace {
    pub fn from_u8(v: u8) -> Self {
        match v {
            x if x == Self::User as u8 => Self::User,
            x if x == Self::Typed as u8 => Self::Typed,

            x if x == Self::QueueMeta as u8 => Self::QueueMeta,
            x if x == Self::QueueData as u8 => Self::QueueData,
            x if x == Self::TypedQueueMeta as u8 => Self::TypedQueueMeta,
            x if x == Self::TypedQueueData as u8 => Self::TypedQueueData,
            x if x == Self::BigMeta as u8 => Self::BigMeta,
            x if x == Self::BigData as u8 => Self::BigData,
            x if x == Self::TypedBigMeta as u8 => Self::TypedBigMeta,
            x if x == Self::TypedBigData as u8 => Self::TypedBigData,

            x if x == Self::ListMeta as u8 => Self::ListMeta,
            x if x == Self::ListIndex as u8 => Self::ListIndex,
            x if x == Self::ListData as u8 => Self::ListData,
            x if x == Self::TypedListMeta as u8 => Self::TypedListMeta,
            x if x == Self::TypedListIndex as u8 => Self::TypedListIndex,
            x if x == Self::TypedListData as u8 => Self::TypedListData,

            x if x == Self::StatsWastedBytes as u8 => Self::StatsWastedBytes,
            _ => Self::_Invalid,
        }
    }

    pub fn is_data_entry(&self) -> bool {
        match self {
            Self::_Invalid => false,
            Self::User => true,
            Self::Typed => true,

            Self::QueueMeta => true,
            Self::QueueData => true,
            Self::TypedQueueMeta => true,
            Self::TypedQueueData => true,
            Self::BigMeta => true,
            Self::BigData => true,
            Self::TypedBigMeta => true,
            Self::TypedBigData => true,

            Self::ListMeta => true,
            Self::ListIndex => true,
            Self::ListData => true,
            Self::TypedListMeta => true,
            Self::TypedListIndex => true,
            Self::TypedListData => true,

            Self::StatsWastedBytes => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HashCoordinates {
    pub row_selector: u32,
    pub signature: u32,
}

impl HashCoordinates {
    pub const INVALID_SIG: u32 = 0;

    pub fn from_key<B: AsRef<[u8]> + ?Sized>(k1: u64, k2: u64, ns: KeyNamespace, key: &B) -> Self {
        let key = key.as_ref();
        let mut hasher = SipHasher13::new_with_keys(k1, k2);
        hasher.write_u8(ns as u8);
        hasher.write(key);
        let h = hasher.finish128();
        let row_selector = (h.h1 as u32) & ROW_SELECTOR_MASK;
        let mut signature = (h.h1 >> 32) as u32;
        if signature == Self::INVALID_SIG {
            signature = h.h2 as u32;
            if signature == Self::INVALID_SIG {
                signature = (h.h2 >> 32) as u32;
                if signature == Self::INVALID_SIG {
                    signature = 0x2f0b_ab83; // just pick a non-zero value
                }
            }
        }

        Self {
            row_selector,
            signature,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum SpecialEntryType {
    WastedBytes = 1,
}

#[derive(Clone, Copy, Debug)]
#[repr(C, packed)]
pub(crate) struct EntryPointer {
    pub file_id: u16,
    pub file_offset: u32,
    pub _row_selector_and_size_hint: u32,
}

impl EntryPointer {
    pub const INVALID_PTR: Self = Self {
        file_id: INVALID_FILE_ID,
        file_offset: 0,
        _row_selector_and_size_hint: 0,
    };

    pub fn new(file_id: u16, file_offset: u32, row_selector: u32, size_hint: u32) -> Self {
        assert!(file_id < SPECIAL_FILE_ID_BASE);
        assert!(row_selector <= ROW_SELECTOR_MASK);
        assert!(size_hint < u8::MAX as u32 * SIZE_HINT_UNIT);
        let size_hint_units = size_hint.div_ceil(SIZE_HINT_UNIT);
        Self {
            file_id,
            file_offset,
            _row_selector_and_size_hint: (row_selector << ROW_SELECTOR_SHIFT) | size_hint_units,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.file_id != INVALID_FILE_ID
    }
    pub fn is_data_pointer(&self) -> bool {
        self.file_id < SPECIAL_FILE_ID_BASE
    }

    #[inline]
    pub fn row_selector(&self) -> u32 {
        self._row_selector_and_size_hint >> ROW_SELECTOR_SHIFT
    }
    #[inline]
    pub fn size_hint(&self) -> u32 {
        (self._row_selector_and_size_hint & ((1 << ROW_SELECTOR_SHIFT) - 1)) * SIZE_HINT_UNIT
    }

    pub fn new_special(
        special_type: SpecialEntryType,
        special_value: u64,
        row_selector: u32,
    ) -> Self {
        let file_id = SPECIAL_FILE_ID_BASE + special_type as u16;
        assert!(file_id >= SPECIAL_FILE_ID_BASE && file_id != INVALID_FILE_ID);
        // We have 40 bits for value (32 in file_offset + 8 in size_hint part)
        // row_selector takes the top 24 bits of _row_selector_and_size_hint
        let val_low = special_value as u32;
        let val_high = (special_value >> 32) as u32;
        debug_assert!(val_high < (1 << ROW_SELECTOR_SHIFT));

        Self {
            file_id,
            file_offset: val_low,
            _row_selector_and_size_hint: (row_selector << ROW_SELECTOR_SHIFT) | val_high,
        }
    }

    pub fn is_special_value(&self) -> bool {
        self.file_id >= SPECIAL_FILE_ID_BASE && self.file_id != INVALID_FILE_ID
    }
    pub fn get_special_value(&self) -> Option<(u8, u64)> {
        if self.is_special_value() {
            let val_low = self.file_offset as u64;
            let val_high =
                (self._row_selector_and_size_hint & ((1 << ROW_SELECTOR_SHIFT) - 1)) as u64;
            Some((
                (self.file_id - SPECIAL_FILE_ID_BASE) as u8,
                val_low | (val_high << 32),
            ))
        } else {
            None
        }
    }

    #[inline]
    pub fn calc_checksum(&self, sig: u32) -> u64 {
        (((self._row_selector_and_size_hint as u64) << 32) | (sig as u64)).wrapping_mul(
            0xe5a8_0000_0000_0000 | (((self.file_id as u64) << 32) | self.file_offset as u64),
        )
    }
}

#[derive(Clone, Copy)]
pub(crate) struct QueueNamespaces {
    pub meta: KeyNamespace,
    pub data: KeyNamespace,
}

pub(crate) const QUEUE_NS: QueueNamespaces = QueueNamespaces {
    meta: KeyNamespace::QueueMeta,
    data: KeyNamespace::QueueData,
};

pub(crate) const TYPED_QUEUE_NS: QueueNamespaces = QueueNamespaces {
    meta: KeyNamespace::TypedQueueMeta,
    data: KeyNamespace::TypedQueueData,
};

pub(crate) const BIG_NS: QueueNamespaces = QueueNamespaces {
    meta: KeyNamespace::BigMeta,
    data: KeyNamespace::BigData,
};

pub(crate) const TYPED_BIG_NS: QueueNamespaces = QueueNamespaces {
    meta: KeyNamespace::TypedBigMeta,
    data: KeyNamespace::TypedBigData,
};

#[derive(Clone, Copy)]
pub(crate) struct ListNamespaces {
    pub meta: KeyNamespace,
    pub index: KeyNamespace,
    pub data: KeyNamespace,
}

pub(crate) const LIST_NS: ListNamespaces = ListNamespaces {
    meta: KeyNamespace::ListMeta,
    index: KeyNamespace::ListIndex,
    data: KeyNamespace::ListData,
};

pub(crate) const TYPED_LIST_NS: ListNamespaces = ListNamespaces {
    meta: KeyNamespace::TypedListMeta,
    index: KeyNamespace::TypedListIndex,
    data: KeyNamespace::TypedListData,
};

#[derive(Debug, Clone)]
/// Statistics about the CandyStore.
pub struct Stats {
    /// Number of rows in the index.
    pub num_rows: usize,
    /// Number of compactions performed.
    pub num_compactions: usize,
    /// Number of active data files.
    pub num_data_files: usize,

    /// Total bytes occupied by valid data.
    pub occupied_bytes: usize,
    /// Total bytes wasted (e.g., by overwritten or deleted data).
    pub wasted_bytes: usize,

    /// Number of insert operations.
    pub num_inserts: usize,
    /// Number of update operations.
    pub num_updates: usize,
    /// Number of successful lookups.
    pub num_positive_lookups: usize,
    /// Number of failed lookups.
    pub num_negative_lookups: usize,
    /// Number of removal operations.
    pub num_removals: usize,

    /// Number of read operations from disk.
    pub num_read_ops: usize,
    /// Number of bytes read from disk.
    pub num_read_bytes: usize,
    /// Number of write operations to disk.
    pub num_write_ops: usize,
    /// Number of bytes written to disk.
    pub num_write_bytes: usize,

    /// Number of entries smaller than 128 bytes.
    pub entries_under_128: usize,
    /// Number of entries smaller than 1KB.
    pub entries_under_1k: usize,
    /// Number of entries smaller than 8KB.
    pub entries_under_8k: usize,
    /// Number of entries smaller than 32KB.
    pub entries_under_32k: usize,
    /// Number of entries larger than 32KB.
    pub entries_over_32k: usize,
}
