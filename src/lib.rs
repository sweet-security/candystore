mod containers;
pub(crate) mod files;
mod store;
mod typed;
pub(crate) mod types;

pub use containers::list::{KVPair, ListCompactionParams, ListIterator};
pub use store::{CandyStore, GetOrCreateStatus, ReplaceStatus, SetStatus};
pub use typed::{CandyTypedDeque, CandyTypedKey, CandyTypedList, CandyTypedStore};
pub use types::{
    CandyError, Config, MAX_KEY_LEN, MAX_VALUE_LEN, OverwriteMode, RecoveryMode, Result, Stats,
};

#[doc(hidden)]
pub mod internal {
    pub use crate::files::data_file::test_offsets::*;
    pub use crate::files::data_file::{read_at, read_exact_at, write_all_at};
    pub use crate::files::index_file::test_offsets::*;
}

#[cfg(test)]
mod tests;
