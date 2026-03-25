mod data_file;
mod index_file;
mod internal;
mod pacer;
mod store;
mod types;

/// The main untyped store API.
pub use crate::store::{
    CandyStore, CandyTypedDeque, CandyTypedKey, CandyTypedList, CandyTypedStore, KVPair,
    ListIterator,
};
/// Public configuration, error, and stats types.
pub use crate::types::*;

/// Backward-compatible alias for the crate error type.
pub type CandyError = Error;
/// Maximum supported user key length in bytes.
pub const MAX_KEY_LEN: usize = MAX_USER_KEY_SIZE;
/// Maximum supported inline value length in bytes.
pub const MAX_VALUE_LEN: usize = MAX_USER_VALUE_SIZE;
