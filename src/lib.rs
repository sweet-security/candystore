mod data_file;
mod index_file;
mod internal;
mod pacer;
mod store;
mod types;

/// Named crash point for whitebox testing.
///
/// When the `whitebox-testing` feature is enabled and the environment variable
/// `CANDYSTORE_CRASH_POINT` matches `name`, the process aborts after the number
/// of hits specified by `CANDYSTORE_CRASH_AFTER` (default 0 = immediate).
#[cfg(feature = "whitebox-testing")]
pub(crate) fn crash_point(name: &str) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let Ok(target) = std::env::var("CANDYSTORE_CRASH_POINT") else {
        return;
    };
    if target != name {
        return;
    }
    let after: u64 = std::env::var("CANDYSTORE_CRASH_AFTER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if COUNTER.fetch_add(1, Ordering::Relaxed) >= after {
        std::process::abort();
    }
}

#[cfg(not(feature = "whitebox-testing"))]
#[inline(always)]
pub(crate) fn crash_point(_name: &str) {}

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
