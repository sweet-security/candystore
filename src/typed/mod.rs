use std::sync::Arc;

use databuf::{DecodeOwned, Encode, config::num::LE};

use crate::{
    store::CandyStore,
    types::{CandyError, Result},
};

pub trait CandyTypedKey: Encode + DecodeOwned {
    /// Stable type identifier used to avoid cross-type collisions.
    const TYPE_ID: u32;
}

macro_rules! typed_builtin {
    ($t:ty, $v:literal) => {
        impl CandyTypedKey for $t {
            const TYPE_ID: u32 = $v;
        }
    };
}

typed_builtin!(u8, 1);
typed_builtin!(u16, 2);
typed_builtin!(u32, 3);
typed_builtin!(u64, 4);
typed_builtin!(u128, 5);
typed_builtin!(i8, 6);
typed_builtin!(i16, 7);
typed_builtin!(i32, 8);
typed_builtin!(i64, 9);
typed_builtin!(i128, 10);
typed_builtin!(bool, 11);
typed_builtin!(usize, 12);
typed_builtin!(isize, 13);
typed_builtin!(char, 14);
typed_builtin!(String, 15);
typed_builtin!(Vec<u8>, 16);
typed_builtin!(uuid::Bytes, 17);

pub(crate) type StoreRef = Arc<CandyStore>;

pub(crate) fn decode_from_bytes<T: DecodeOwned>(bytes: &[u8]) -> Result<T> {
    T::from_bytes::<LE>(bytes).map_err(|e| CandyError::DataCorruption(format!("decode error: {e}")))
}

pub(crate) fn append_type_id(mut bytes: Vec<u8>, type_id: u32) -> Vec<u8> {
    bytes.extend_from_slice(&type_id.to_le_bytes());
    bytes
}

mod typed_list;
mod typed_queue;
mod typed_store;

pub use typed_list::CandyTypedList;
pub use typed_queue::CandyTypedDeque;
pub use typed_store::CandyTypedStore;
