use std::{borrow::Borrow, marker::PhantomData};

use databuf::{DecodeOwned, Encode, config::num::LE};

use crate::{
    store::ops::SetOptions,
    types::{KeyNamespace, TYPED_BIG_NS},
};

use super::{CandyTypedKey, StoreRef, append_type_id, decode_from_bytes};

/// A typed wrapper around CandyStore.
///
/// This struct provides a type-safe interface for storing and retrieving values.
/// Keys and values are automatically serialized and deserialized using `databuf`.
pub struct CandyTypedStore<K, V> {
    store: StoreRef,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Clone for CandyTypedStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            _phantom: Default::default(),
        }
    }
}

impl<K, V> CandyTypedStore<K, V>
where
    K: CandyTypedKey + Encode,
    V: Encode + DecodeOwned,
{
    /// Creates a new `CandyTypedStore` wrapping the given `CandyStore`.
    pub fn new(store: StoreRef) -> Self {
        Self {
            store,
            _phantom: Default::default(),
        }
    }

    fn make_key<Q: ?Sized + Encode>(key: &Q) -> Vec<u8>
    where
        K: Borrow<Q>,
    {
        let kbytes = key.to_bytes::<LE>();
        append_type_id(kbytes, K::TYPE_ID)
    }

    /// Retrieves the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// The value associated with the key, or `None` if the key does not exist.
    pub fn get<Q: ?Sized + Encode>(&self, key: &Q) -> crate::Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(key);
        match self.store._get(KeyNamespace::Typed, &kbytes)? {
            Some(v) => Ok(Some(decode_from_bytes::<V>(&v)?)),
            None => Ok(None),
        }
    }

    /// Sets the value for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set.
    /// * `val` - The value to store.
    ///
    /// # Returns
    ///
    /// The previous value if it existed, or `None` if it was a new key.
    pub fn set<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> crate::Result<Option<V>>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        let vbytes = val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;
        match self.store._set(KeyNamespace::Typed, &kbytes, &vbytes)? {
            Some(prev) => Ok(Some(decode_from_bytes::<V>(&prev)?)),
            None => Ok(None),
        }
    }

    /// Removes the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// The value of the removed key, or `None` if the key did not exist.
    pub fn remove<Q: ?Sized + Encode>(&self, key: &Q) -> crate::Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(key);
        match self.store._remove(KeyNamespace::Typed, &kbytes)? {
            Some(prev) => Ok(Some(decode_from_bytes::<V>(&prev)?)),
            None => Ok(None),
        }
    }

    /// Checks if the store contains the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check.
    ///
    /// # Returns
    ///
    /// `true` if the key exists, `false` otherwise.
    pub fn contains<Q: ?Sized + Encode>(&self, key: &Q) -> crate::Result<bool>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(key);
        self.store
            ._get(KeyNamespace::Typed, &kbytes)
            .map(|v| v.is_some())
    }

    /// Atomically fetch the current value or create it if absent.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up or create.
    /// * `val` - The value to set if the key does not exist.
    ///
    /// # Returns
    ///
    /// The value of the key (either existing or newly created).
    pub fn get_or_create<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> crate::Result<V>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        let vbytes = val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;
        let outcome = self.store._set_with_options(
            KeyNamespace::Typed,
            &kbytes,
            &vbytes,
            SetOptions::InsertIfVacant,
        )?;

        if let Some(existing) = outcome.previous {
            Ok(decode_from_bytes::<V>(&existing)?)
        } else {
            Ok(decode_from_bytes::<V>(&vbytes)?)
        }
    }

    /// Atomically replace the value only if the key already exists.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to replace.
    /// * `val` - The new value.
    /// * `expected_val` - If provided, the replacement only happens if the current value matches this.
    ///
    /// # Returns
    ///
    /// The previous value if replaced, or `None` if the operation failed (wrong value or missing key).
    pub fn replace<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
        expected_val: Option<&Q2>,
    ) -> crate::Result<Option<V>>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        let vbytes = val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;

        let expected_bytes = expected_val.map(|v| v.to_bytes::<LE>());

        let outcome = self.store._set_with_options(
            KeyNamespace::Typed,
            &kbytes,
            &vbytes,
            SetOptions::ReplaceIfExists(expected_bytes),
        )?;

        if outcome.wrong_value {
            return Ok(None);
        }

        outcome
            .previous
            .map(|prev| decode_from_bytes::<V>(&prev))
            .transpose()
    }

    /// Sets a value that might be larger than the standard limit by splitting it into chunks.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set.
    /// * `val` - The large value.
    ///
    /// # Returns
    ///
    /// `true` if the key already existed and was overwritten, `false` otherwise.
    pub fn set_big<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> crate::Result<bool>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        let vbytes = val.to_bytes::<LE>();
        self.store
            .queue_set_big_with_ns(TYPED_BIG_NS, &kbytes, &vbytes)
    }

    /// Retrieves a large value that was stored using `set_big`.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve.
    ///
    /// # Returns
    ///
    /// The large value, or `None` if the key does not exist.
    pub fn get_big<Q: ?Sized + Encode>(&self, key: &Q) -> crate::Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(key);
        self.store
            .queue_get_big_with_ns(TYPED_BIG_NS, &kbytes)?
            .map(|bytes| decode_from_bytes::<V>(&bytes))
            .transpose()
    }

    /// Removes a large value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// `true` if the key existed and was removed, `false` otherwise.
    pub fn remove_big<Q: ?Sized + Encode>(&self, key: &Q) -> crate::Result<bool>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(key);
        self.store.queue_discard_with_ns(TYPED_BIG_NS, &kbytes)
    }
}
