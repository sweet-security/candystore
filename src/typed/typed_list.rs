use std::{borrow::Borrow, marker::PhantomData, ops::Range};

use databuf::{DecodeOwned, Encode, config::num::LE};

use crate::{ListCompactionParams, types::TYPED_LIST_NS};

use super::{CandyTypedKey, StoreRef, append_type_id, decode_from_bytes};

/// A typed wrapper around CandyStore's list functionality.
///
/// This struct provides a type-safe interface for managing lists of key-value pairs.
/// Lists are identified by a list key (L), and contain items identified by an item key (K) with a value (V).
pub struct CandyTypedList<L, K, V> {
    store: StoreRef,
    _phantom: PhantomData<(L, K, V)>,
}

impl<L, K, V> Clone for CandyTypedList<L, K, V> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            _phantom: Default::default(),
        }
    }
}

impl<L, K, V> CandyTypedList<L, K, V>
where
    L: CandyTypedKey + Encode,
    K: Encode + DecodeOwned,
    V: Encode + DecodeOwned,
{
    /// Creates a new `CandyTypedList` wrapping the given `CandyStore`.
    pub fn new(store: StoreRef) -> Self {
        Self {
            store,
            _phantom: Default::default(),
        }
    }

    fn make_list_key<Q: ?Sized + Encode>(list_key: &Q) -> Vec<u8>
    where
        L: Borrow<Q>,
    {
        let lbytes = list_key.to_bytes::<LE>();
        append_type_id(lbytes, L::TYPE_ID)
    }

    fn make_item_key<Q: ?Sized + Encode>(item_key: &Q) -> Vec<u8>
    where
        K: Borrow<Q>,
    {
        item_key.to_bytes::<LE>()
    }

    /// Checks if the list contains the given item.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item within the list.
    ///
    /// # Returns
    ///
    /// `true` if the item exists, `false` otherwise.
    pub fn contains<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> crate::Result<bool>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        self.get(list_key, item_key).map(|v| v.is_some())
    }

    /// Sets an item in the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    /// * `val` - The value to store.
    ///
    /// # Returns
    ///
    /// The previous value if it existed, or `None` if it was a new item.
    pub fn set<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        val: &Q3,
    ) -> crate::Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;
        match self
            .store
            .list_set_at_tail_with_ns(TYPED_LIST_NS, &lkey, &ikey, &vbytes)?
        {
            Some(prev) => Ok(Some(decode_from_bytes::<V>(&prev)?)),
            None => Ok(None),
        }
    }

    /// Retrieves an item from the list, or creates it if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    /// * `default_val` - The value to set if the item does not exist.
    ///
    /// # Returns
    ///
    /// The value of the item (either existing or newly created).
    pub fn get_or_create<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        default_val: &Q3,
    ) -> crate::Result<V>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = default_val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;

        let res = self
            .store
            .list_get_or_create_with_ns(TYPED_LIST_NS, &lkey, &ikey, &vbytes)?;

        match res {
            crate::GetOrCreateStatus::ExistingValue(v) => decode_from_bytes::<V>(&v),
            crate::GetOrCreateStatus::CreatedNew(v) => decode_from_bytes::<V>(&v),
        }
    }

    /// Replaces an item in the list only if it matches an expected value.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    /// * `val` - The new value.
    /// * `expected_val` - If provided, replacement only happens if the current value matches this.
    ///
    /// # Returns
    ///
    /// The previous value if replaced, or `None` if the operation failed (wrong value or missing key).
    pub fn replace<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        val: &Q3,
        expected_val: Option<&Q3>,
    ) -> crate::Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = val.to_bytes::<LE>();
        let expected_bytes = expected_val.map(|v| v.to_bytes::<LE>());

        self.store.ensure_user_value_len(vbytes.len())?;

        let res = self.store.list_replace_with_ns(
            TYPED_LIST_NS,
            &lkey,
            &ikey,
            &vbytes,
            expected_bytes.as_deref(),
        )?;

        match res {
            crate::ReplaceStatus::PrevValue(prev) => Ok(Some(decode_from_bytes::<V>(&prev)?)),
            crate::ReplaceStatus::WrongValue(_) => Ok(None),
            crate::ReplaceStatus::DoesNotExist => Ok(None),
        }
    }

    /// Retrieves an item from the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if it does not exist.
    pub fn get<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> crate::Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        match self.store.list_get_with_ns(TYPED_LIST_NS, &lkey, &ikey)? {
            Some(v) => Ok(Some(decode_from_bytes::<V>(&v)?)),
            None => Ok(None),
        }
    }

    /// Removes an item from the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    ///
    /// # Returns
    ///
    /// The value of the removed item, or `None` if it did not exist.
    pub fn remove<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> crate::Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        match self
            .store
            .list_remove_with_ns(TYPED_LIST_NS, &lkey, &ikey)?
        {
            Some(v) => Ok(Some(decode_from_bytes::<V>(&v)?)),
            None => Ok(None),
        }
    }

    /// Returns the number of items in the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The number of items in the list.
    pub fn len<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<usize>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store.list_len_with_ns(TYPED_LIST_NS, &lkey)
    }

    /// Returns the range of internal indices occupied by the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The range of internal indices occupied by the list.
    pub fn range<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<Range<usize>>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store.list_range_with_ns(TYPED_LIST_NS, &lkey)
    }

    /// Checks if the list is empty.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// `true` if the list is empty, `false` otherwise.
    pub fn is_empty<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<bool>
    where
        L: Borrow<Q>,
    {
        self.len(list_key).map(|len| len == 0)
    }

    /// Removes all items from the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// `true` if the list existed and was removed, `false` otherwise.
    pub fn discard<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<bool>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store.list_discard_with_ns(TYPED_LIST_NS, &lkey)
    }

    /// Compacts the list if it meets the criteria specified in `params`.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `params` - Parameters controlling when compaction should occur.
    ///
    /// # Returns
    ///
    /// `true` if compaction was performed, `false` otherwise.
    pub fn compact_if_needed<Q: ?Sized + Encode>(
        &self,
        list_key: &Q,
        params: ListCompactionParams,
    ) -> crate::Result<bool>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store
            .list_compact_with_ns(TYPED_LIST_NS, &lkey, params)
    }

    /// Promotes an item to the head of the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    ///
    /// # Returns
    ///
    /// `true` if the item was promoted, `false` otherwise.
    pub fn set_promoting<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> crate::Result<bool>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        self.store.list_promote_with_ns(TYPED_LIST_NS, &lkey, &ikey)
    }

    /// Returns an iterator over the items in the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// An iterator over the items in the list.
    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        list_key: &Q,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(K, V)>> + 'a
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store
            .list_iter_with_ns(TYPED_LIST_NS, &lkey)
            .map(|res| {
                res.and_then(|(k, v)| {
                    Ok((decode_from_bytes::<K>(&k)?, decode_from_bytes::<V>(&v)?))
                })
            })
    }

    /// Removes and returns the item at the tail of the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the tail of the list, or `None` if the list is empty.
    pub fn pop_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        if let Some((k, v)) = self.store.pop_list_tail_with_ns(TYPED_LIST_NS, &lkey)? {
            return Ok(Some((
                decode_from_bytes::<K>(&k)?,
                decode_from_bytes::<V>(&v)?,
            )));
        }
        Ok(None)
    }

    /// Removes and returns the item at the head of the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the head of the list, or `None` if the list is empty.
    pub fn pop_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        if let Some((k, v)) = self.store.pop_list_head_with_ns(TYPED_LIST_NS, &lkey)? {
            return Ok(Some((
                decode_from_bytes::<K>(&k)?,
                decode_from_bytes::<V>(&v)?,
            )));
        }
        Ok(None)
    }

    /// Returns the item at the tail of the list without removing it.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the tail of the list, or `None` if the list is empty.
    pub fn peek_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let mut iter = self.iter(list_key);
        match iter.next_back() {
            Some(Ok(kv)) => Ok(Some(kv)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Returns the item at the head of the list without removing it.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the head of the list, or `None` if the list is empty.
    pub fn peek_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> crate::Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        match self.iter(list_key).next() {
            Some(Ok(kv)) => Ok(Some(kv)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `func` - The predicate function.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    pub fn retain<Q: ?Sized + Encode>(
        &self,
        list_key: &Q,
        mut func: impl FnMut(&K, &V) -> crate::Result<bool>,
    ) -> crate::Result<()>
    where
        L: Borrow<Q>,
    {
        self.store.list_retain_with_ns(
            TYPED_LIST_NS,
            &Self::make_list_key(list_key),
            |k_bytes, v_bytes| {
                let k = decode_from_bytes::<K>(k_bytes)?;
                let v = decode_from_bytes::<V>(v_bytes)?;
                func(&k, &v)
            },
        )
    }
}
