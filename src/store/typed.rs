use std::{borrow::Borrow, marker::PhantomData, ops::Range, sync::Arc};

use databuf::{DecodeOwned, Encode, config::num::LE};
use smallvec::SmallVec;

use crate::{
    internal::KeyNamespace,
    store::CandyStore,
    types::{Error, ListCompactionParams, Result},
};

#[derive(Clone, Copy)]
struct TypedBigNamespaces {
    meta: KeyNamespace,
    data: KeyNamespace,
}

const TYPED_BIG_NS: TypedBigNamespaces = TypedBigNamespaces {
    meta: KeyNamespace::TypedBigMeta,
    data: KeyNamespace::TypedBigData,
};

const TYPED_QUEUE_NS: super::queue::QueueNamespaces = super::queue::QueueNamespaces {
    meta: KeyNamespace::TypedQueueMeta,
    data: KeyNamespace::TypedQueueData,
};

const TYPED_LIST_NS: super::list::ListNamespaces = super::list::ListNamespaces {
    meta: KeyNamespace::TypedListMeta,
    index: KeyNamespace::TypedListIndex,
    data: KeyNamespace::TypedListData,
};

const INLINE_TYPED_BUF_SIZE: usize = 128;

type InlineBytes = SmallVec<[u8; INLINE_TYPED_BUF_SIZE]>;

/// Marker trait for typed keys and collection identifiers used by the typed wrappers.
pub trait CandyTypedKey: Encode + DecodeOwned {
    const TYPE_ID: u32;
}

macro_rules! typed_builtin {
    ($ty:ty, $type_id:literal) => {
        impl CandyTypedKey for $ty {
            const TYPE_ID: u32 = $type_id;
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

/// Typed wrapper over the store key-value API.
pub struct CandyTypedStore<K, V> {
    store: Arc<CandyStore>,
    _phantom: PhantomData<(K, V)>,
}

/// Typed wrapper over the queue API.
pub struct CandyTypedDeque<L, V> {
    store: Arc<CandyStore>,
    _phantom: PhantomData<(L, V)>,
}

/// Typed wrapper over the ordered map/list API.
pub struct CandyTypedList<L, K, V> {
    store: Arc<CandyStore>,
    _phantom: PhantomData<(L, K, V)>,
}

impl<L, V> Clone for CandyTypedDeque<L, V> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> Clone for CandyTypedStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            _phantom: PhantomData,
        }
    }
}

impl<L, K, V> Clone for CandyTypedList<L, K, V> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> CandyTypedStore<K, V>
where
    K: CandyTypedKey + Encode,
    V: Encode + DecodeOwned,
{
    /// Creates a typed key-value view over `store`.
    pub fn new(store: Arc<CandyStore>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn make_key<Q: ?Sized + Encode>(key: &Q) -> InlineBytes
    where
        K: Borrow<Q>,
    {
        append_type_id(encode_to_smallvec(key), K::TYPE_ID)
    }

    /// Returns the decoded value for `key`, if present.
    pub fn get<Q: ?Sized + Encode>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let key_bytes = Self::make_key(key);
        self.store
            .get_ns(KeyNamespace::Typed, &key_bytes)?
            .map(|bytes| decode_from_bytes::<V>(&bytes))
            .transpose()
    }

    /// Inserts or replaces `key` with `val`.
    pub fn set<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> Result<Option<V>>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let key_bytes = Self::make_key(key);
        let value_bytes = encode_to_smallvec(val);
        self.store
            .set_ns(KeyNamespace::Typed, &key_bytes, &value_bytes)?
            .map(|prev| decode_from_bytes::<V>(&prev))
            .transpose()
    }

    /// Removes `key` and returns its previous decoded value if it existed.
    pub fn remove<Q: ?Sized + Encode>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let key_bytes = Self::make_key(key);
        self.store
            .remove_ns(KeyNamespace::Typed, &key_bytes)?
            .map(|prev| decode_from_bytes::<V>(&prev))
            .transpose()
    }

    /// Returns `true` if `key` currently exists.
    pub fn contains<Q: ?Sized + Encode>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
    {
        let key_bytes = Self::make_key(key);
        self.store
            .get_ns(KeyNamespace::Typed, &key_bytes)
            .map(|value| value.is_some())
    }

    /// Returns the current value for `key`, or inserts and returns `val` if the key is missing.
    pub fn get_or_create<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> Result<V>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let key_bytes = Self::make_key(key);
        let value_bytes = encode_to_smallvec(val);
        let status = self
            .store
            .get_or_create_ns(KeyNamespace::Typed, &key_bytes, &value_bytes)?;
        match status {
            crate::GetOrCreateStatus::ExistingValue(value)
            | crate::GetOrCreateStatus::CreatedNew(value) => decode_from_bytes::<V>(&value),
        }
    }

    /// Replaces `key` with `val` only if the current value matches `expected_val` when provided.
    pub fn replace<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
        expected_val: Option<&Q3>,
    ) -> Result<Option<V>>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let key_bytes = Self::make_key(key);
        let value_bytes = encode_to_smallvec(val);
        let expected_bytes = expected_val.map(encode_to_smallvec);
        match self.store.replace_ns(
            KeyNamespace::Typed,
            &key_bytes,
            &value_bytes,
            expected_bytes.as_deref(),
        )? {
            crate::ReplaceStatus::PrevValue(prev) => decode_from_bytes::<V>(&prev).map(Some),
            crate::ReplaceStatus::WrongValue(_) | crate::ReplaceStatus::DoesNotExist => Ok(None),
        }
    }

    /// Stores a large typed value under `key`.
    pub fn set_big<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> Result<bool>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let key_bytes = Self::make_key(key);
        let value_bytes = encode_to_smallvec(val);
        self.store.queue_set_big_with_ns(
            super::queue::QueueNamespaces {
                meta: TYPED_BIG_NS.meta,
                data: TYPED_BIG_NS.data,
            },
            &key_bytes,
            &value_bytes,
        )
    }

    /// Loads a large typed value previously stored with [`CandyTypedStore::set_big`].
    pub fn get_big<Q: ?Sized + Encode>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let key_bytes = Self::make_key(key);
        self.store
            .queue_get_big_with_ns(
                super::queue::QueueNamespaces {
                    meta: TYPED_BIG_NS.meta,
                    data: TYPED_BIG_NS.data,
                },
                &key_bytes,
            )?
            .map(|value| decode_from_bytes::<V>(&value))
            .transpose()
    }

    /// Removes a large typed value previously stored with [`CandyTypedStore::set_big`].
    pub fn remove_big<Q: ?Sized + Encode>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
    {
        let key_bytes = Self::make_key(key);
        self.store.queue_discard_with_ns(
            super::queue::QueueNamespaces {
                meta: TYPED_BIG_NS.meta,
                data: TYPED_BIG_NS.data,
            },
            &key_bytes,
        )
    }
}

impl<L, V> CandyTypedDeque<L, V>
where
    L: CandyTypedKey + Encode,
    V: Encode + DecodeOwned,
{
    /// Creates a typed queue view over `store`.
    pub fn new(store: Arc<CandyStore>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn make_queue_key<Q: ?Sized + Encode>(queue_key: &Q) -> InlineBytes
    where
        L: Borrow<Q>,
    {
        append_type_id(encode_to_smallvec(queue_key), L::TYPE_ID)
    }

    /// Pushes `val` to the tail of `queue_key`.
    pub fn push_tail<Q: ?Sized + Encode, QV: ?Sized + Encode>(
        &self,
        queue_key: &Q,
        val: &QV,
    ) -> Result<()>
    where
        L: Borrow<Q>,
        V: Borrow<QV>,
    {
        let qkey = Self::make_queue_key(queue_key);
        let vbytes = encode_to_smallvec(val);
        self.store
            .queue_push_tail_with_ns(TYPED_QUEUE_NS, &qkey, &vbytes)
            .map(|_| ())
    }

    /// Pushes `val` to the head of `queue_key`.
    pub fn push_head<Q: ?Sized + Encode, QV: ?Sized + Encode>(
        &self,
        queue_key: &Q,
        val: &QV,
    ) -> Result<()>
    where
        L: Borrow<Q>,
        V: Borrow<QV>,
    {
        let qkey = Self::make_queue_key(queue_key);
        let vbytes = encode_to_smallvec(val);
        self.store
            .queue_push_head_with_ns(TYPED_QUEUE_NS, &qkey, &vbytes)
            .map(|_| ())
    }

    /// Removes and returns the head item of `queue_key` together with its logical index.
    pub fn pop_head_with_idx<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_pop_head_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, value)) => {
                decode_from_bytes::<V>(&value).map(|value| Some((idx as usize, value)))
            }
            None => Ok(None),
        }
    }

    /// Removes and returns the head value of `queue_key`.
    pub fn pop_head<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.pop_head_with_idx(queue_key)?.map(|(_, value)| value))
    }

    /// Removes and returns the tail item of `queue_key` together with its logical index.
    pub fn pop_tail_with_idx<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_pop_tail_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, value)) => {
                decode_from_bytes::<V>(&value).map(|value| Some((idx as usize, value)))
            }
            None => Ok(None),
        }
    }

    /// Removes and returns the tail value of `queue_key`.
    pub fn pop_tail<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.pop_tail_with_idx(queue_key)?.map(|(_, value)| value))
    }

    /// Returns the head item of `queue_key` and its logical index without removing it.
    pub fn peek_head_with_idx<Q: ?Sized + Encode>(
        &self,
        queue_key: &Q,
    ) -> Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_peek_head_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, value)) => {
                decode_from_bytes::<V>(&value).map(|value| Some((idx as usize, value)))
            }
            None => Ok(None),
        }
    }

    /// Returns the head value of `queue_key` without removing it.
    pub fn peek_head<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.peek_head_with_idx(queue_key)?.map(|(_, value)| value))
    }

    /// Returns the tail item of `queue_key` and its logical index without removing it.
    pub fn peek_tail_with_idx<Q: ?Sized + Encode>(
        &self,
        queue_key: &Q,
    ) -> Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_peek_tail_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, value)) => {
                decode_from_bytes::<V>(&value).map(|value| Some((idx as usize, value)))
            }
            None => Ok(None),
        }
    }

    /// Returns the tail value of `queue_key` without removing it.
    pub fn peek_tail<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.peek_tail_with_idx(queue_key)?.map(|(_, value)| value))
    }

    /// Returns the number of live items in `queue_key`.
    pub fn len<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<usize>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store
            .queue_len_with_ns(TYPED_QUEUE_NS, &qkey)
            .map(|len| len as usize)
    }

    /// Returns the current inclusive-exclusive logical index span for `queue_key`.
    pub fn range<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<Range<usize>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store.queue_range_with_ns(TYPED_QUEUE_NS, &qkey)
    }

    /// Returns `true` when `queue_key` has no live items.
    pub fn is_empty<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<bool>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store
            .queue_len_with_ns(TYPED_QUEUE_NS, &qkey)
            .map(|len| len == 0)
    }

    /// Removes all items from `queue_key`.
    pub fn discard<Q: ?Sized + Encode>(&self, queue_key: &Q) -> Result<bool>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store.queue_discard_with_ns(TYPED_QUEUE_NS, &qkey)
    }

    /// Iterates over live items in `queue_key` from head to tail.
    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        queue_key: &Q,
    ) -> impl DoubleEndedIterator<Item = Result<(usize, V)>> + 'a
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store
            .queue_iter_with_ns(TYPED_QUEUE_NS, &qkey)
            .map(|res| {
                res.and_then(|(idx, value)| {
                    decode_from_bytes::<V>(&value).map(|value| (idx, value))
                })
            })
    }
}

impl<L, K, V> CandyTypedList<L, K, V>
where
    L: CandyTypedKey + Encode,
    K: Encode + DecodeOwned,
    V: Encode + DecodeOwned,
{
    /// Creates a typed ordered-map/list view over `store`.
    pub fn new(store: Arc<CandyStore>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn make_list_key<Q: ?Sized + Encode>(list_key: &Q) -> InlineBytes
    where
        L: Borrow<Q>,
    {
        append_type_id(encode_to_smallvec(list_key), L::TYPE_ID)
    }

    fn make_item_key<Q: ?Sized + Encode>(item_key: &Q) -> InlineBytes
    where
        K: Borrow<Q>,
    {
        encode_to_smallvec(item_key)
    }

    /// Returns `true` if `item_key` exists in `list_key`.
    pub fn contains<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> Result<bool>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        self.get(list_key, item_key).map(|value| value.is_some())
    }

    /// Inserts or replaces `item_key` in `list_key`, placing it at the logical tail.
    pub fn set<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        val: &Q3,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = encode_to_smallvec(val);
        self.store
            .list_set_at_tail_with_ns(TYPED_LIST_NS, &lkey, &ikey, &vbytes)?
            .map(|prev| decode_from_bytes::<V>(&prev))
            .transpose()
    }

    /// Returns the current value for `item_key`, or inserts `default_val` if it is missing.
    pub fn get_or_create<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        default_val: &Q3,
    ) -> Result<V>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = encode_to_smallvec(default_val);
        match self
            .store
            .list_get_or_create_with_ns(TYPED_LIST_NS, &lkey, &ikey, &vbytes)?
        {
            crate::GetOrCreateStatus::ExistingValue(value)
            | crate::GetOrCreateStatus::CreatedNew(value) => decode_from_bytes::<V>(&value),
        }
    }

    /// Replaces `item_key` only if its current value matches `expected_val` when provided.
    pub fn replace<
        Q1: ?Sized + Encode,
        Q2: ?Sized + Encode,
        Q3: ?Sized + Encode,
        Q4: ?Sized + Encode,
    >(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        val: &Q3,
        expected_val: Option<&Q4>,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = encode_to_smallvec(val);
        let expected_bytes = expected_val.map(encode_to_smallvec);
        match self.store.list_replace_with_ns(
            TYPED_LIST_NS,
            &lkey,
            &ikey,
            &vbytes,
            expected_bytes.as_deref(),
        )? {
            crate::ReplaceStatus::PrevValue(prev) => decode_from_bytes::<V>(&prev).map(Some),
            crate::ReplaceStatus::WrongValue(_) | crate::ReplaceStatus::DoesNotExist => Ok(None),
        }
    }

    /// Returns the decoded value for `item_key`, if present.
    pub fn get<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        self.store
            .list_get_with_ns(TYPED_LIST_NS, &lkey, &ikey)?
            .map(|value| decode_from_bytes::<V>(&value))
            .transpose()
    }

    /// Removes `item_key` and returns its previous decoded value if it existed.
    pub fn remove<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        self.store
            .list_remove_with_ns(TYPED_LIST_NS, &lkey, &ikey)?
            .map(|value| decode_from_bytes::<V>(&value))
            .transpose()
    }

    /// Returns the number of live items in `list_key`.
    pub fn len<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<usize>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store.list_len_with_ns(TYPED_LIST_NS, &lkey)
    }

    /// Returns the current inclusive-exclusive logical span for `list_key`.
    pub fn range<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Range<usize>>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store.list_range_with_ns(TYPED_LIST_NS, &lkey)
    }

    /// Returns `true` when `list_key` has no live items.
    pub fn is_empty<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<bool>
    where
        L: Borrow<Q>,
    {
        self.len(list_key).map(|len| len == 0)
    }

    /// Removes all items from `list_key`.
    pub fn discard<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<bool>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store.list_discard_with_ns(TYPED_LIST_NS, &lkey)
    }

    /// Compacts `list_key` when `params` indicate enough holes exist to justify rewriting it.
    pub fn compact_if_needed<Q: ?Sized + Encode>(
        &self,
        list_key: &Q,
        params: ListCompactionParams,
    ) -> Result<bool>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store
            .list_compact_with_ns(TYPED_LIST_NS, &lkey, params)
    }

    /// Inserts or replaces `item_key`, moving it to the logical tail and returning the previous value when present.
    pub fn set_promoting<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        value: &Q3,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let lkey = Self::make_list_key(list_key);
        let ikey = Self::make_item_key(item_key);
        let vbytes = encode_to_smallvec(value);
        self.store
            .list_promote_with_ns(TYPED_LIST_NS, &lkey, &ikey, &vbytes)?
            .map(|prev| decode_from_bytes::<V>(&prev))
            .transpose()
    }

    /// Iterates over live items in `list_key` from head to tail.
    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        list_key: &Q,
    ) -> impl DoubleEndedIterator<Item = Result<(K, V)>> + 'a
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store
            .list_iter_with_ns(TYPED_LIST_NS, &lkey)
            .map(|res| {
                res.and_then(|(key, value)| {
                    Ok((
                        decode_from_bytes::<K>(&key)?,
                        decode_from_bytes::<V>(&value)?,
                    ))
                })
            })
    }

    /// Removes and returns the tail item of `list_key`.
    pub fn pop_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        match self.store.pop_list_tail_with_ns(TYPED_LIST_NS, &lkey)? {
            Some((key, value)) => Ok(Some((
                decode_from_bytes::<K>(&key)?,
                decode_from_bytes::<V>(&value)?,
            ))),
            None => Ok(None),
        }
    }

    /// Removes and returns the head item of `list_key`.
    pub fn pop_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        match self.store.pop_list_head_with_ns(TYPED_LIST_NS, &lkey)? {
            Some((key, value)) => Ok(Some((
                decode_from_bytes::<K>(&key)?,
                decode_from_bytes::<V>(&value)?,
            ))),
            None => Ok(None),
        }
    }

    /// Returns the tail item of `list_key` without removing it.
    pub fn peek_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let mut iter = self.iter(list_key);
        match iter.next_back() {
            Some(Ok(pair)) => Ok(Some(pair)),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    /// Returns the head item of `list_key` without removing it.
    pub fn peek_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        match self.iter(list_key).next() {
            Some(Ok(pair)) => Ok(Some(pair)),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    /// Retains only items for which `func` returns `true`, preserving list order.
    pub fn retain<Q: ?Sized + Encode>(
        &self,
        list_key: &Q,
        mut func: impl FnMut(&K, &V) -> Result<bool>,
    ) -> Result<()>
    where
        L: Borrow<Q>,
    {
        let lkey = Self::make_list_key(list_key);
        self.store
            .list_retain_with_ns(TYPED_LIST_NS, &lkey, |k_bytes, v_bytes| {
                let key = decode_from_bytes::<K>(k_bytes)?;
                let value = decode_from_bytes::<V>(v_bytes)?;
                func(&key, &value)
            })
    }
}

fn decode_from_bytes<T: DecodeOwned>(bytes: &[u8]) -> Result<T> {
    T::from_bytes::<LE>(bytes).map_err(|err| {
        Error::IOError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("decode error: {err}"),
        ))
    })
}

fn encode_to_smallvec<T: ?Sized + Encode>(value: &T) -> InlineBytes {
    let mut bytes = InlineBytes::new();
    value.encode::<LE>(&mut bytes).unwrap();
    bytes
}

fn append_type_id(mut bytes: InlineBytes, type_id: u32) -> InlineBytes {
    bytes.extend_from_slice(&type_id.to_le_bytes());
    bytes
}
