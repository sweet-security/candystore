use anyhow::anyhow;
use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use crate::{
    insertion::{ReplaceStatus, SetStatus},
    store::TYPED_NAMESPACE,
    CandyStore,
};

use crate::encodable::EncodableUuid;
use crate::Result;
use databuf::{config::num::LE, DecodeOwned, Encode};

pub trait CandyTypedKey: Encode + DecodeOwned {
    /// a random number that remains consistent (unlike [std::any::TypeId]), so that `MyPair(u32, u32)`
    /// is different from `YourPair(u32, u32)`
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

fn from_bytes<T: DecodeOwned>(bytes: &[u8]) -> Result<T> {
    T::from_bytes::<LE>(bytes).map_err(|e| anyhow!(e))
}

/// Typed stores are wrappers around an underlying [CandyStore], that serialize keys and values (using [databuf]).
/// These are but thin wrappers, and multiple such wrappers can exist over the same store.
///
/// The keys and values must support [Encode] and [DecodeOwned], with the addition that keys also provide
/// a `TYPE_ID` const, via the [CandyTypedKey] trait.
///
/// Notes:
/// * All APIs take keys and values by-ref, because they will serialize them, so taking owned values doesn't
///   make sense
/// * [CandyStore::iter] will skip typed items, since it's meaningless to interpret them without the wrapper
pub struct CandyTypedStore<K, V> {
    store: Arc<CandyStore>,
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
    K: CandyTypedKey,
    V: Encode + DecodeOwned,
{
    /// Constructs a typed wrapper over a CandyStore
    pub fn new(store: Arc<CandyStore>) -> Self {
        Self {
            store,
            _phantom: Default::default(),
        }
    }

    fn make_key<Q: ?Sized + Encode>(key: &Q) -> Vec<u8>
    where
        K: Borrow<Q>,
    {
        let mut kbytes = key.to_bytes::<LE>();
        kbytes.extend_from_slice(&K::TYPE_ID.to_le_bytes());
        kbytes.extend_from_slice(TYPED_NAMESPACE);
        kbytes
    }

    /// Same as [CandyStore::contains] but serializes the key
    pub fn contains<Q: ?Sized + Encode>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
    {
        Ok(self.store.get_raw(&Self::make_key(key))?.is_some())
    }

    /// Same as [CandyStore::get] but serializes the key and deserializes the value
    pub fn get<Q: ?Sized + Encode>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(key);
        if let Some(vbytes) = self.store.get_raw(&kbytes)? {
            Ok(Some(from_bytes::<V>(&vbytes)?))
        } else {
            Ok(None)
        }
    }

    /// Same as [CandyStore::replace] but serializes the key and the value
    pub fn replace<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
        expected_val: Option<&Q2>,
    ) -> Result<Option<V>>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        let vbytes = val.to_bytes::<LE>();
        let ebytes = expected_val.map(|ev| ev.to_bytes::<LE>()).unwrap_or(vec![]);
        match self
            .store
            .replace_raw(&kbytes, &vbytes, expected_val.map(|_| &*ebytes))?
        {
            ReplaceStatus::DoesNotExist => Ok(None),
            ReplaceStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
            ReplaceStatus::WrongValue(_) => Ok(None),
        }
    }

    /// Same as [CandyStore::set] but serializes the key and the value.
    pub fn set<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        val: &Q2,
    ) -> Result<Option<V>>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        let vbytes = val.to_bytes::<LE>();
        match self.store.set_raw(&kbytes, &vbytes)? {
            SetStatus::CreatedNew => Ok(None),
            SetStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
        }
    }

    /// Same as [CandyStore::get_or_create] but serializes the key and the default value
    pub fn get_or_create<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        key: &Q1,
        default_val: &Q2,
    ) -> Result<V>
    where
        K: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        let kbytes = Self::make_key(key);
        Ok(from_bytes::<V>(
            &self
                .store
                .get_or_create_raw(&kbytes, default_val.to_bytes::<LE>())?
                .value(),
        )?)
    }

    /// Same as [CandyStore::remove] but serializes the key
    pub fn remove<Q: ?Sized + Encode>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(k);
        if let Some(vbytes) = self.store.remove_raw(&kbytes)? {
            Ok(Some(from_bytes::<V>(&vbytes)?))
        } else {
            Ok(None)
        }
    }
}

/// A wrapper around [CandyStore] that exposes the linked-list API in a typed manner. See [CandyTypedStore] for more
/// info
pub struct CandyTypedList<L, K, V> {
    store: Arc<CandyStore>,
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
    L: CandyTypedKey,
    K: Encode + DecodeOwned,
    V: Encode + DecodeOwned,
{
    /// Constructs a [CandyTypedList] over an existing [CandyStore]
    pub fn new(store: Arc<CandyStore>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn make_list_key<Q: ?Sized + Encode>(list_key: &Q) -> Vec<u8>
    where
        L: Borrow<Q>,
    {
        let mut kbytes = list_key.to_bytes::<LE>();
        kbytes.extend_from_slice(&L::TYPE_ID.to_le_bytes());
        kbytes
    }

    /// Tests if the given typed `item_key` exists in this list (identified by `list_key`)
    pub fn contains<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> Result<bool>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let list_key = Self::make_list_key(list_key);
        let item_key = item_key.to_bytes::<LE>();
        Ok(self
            .store
            .owned_get_from_list(list_key, item_key)?
            .is_some())
    }

    /// Same as [CandyStore::get_from_list], but `list_key` and `item_key` are typed
    pub fn get<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let list_key = Self::make_list_key(list_key);
        let item_key = item_key.to_bytes::<LE>();
        if let Some(vbytes) = self.store.owned_get_from_list(list_key, item_key)? {
            Ok(Some(from_bytes::<V>(&vbytes)?))
        } else {
            Ok(None)
        }
    }

    fn _set<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        val: &Q3,
        promote: bool,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let list_key = Self::make_list_key(list_key);
        let item_key = item_key.to_bytes::<LE>();
        let val = val.to_bytes::<LE>();
        match self
            .store
            .owned_set_in_list(list_key, item_key, val, promote)?
        {
            SetStatus::CreatedNew => Ok(None),
            SetStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
        }
    }

    /// Same as [CandyStore::set_in_list], but `list_key`, `item_key` and `val` are typed
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
        self._set(list_key, item_key, val, false)
    }

    /// Same as [CandyStore::set_in_list_promoting], but `list_key`, `item_key` and `val` are typed
    pub fn set_promoting<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
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
        self._set(list_key, item_key, val, true)
    }

    /// Same as [CandyStore::get_or_create_in_list], but `list_key`, `item_key` and `default_val` are typed
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
        let list_key = Self::make_list_key(list_key);
        let item_key = item_key.to_bytes::<LE>();
        let default_val = default_val.to_bytes::<LE>();
        let vbytes = self
            .store
            .owned_get_or_create_in_list(list_key, item_key, default_val)?
            .value();
        from_bytes::<V>(&vbytes)
    }

    /// Same as [CandyStore::replace_in_list], but `list_key`, `item_key` and `val` are typed
    pub fn replace<Q1: ?Sized + Encode, Q2: ?Sized + Encode, Q3: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
        val: &Q3,
        expected_val: Option<&Q3>,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
        V: Borrow<Q3>,
    {
        let list_key = Self::make_list_key(list_key);
        let item_key = item_key.to_bytes::<LE>();
        let val = val.to_bytes::<LE>();
        let ebytes = expected_val
            .map(|ev| ev.to_bytes::<LE>())
            .unwrap_or_default();
        match self.store.owned_replace_in_list(
            list_key,
            item_key,
            val,
            expected_val.map(|_| &*ebytes),
        )? {
            ReplaceStatus::DoesNotExist => Ok(None),
            ReplaceStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
            ReplaceStatus::WrongValue(_) => Ok(None),
        }
    }

    /// Same as [CandyStore::remove_from_list], but `list_key` and `item_key`  are typed
    pub fn remove<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        item_key: &Q2,
    ) -> Result<Option<V>>
    where
        L: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let list_key = Self::make_list_key(list_key);
        let item_key = item_key.to_bytes::<LE>();
        if let Some(vbytes) = self.store.owned_remove_from_list(list_key, item_key)? {
            Ok(Some(from_bytes::<V>(&vbytes)?))
        } else {
            Ok(None)
        }
    }

    /// Same as [CandyStore::iter_list], but `list_key` is typed
    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        list_key: &Q,
    ) -> impl Iterator<Item = Result<Option<(K, V)>>> + 'a
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        self.store.owned_iter_list(list_key).map(|res| match res {
            Err(e) => Err(e),
            Ok((k, v)) => {
                let key = from_bytes::<K>(&k)?;
                let val = from_bytes::<V>(&v)?;
                Ok(Some((key, val)))
            }
        })
    }

    /// Same as [CandyStore::iter_list_backwards], but `list_key` is typed
    pub fn iter_backwards<'a, Q: ?Sized + Encode>(
        &'a self,
        list_key: &Q,
    ) -> impl Iterator<Item = Result<Option<(K, V)>>> + 'a
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        self.store
            .owned_iter_list_backwards(list_key)
            .map(|res| match res {
                Err(e) => Err(e),
                Ok((k, v)) => {
                    let key = from_bytes::<K>(&k)?;
                    let val = from_bytes::<V>(&v)?;
                    Ok(Some((key, val)))
                }
            })
    }

    /// Same as [CandyStore::discard_list], but `list_key` is typed
    pub fn discard<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<()>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        self.store.owned_discard_list(list_key)
    }

    /// Same as [CandyStore::push_to_list_head], but `list_key` is typed
    pub fn push_head<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        val: &Q2,
    ) -> Result<EncodableUuid>
    where
        L: Borrow<Q1>,
        EncodableUuid: From<K>,
        V: Borrow<Q2>,
    {
        let list_key = Self::make_list_key(list_key);
        let val = val.to_bytes::<LE>();
        self.store.owned_push_to_list_head(list_key, val)
    }

    /// Same as [CandyStore::push_to_list_tail], but `list_key` is typed
    pub fn push_tail<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        val: &Q2,
    ) -> Result<EncodableUuid>
    where
        L: Borrow<Q1>,
        EncodableUuid: From<K>,
        V: Borrow<Q2>,
    {
        let list_key = Self::make_list_key(list_key);
        let val = val.to_bytes::<LE>();
        self.store.owned_push_to_list_tail(list_key, val)
    }

    /// Same as [CandyStore::pop_list_tail], but `list_key` is typed
    pub fn pop_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let Some((k, v)) = self.store.owned_pop_list_tail(list_key)? else {
            return Ok(None);
        };
        Ok(Some((from_bytes::<K>(&k)?, from_bytes::<V>(&v)?)))
    }

    /// Same as [CandyStore::pop_list_head], but `list_key` is typed
    pub fn pop_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let Some((k, v)) = self.store.owned_pop_list_head(list_key)? else {
            return Ok(None);
        };
        Ok(Some((from_bytes::<K>(&k)?, from_bytes::<V>(&v)?)))
    }

    /// Same as [CandyStore::peek_list_tail], but `list_key` is typed
    pub fn peek_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let Some((k, v)) = self.store.owned_peek_list_tail(list_key)? else {
            return Ok(None);
        };
        Ok(Some((from_bytes::<K>(&k)?, from_bytes::<V>(&v)?)))
    }

    /// Same as [CandyStore::peek_list_head], but `list_key` is typed
    pub fn peek_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<(K, V)>>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let Some((k, v)) = self.store.owned_peek_list_head(list_key)? else {
            return Ok(None);
        };
        Ok(Some((from_bytes::<K>(&k)?, from_bytes::<V>(&v)?)))
    }
}

/// A wrapper around [CandyTypedList] that's specialized for double-ended queues - only allows pushing
/// and popping from either the head or the tail. The keys are auto-generated internally and are not exposed to
/// the caller
pub struct CandyTypedDeque<L, V> {
    pub list: CandyTypedList<L, EncodableUuid, V>,
}

impl<L, V> Clone for CandyTypedDeque<L, V> {
    fn clone(&self) -> Self {
        Self {
            list: self.list.clone(),
        }
    }
}

impl<L, V> CandyTypedDeque<L, V>
where
    L: CandyTypedKey,
    V: Encode + DecodeOwned,
{
    pub fn new(store: Arc<CandyStore>) -> Self {
        Self {
            list: CandyTypedList::new(store),
        }
    }

    /// Pushes a value at the beginning (head) of the queue
    pub fn push_head<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        val: &Q2,
    ) -> Result<()>
    where
        L: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        self.list.push_head(list_key, val)?;
        Ok(())
    }

    /// Pushes a value at the end (tail) of the queue
    pub fn push_tail<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        list_key: &Q1,
        val: &Q2,
    ) -> Result<()>
    where
        L: Borrow<Q1>,
        V: Borrow<Q2>,
    {
        self.list.push_tail(list_key, val)?;
        Ok(())
    }

    /// Pops a value from the beginning (head) of the queue
    pub fn pop_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.list.pop_head(list_key)?.map(|kv| kv.1))
    }

    /// Pops a value from the end (tail) of the queue
    pub fn pop_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.list.pop_tail(list_key)?.map(|kv| kv.1))
    }

    /// Peek at the value from the beginning (head) of the queue
    pub fn peek_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.list.peek_head(list_key)?.map(|kv| kv.1))
    }

    /// Peek at the value from the end (tail) of the queue
    pub fn peek_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.list.peek_tail(list_key)?.map(|kv| kv.1))
    }

    /// See [CandyTypedList::iter]
    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        list_key: &Q,
    ) -> impl Iterator<Item = Result<Option<V>>> + 'a
    where
        L: Borrow<Q>,
    {
        self.list.iter(list_key).map(|res| match res {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some((_, v))) => Ok(Some(v)),
        })
    }

    /// See [CandyTypedList::iter_backwards]
    pub fn iter_backwards<'a, Q: ?Sized + Encode>(
        &'a self,
        list_key: &Q,
    ) -> impl Iterator<Item = Result<Option<V>>> + 'a
    where
        L: Borrow<Q>,
    {
        self.list.iter_backwards(list_key).map(|res| match res {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some((_, v))) => Ok(Some(v)),
        })
    }
}
