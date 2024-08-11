use anyhow::anyhow;
use std::{borrow::Borrow, marker::PhantomData, sync::Arc};
use uuid::Uuid;

use crate::{
    insertion::{ReplaceStatus, SetStatus},
    store::TYPED_NAMESPACE,
    ModifyStatus, VickyStore,
};

use crate::Result;
use databuf::{config::num::LE, DecodeOwned, Encode};

pub trait VickyTypedKey: Encode + DecodeOwned {
    /// a random number that remains consistent (unlike `TypeId`), so that `MyPair(u32, u32)` is different than
    /// `YourPair(u32, u32)``
    const TYPE_ID: u32;
}

macro_rules! typed_builtin {
    ($t:ty, $v:literal) => {
        impl VickyTypedKey for $t {
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

fn from_bytes<T: DecodeOwned>(bytes: &[u8]) -> Result<T> {
    T::from_bytes::<LE>(bytes).map_err(|e| anyhow!(e))
}

#[derive(Clone)]
pub struct VickyTypedStore<K, V> {
    store: Arc<VickyStore>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> VickyTypedStore<K, V>
where
    K: VickyTypedKey,
    V: Encode + DecodeOwned,
{
    pub fn new(store: Arc<VickyStore>) -> Self {
        Self {
            store,
            _phantom: Default::default(),
        }
    }

    fn make_key<Q: ?Sized + Encode>(k: &Q) -> Vec<u8>
    where
        K: Borrow<Q>,
    {
        let mut kbytes = k.to_bytes::<LE>();
        kbytes.extend_from_slice(&K::TYPE_ID.to_le_bytes());
        kbytes.extend_from_slice(TYPED_NAMESPACE);
        kbytes
    }

    pub fn contains<Q: ?Sized + Encode>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
    {
        Ok(self.store.get_raw(&Self::make_key(k))?.is_some())
    }

    pub fn get<Q: ?Sized + Encode>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(k);
        if let Some(vbytes) = self.store.get_raw(&kbytes)? {
            Ok(Some(from_bytes::<V>(&vbytes)?))
        } else {
            Ok(None)
        }
    }

    pub fn replace(&self, k: K, v: V) -> Result<Option<V>> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        match self.store.replace_raw(&kbytes, &vbytes)? {
            ReplaceStatus::DoesNotExist => Ok(None),
            ReplaceStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
        }
    }

    pub fn replace_inplace(&self, k: K, v: V) -> Result<Option<V>> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        match self.store.replace_inplace_raw(&kbytes, &vbytes)? {
            ModifyStatus::DoesNotExist => Ok(None),
            ModifyStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
            ModifyStatus::ValueMismatch(_) => unreachable!(),
            ModifyStatus::ValueTooLong(_, _, _) => Ok(None),
            ModifyStatus::WrongLength(_, _) => Ok(None),
        }
    }

    pub fn set(&self, k: K, v: V) -> Result<Option<V>> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        match self.store.set_raw(&kbytes, &vbytes)? {
            SetStatus::CreatedNew => Ok(None),
            SetStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
        }
    }

    pub fn get_or_create(&self, k: K, v: V) -> Result<V> {
        let kbytes = Self::make_key(&k);
        Ok(from_bytes::<V>(
            &self
                .store
                .get_or_create_raw(&kbytes, v.to_bytes::<LE>())?
                .value(),
        )?)
    }

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

#[derive(Clone)]
pub struct VickyTypedList<L, K, V> {
    store: Arc<VickyStore>,
    _phantom: PhantomData<(L, K, V)>,
}

impl<L, K, V> VickyTypedList<L, K, V>
where
    L: VickyTypedKey,
    K: Encode + DecodeOwned,
    V: Encode + DecodeOwned,
{
    pub fn new(store: Arc<VickyStore>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn make_list_key<Q: ?Sized + Encode>(k: &Q) -> Vec<u8>
    where
        L: Borrow<Q>,
    {
        let mut kbytes = k.to_bytes::<LE>();
        kbytes.extend_from_slice(&L::TYPE_ID.to_le_bytes());
        kbytes
    }

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

    pub fn set(&self, list_key: L, item_key: K, val: V) -> Result<Option<V>> {
        let list_key = Self::make_list_key(&list_key);
        let item_key = item_key.to_bytes::<LE>();
        let val = val.to_bytes::<LE>();
        match self.store.owned_set_in_list(list_key, item_key, val)? {
            SetStatus::CreatedNew => Ok(None),
            SetStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
        }
    }

    pub fn get_or_create(&self, list_key: L, item_key: K, val: V) -> Result<V> {
        let list_key = Self::make_list_key(&list_key);
        let item_key = item_key.to_bytes::<LE>();
        let val = val.to_bytes::<LE>();
        let vbytes = self
            .store
            .owned_get_or_create_in_list(list_key, item_key, val)?
            .value();
        from_bytes::<V>(&vbytes)
    }

    pub fn replace(&self, list_key: L, item_key: K, val: V) -> Result<Option<V>> {
        let list_key = Self::make_list_key(&list_key);
        let item_key = item_key.to_bytes::<LE>();
        let val = val.to_bytes::<LE>();
        match self.store.owned_replace_in_list(list_key, item_key, val)? {
            ReplaceStatus::DoesNotExist => Ok(None),
            ReplaceStatus::PrevValue(v) => Ok(Some(from_bytes::<V>(&v)?)),
        }
    }

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
            Ok(None) => Ok(None),
            Ok(Some((k, v))) => {
                let key = from_bytes::<K>(&k)?;
                let val = from_bytes::<V>(&v)?;
                Ok(Some((key, val)))
            }
        })
    }

    pub fn discard<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<()>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        self.store.owned_discard_list(list_key)
    }
}

#[derive(Clone)]
pub struct VickyTypedQueue<L, V> {
    store: Arc<VickyStore>,
    _phantom: PhantomData<(L, V)>,
}

impl<L, V> VickyTypedQueue<L, V>
where
    L: VickyTypedKey,
    V: Encode + DecodeOwned,
{
    pub fn new(store: Arc<VickyStore>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn make_list_key<Q: ?Sized + Encode>(k: &Q) -> Vec<u8>
    where
        L: Borrow<Q>,
    {
        let mut kbytes = k.to_bytes::<LE>();
        kbytes.extend_from_slice(&L::TYPE_ID.to_le_bytes());
        kbytes
    }

    pub fn push<Q: ?Sized + Encode>(&self, list_key: &Q, v: V) -> Result<Uuid>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let val = v.to_bytes::<LE>();
        self.store.owned_push_to_list(list_key, val)
    }

    pub fn pop_head<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let Some((_, v)) = self.store.owned_pop_list_head(list_key)? else {
            return Ok(None);
        };
        Ok(Some(from_bytes::<V>(&v)?))
    }

    pub fn pop_tail<Q: ?Sized + Encode>(&self, list_key: &Q) -> Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        let list_key = Self::make_list_key(list_key);
        let Some((_, v)) = self.store.owned_pop_list_tail(list_key)? else {
            return Ok(None);
        };
        Ok(Some(from_bytes::<V>(&v)?))
    }
}
