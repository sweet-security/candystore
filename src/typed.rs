use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use crate::{
    insertion::{GetOrCreateStatus, ReplaceStatus, SetStatus},
    store::TYPED_NAMESPACE,
    ModifyStatus, VickyStore,
};

use databuf::{config::num::LE, DecodeOwned, Encode, Result};

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

#[derive(Clone)]
pub struct VickyTypedStore<K, V> {
    store: Arc<VickyStore>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> VickyTypedStore<K, V>
where
    K: VickyTypedKey,
    V: Encode + DecodeOwned,
{
    pub fn new(store: Arc<VickyStore>) -> Self {
        Self {
            store,
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    fn make_key<Q: ?Sized + Encode>(k: &Q) -> Vec<u8>
    where
        K: Borrow<Q>,
    {
        let mut kbytes = vec![];
        kbytes.extend_from_slice(&k.to_bytes::<LE>());
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
        let vbytes = self.store.get_raw(&kbytes)?;
        if let Some(vbytes) = vbytes {
            let val = V::from_bytes::<LE>(&vbytes)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn replace(&self, k: K, v: V) -> Result<ReplaceStatus> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        self.store.replace_raw(&kbytes, &vbytes)
    }

    pub fn replace_inplace(&self, k: K, v: V) -> Result<ModifyStatus> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        self.store.replace_inplace_raw(&kbytes, &vbytes)
    }

    pub fn set(&self, k: K, v: V) -> Result<SetStatus> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        self.store.set_raw(&kbytes, &vbytes)
    }

    pub fn get_or_create(&self, k: K, v: V) -> Result<GetOrCreateStatus> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        match self.store.get_or_create_raw(&kbytes, &vbytes)? {
            GetOrCreateStatus::CreatedNew(_) => Ok(GetOrCreateStatus::CreatedNew(vbytes)),
            v @ GetOrCreateStatus::ExistingValue(_) => Ok(v),
        }
    }

    pub fn remove<Q: ?Sized + Encode>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(k);
        let vbytes = self.store.remove_raw(&kbytes)?;
        if let Some(vbytes) = vbytes {
            let val = V::from_bytes::<LE>(&vbytes)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct VickyTypedCollection<C, K, V> {
    store: Arc<VickyStore>,
    _c: PhantomData<C>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<C, K, V> VickyTypedCollection<C, K, V>
where
    C: VickyTypedKey,
    K: VickyTypedKey,
    V: Encode + DecodeOwned,
{
    pub fn new(store: Arc<VickyStore>) -> Self {
        Self {
            store,
            _c: PhantomData,
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    pub fn contains<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        coll_key: &Q1,
        item_key: &Q2,
    ) -> Result<bool>
    where
        C: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let coll_key = coll_key.to_bytes::<LE>();
        let item_key = item_key.to_bytes::<LE>();
        Ok(self
            .store
            .get_from_collection(&coll_key, &item_key)?
            .is_some())
    }

    pub fn get<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        coll_key: &Q1,
        item_key: &Q2,
    ) -> Result<Option<V>>
    where
        C: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let coll_key = coll_key.to_bytes::<LE>();
        let item_key = item_key.to_bytes::<LE>();
        let vbytes = self.store.get_from_collection(&coll_key, &item_key)?;
        if let Some(vbytes) = vbytes {
            let val = V::from_bytes::<LE>(&vbytes)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn set(&self, coll_key: C, item_key: K, val: V) -> Result<SetStatus> {
        let coll_key = coll_key.to_bytes::<LE>();
        let item_key = item_key.to_bytes::<LE>();
        let val = val.to_bytes::<LE>();
        self.store.set_in_collection(&coll_key, &item_key, &val)
    }

    pub fn remove<Q1: ?Sized + Encode, Q2: ?Sized + Encode>(
        &self,
        coll_key: &Q1,
        item_key: &Q2,
    ) -> Result<Option<V>>
    where
        C: Borrow<Q1>,
        K: Borrow<Q2>,
    {
        let coll_key = coll_key.to_bytes::<LE>();
        let item_key = item_key.to_bytes::<LE>();
        let vbytes = self.store.remove_from_collection(&coll_key, &item_key)?;
        if let Some(vbytes) = vbytes {
            let val = V::from_bytes::<LE>(&vbytes)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        coll_key: &Q,
    ) -> impl Iterator<Item = Result<(K, V)>> + 'a
    where
        C: Borrow<Q>,
    {
        let coll_key = coll_key.to_bytes::<LE>();
        self.store.iter_collection(&coll_key).map(|res| match res {
            Err(e) => Err(e),
            Ok((k, v)) => {
                let key = K::from_bytes::<LE>(&k)?;
                let val = V::from_bytes::<LE>(&v)?;
                Ok((key, val))
            }
        })
    }
}
