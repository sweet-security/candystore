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

impl<K, V> Clone for VickyTypedStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}
