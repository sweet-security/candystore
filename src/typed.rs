use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use crate::VickyStore;

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
        let mut kbytes = k.to_bytes::<LE>();
        kbytes.extend_from_slice(&K::TYPE_ID.to_le_bytes()[..]);
        kbytes
    }

    pub fn contains<Q: ?Sized + Encode>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
    {
        self.store.contains(&Self::make_key(k))
    }

    pub fn get<Q: ?Sized + Encode>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(k);
        let vbytes = self.store.get(&kbytes)?;
        if let Some(vbytes) = vbytes {
            let val = V::from_bytes::<LE>(&vbytes)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn insert(&self, k: K, v: V) -> Result<()> {
        let kbytes = Self::make_key(&k);
        let vbytes = v.to_bytes::<LE>();
        self.store.insert(&kbytes, &vbytes)
    }

    pub fn remove<Q: ?Sized + Encode>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
    {
        let kbytes = Self::make_key(k);
        let vbytes = self.store.remove(&kbytes)?;
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