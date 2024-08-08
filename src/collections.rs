use std::sync::MutexGuard;

use crate::{
    hashing::PartedHash,
    shard::KVPair,
    store::{ITEM_NAMESPACE, LIST_NAMESPACE},
    GetOrCreateStatus, ModifyStatus, ReplaceStatus, Result, SetStatus, VickyError, VickyStore,
};

use bytemuck::{bytes_of, from_bytes, Pod, Zeroable};

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct LinkedList {
    tail: PartedHash,
    head: PartedHash,
    len: u64,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct Chain {
    prev: PartedHash,
    next: PartedHash,
}

impl Chain {
    const INVALID: Self = Self {
        prev: PartedHash::INVALID,
        next: PartedHash::INVALID,
    };
}

const ITEM_SUFFIX_LEN: usize = size_of::<PartedHash>() + ITEM_NAMESPACE.len();

fn chain_of(buf: &[u8]) -> Chain {
    bytemuck::pod_read_unaligned(&buf[buf.len() - size_of::<Chain>()..])
}

pub struct LinkedListIterator<'a> {
    store: &'a VickyStore,
    coll_key: Vec<u8>,
    coll_ph: PartedHash,
    next_ph: Option<PartedHash>,
}

impl<'a> Iterator for LinkedListIterator<'a> {
    type Item = Result<KVPair>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_ph.is_none() {
            let buf = match self.store.get_raw(&self.coll_key) {
                Ok(buf) => buf,
                Err(e) => return Some(Err(e)),
            };
            let Some(buf) = buf else {
                return None;
            };
            let list = *from_bytes::<LinkedList>(&buf);
            self.next_ph = Some(list.head);
        }
        let Some(curr) = self.next_ph else {
            return None;
        };
        if curr == PartedHash::INVALID {
            return None;
        }
        let kv = match self.store._get_from_collection(self.coll_ph, curr) {
            Err(e) => return Some(Err(e)),
            Ok(kv) => kv,
        };
        let Some((mut k, mut v)) = kv else {
            return Some(Err(Box::new(VickyError::BrokenList)));
        };
        k.truncate(k.len() - ITEM_SUFFIX_LEN);
        let chain = chain_of(&v);
        self.next_ph = Some(chain.next);
        v.truncate(v.len() - size_of::<Chain>());

        Some(Ok((k, v)))
    }
}

impl VickyStore {
    fn make_coll_key(&self, coll_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_key = coll_key.to_owned();
        full_key.extend_from_slice(LIST_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &full_key), full_key)
    }

    fn make_item_key(&self, coll_ph: PartedHash, item_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_key = item_key.to_owned();
        full_key.extend_from_slice(bytes_of(&coll_ph));
        full_key.extend_from_slice(ITEM_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &full_key), full_key)
    }

    fn lock_collection(&self, coll_ph: PartedHash) -> MutexGuard<()> {
        self.keyed_locks[coll_ph.signature() as usize % self.keyed_locks.len()]
            .lock()
            .unwrap()
    }

    fn _get_from_collection(
        &self,
        coll_ph: PartedHash,
        item_ph: PartedHash,
    ) -> Result<Option<KVPair>> {
        let mut suffix = [0u8; ITEM_SUFFIX_LEN];
        suffix[0..PartedHash::LEN].copy_from_slice(bytes_of(&coll_ph));
        suffix[PartedHash::LEN..].copy_from_slice(ITEM_NAMESPACE);

        for res in self.get_by_hash(item_ph) {
            let (k, v) = res?;
            if k.ends_with(&suffix) {
                return Ok(Some((k, v)));
            }
        }
        Ok(None)
    }

    pub fn set_in_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<SetStatus> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());
        let (item_ph, item_key) = self.make_item_key(coll_ph, item_key.as_ref());

        let _guard = self.lock_collection(coll_ph);

        // if the item already exists, it means it belongs to this list. we just need to update the value and
        // keep the existing chain part
        if let Some(old_val) = self.get_raw(&item_key)? {
            let mut new_val = val.as_ref().to_owned();
            new_val.extend_from_slice(&old_val[old_val.len() - size_of::<Chain>()..]);
            match self.replace_raw(&item_key, &new_val)? {
                ReplaceStatus::DoesNotExist => {
                    // another thread deleted this entry, but that's acceptable
                    return Ok(SetStatus::CreatedNew);
                }
                ReplaceStatus::PrevValue(mut v) => {
                    v.truncate(v.len() - size_of::<Chain>());
                    return Ok(SetStatus::PrevValue(v));
                }
            }
        }

        // item does not exist, and the list itself might also not exist. get or create the list
        let curr_list = LinkedList {
            tail: item_ph,
            head: item_ph,
            len: 1,
        };

        let curr_list = match self.get_or_create_raw(&coll_key, bytes_of(&curr_list))? {
            GetOrCreateStatus::CreatedNew(_) => curr_list,
            GetOrCreateStatus::ExistingValue(v) => *from_bytes::<LinkedList>(&v),
        };

        let mut this_val = val.as_ref().to_owned();

        // we have the list. if the list points to this item, it means we've just created it and we point to the
        // first item. the first time should have prev=INVALID and next=INVALID
        if curr_list.head == item_ph {
            assert_eq!(curr_list.tail, item_ph);
            assert_eq!(curr_list.len, 1);
            this_val.extend_from_slice(bytes_of(&Chain::INVALID));
            return self.set_raw(&item_key, &this_val);
        }

        // the list already existed, which means we need to read the last element (tail) and update its pointers
        let Some((last_k, last_v)) = self._get_from_collection(coll_ph, curr_list.tail)? else {
            return Err(Box::new(VickyError::BrokenList));
        };

        //  list [h  t]                \\         list [h  t]
        //        |  \_____             \\              |  \________________
        //        |        \             \\             |                   \
        //       item1     item2         //            item1     item2      item3
        //        n---------^ n--X      //              n---------^ n--------^ n--X
        //    X__p \________p          //           X__p \________p \_________p

        let mut last_chain = chain_of(&last_v);
        last_chain.next = item_ph;
        let new_list = LinkedList {
            head: curr_list.head,
            tail: item_ph,
            len: curr_list.len + 1,
        };
        let this_chain = Chain {
            next: PartedHash::INVALID,
            prev: curr_list.tail,
        };
        this_val.extend_from_slice(bytes_of(&this_chain));

        // first create the new item, then update last_k, then update the list

        if self.set_raw(&item_key, &this_val)?.was_replaced() {
            return Err(Box::new(VickyError::BrokenList));
        }

        match self.modify_inplace_raw(
            &last_k,
            bytes_of(&last_chain),
            last_v.len() - size_of::<Chain>(),
            Some(&last_v[last_v.len() - size_of::<Chain>()..]),
        )? {
            ModifyStatus::DoesNotExist => return Err(Box::new(VickyError::BrokenList)),
            ModifyStatus::PrevValue(_) => {}     // success
            ModifyStatus::ValueMismatch(_) => {} // retry
            ModifyStatus::ValueTooLong(_, _, _) => return Err(Box::new(VickyError::BrokenList)),
            ModifyStatus::WrongLength(_, _) => return Err(Box::new(VickyError::BrokenList)),
        }

        match self.modify_inplace_raw(
            &coll_key,
            bytes_of(&new_list),
            0,
            Some(bytes_of(&curr_list)),
        )? {
            ModifyStatus::DoesNotExist => return Err(Box::new(VickyError::BrokenList)),
            ModifyStatus::PrevValue(_) => {
                return Ok(SetStatus::CreatedNew);
            }
            ModifyStatus::ValueMismatch(_) => return Err(Box::new(VickyError::BrokenList)),
            ModifyStatus::ValueTooLong(_, _, _) => return Err(Box::new(VickyError::BrokenList)),
            ModifyStatus::WrongLength(_, _) => return Err(Box::new(VickyError::BrokenList)),
        }
    }

    pub fn get_from_collection<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        coll_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        let (coll_ph, _) = self.make_coll_key(coll_key.as_ref());
        let (_, item_key) = self.make_item_key(coll_ph, item_key.as_ref());
        let Some(mut v) = self.get_raw(&item_key)? else {
            return Ok(None);
        };
        v.truncate(v.len() - size_of::<Chain>());
        Ok(Some(v))
    }

    fn _remove_from_collection_single(
        &self,
        list: LinkedList,
        chain: Chain,
        coll_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<()> {
        // this is the only element - remove it and the list itself
        assert_eq!(list.len, 0);
        assert_eq!(chain.next, PartedHash::INVALID);
        assert_eq!(chain.prev, PartedHash::INVALID);

        self.remove_raw(&coll_key)?;
        self.remove_raw(&item_key)?;
        Ok(())
    }

    fn _remove_from_collection_last(
        &self,
        list_buf: Vec<u8>,
        mut list: LinkedList,
        chain: Chain,
        coll_ph: PartedHash,
        coll_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<()> {
        // this item is the last one, and we have a previous item. update list.tail to the previous one,
        // and set prev.next = INVALID, and remove this item
        assert_eq!(chain.next, PartedHash::INVALID);
        assert_ne!(chain.prev, PartedHash::INVALID);
        list.tail = chain.prev;

        let Some((prev_k, prev_v)) = self._get_from_collection(coll_ph, chain.prev)? else {
            return Err(Box::new(VickyError::BrokenList));
        };

        if !self
            .modify_inplace_raw(&coll_key, bytes_of(&list), 0, Some(&list_buf))?
            .was_replaced()
        {
            return Err(Box::new(VickyError::BrokenList));
        }

        let mut prev_chain = chain_of(&prev_v);
        prev_chain.next = PartedHash::INVALID;
        self.modify_inplace_raw(
            &prev_k,
            bytes_of(&prev_chain),
            prev_v.len() - size_of::<Chain>(),
            Some(&prev_v[prev_v.len() - size_of::<Chain>()..]),
        )?;

        self.remove_raw(&item_key)?;
        Ok(())
    }

    fn _remove_from_collection_first(
        &self,
        list_buf: Vec<u8>,
        mut list: LinkedList,
        chain: Chain,
        coll_ph: PartedHash,
        coll_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<()> {
        // this is the first item in the list, and we have a following one. set list.head = next,
        // set next.prev = INVALID, and remove the item
        assert_eq!(chain.prev, PartedHash::INVALID);
        assert_ne!(chain.next, PartedHash::INVALID);
        list.head = chain.next;

        let Some((next_k, next_v)) = self._get_from_collection(coll_ph, chain.next)? else {
            return Err(Box::new(VickyError::BrokenList));
        };

        if !self
            .modify_inplace_raw(&coll_key, bytes_of(&list), 0, Some(&list_buf))?
            .was_replaced()
        {
            return Err(Box::new(VickyError::BrokenList));
        }

        let mut next_chain = chain_of(&next_v);
        next_chain.prev = PartedHash::INVALID;
        if !self
            .modify_inplace_raw(
                &next_k,
                bytes_of(&next_chain),
                next_v.len() - size_of::<Chain>(),
                Some(&next_v[next_v.len() - size_of::<Chain>()..]),
            )?
            .was_replaced()
        {
            return Err(Box::new(VickyError::BrokenList));
        }

        self.remove_raw(&item_key)?;
        Ok(())
    }

    fn _remove_from_collection_middle(
        &self,
        list_buf: Vec<u8>,
        list: LinkedList,
        chain: Chain,
        coll_ph: PartedHash,
        coll_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<()> {
        // this is a "middle" item, it has a prev one and a next one. set prev.next = this.next,
        // set next.prev = prev, update list (for `len`)
        assert_ne!(chain.next, PartedHash::INVALID);
        assert_ne!(chain.prev, PartedHash::INVALID);

        let Some((prev_k, prev_v)) = self._get_from_collection(coll_ph, chain.prev)? else {
            return Err(Box::new(VickyError::BrokenList));
        };
        let Some((next_k, next_v)) = self._get_from_collection(coll_ph, chain.next)? else {
            return Err(Box::new(VickyError::BrokenList));
        };

        if !self
            .modify_inplace_raw(&coll_key, bytes_of(&list), 0, Some(&list_buf))?
            .was_replaced()
        {
            return Err(Box::new(VickyError::BrokenList));
        }

        let mut prev_chain = chain_of(&prev_v);
        let mut next_chain = chain_of(&next_v);
        prev_chain.next = chain.next;
        next_chain.prev = chain.prev;

        if !self
            .modify_inplace_raw(
                &prev_k,
                bytes_of(&prev_chain),
                prev_v.len() - size_of::<Chain>(),
                Some(&prev_v[prev_v.len() - size_of::<Chain>()..]),
            )?
            .was_replaced()
        {
            return Err(Box::new(VickyError::BrokenList));
        }
        if !self
            .modify_inplace_raw(
                &next_k,
                bytes_of(&next_chain),
                next_v.len() - size_of::<Chain>(),
                Some(&next_v[next_v.len() - size_of::<Chain>()..]),
            )?
            .was_replaced()
        {
            return Err(Box::new(VickyError::BrokenList));
        }

        self.remove_raw(&item_key)?;
        Ok(())
    }

    pub fn remove_from_collection<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        coll_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());
        let (item_ph, item_key) = self.make_item_key(coll_ph, item_key.as_ref());

        let _guard = self.lock_collection(coll_ph);

        let Some(mut v) = self.get_raw(&item_key)? else {
            return Ok(None);
        };

        let Some(list_buf) = self.get_raw(&coll_key)? else {
            return Err(Box::new(VickyError::BrokenList));
        };
        let mut list = *from_bytes::<LinkedList>(&list_buf);
        let chain = chain_of(&v);

        assert!(list.len > 0);
        list.len -= 1;

        if list.tail == item_ph && list.head == item_ph {
            self._remove_from_collection_single(list, chain, coll_key, item_key)?
        } else if list.tail == item_ph {
            self._remove_from_collection_last(list_buf, list, chain, coll_ph, coll_key, item_key)?
        } else if list.head == item_ph {
            self._remove_from_collection_first(list_buf, list, chain, coll_ph, coll_key, item_key)?
        } else {
            self._remove_from_collection_middle(list_buf, list, chain, coll_ph, coll_key, item_key)?
        };

        v.truncate(v.len() - size_of::<Chain>());
        Ok(Some(v))
    }

    pub fn iter_collection<B: AsRef<[u8]> + ?Sized>(&self, coll_key: &B) -> LinkedListIterator {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());
        LinkedListIterator {
            store: &self,
            coll_key,
            coll_ph,
            next_ph: None,
        }
    }
}
