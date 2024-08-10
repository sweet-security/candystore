use std::sync::MutexGuard;

use crate::{
    hashing::PartedHash,
    shard::{InsertMode, KVPair},
    store::{ITEM_NAMESPACE, LIST_NAMESPACE},
    GetOrCreateStatus, ReplaceStatus, Result, SetStatus, VickyError, VickyStore,
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
    type Item = Result<Option<KVPair>>;
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
            // this means the current element was removed by another thread, and that's okay
            // because we don't hold any locks during iteration. this is an early stop,
            // which means the reader might want to retry
            return Some(Ok(None));
        };
        k.truncate(k.len() - ITEM_SUFFIX_LEN);
        let chain = chain_of(&v);
        self.next_ph = Some(chain.next);
        v.truncate(v.len() - size_of::<Chain>());

        Some(Ok(Some((k, v))))
    }
}

macro_rules! corrupted_list {
    ($($arg:tt)*) => {
        return Err(Box::new(VickyError::CorruptedLinkedList(format!($($arg)*))));
    };
}

macro_rules! corrupted_if {
    ($e1: expr, $e2: expr, $($arg:tt)*) => {
        if ($e1 == $e2) {
            let tmp = format!($($arg)*);
            let full = format!("{tmp} ({:?} == {:?})", $e1, $e2);
            return Err(Box::new(VickyError::CorruptedLinkedList(full)));
        }
    };
}

macro_rules! corrupted_unless {
    ($e1: expr, $e2: expr, $($arg:tt)*) => {
        if ($e1 != $e2) {
            let tmp = format!($($arg)*);
            let full = format!("{tmp} ({:?} != {:?})", $e1, $e2);
            return Err(Box::new(VickyError::CorruptedLinkedList(full)));
        }
    };
}

/// in order to make linked-lists crash-safe, the algorithm will be as follows:
///
/// insert:
///  * check for existance of item, if it exists, it's already a member of the list
///  * if it does not exist, go to list.tail. this element must exist.
///  * start walking from list.tail over the next elements until we find a valid item
///  * this is the true tail of the list. if curr->next is valid but missing, we consider
///    curr to be true end as well.
///  * make curr->next = new item's key
///  * insert new item (prev pointing to curr)
///  * set list.tail = new item, len+=1
///
///    this is safe because if we crash at any point, the list is still valid, and
///    the accounting will be fixed by the next insert (patching len and tail)
///
/// removal:
///  * check if the element exists. if not, no-op
///  * if the element is the only item in the list, remove the list, and then remove the item.
///  * if the element is the first in the (non-empty) list:
///    * point list.head to element->next, set len-=1
///    * point the new first element.prev = INVALID
///    * remove the element
///  * if the element is the last in the (non-empty) list:
///    * point list.tail to element->prev, set len-=1
///    * point the new last element.next = INVALID
///    * remove the element
///  * if the element is a middle element:
///    * point element->prev->next to element->next -- now the element will not be traversed by iteration
///    * point element->next->prev to element->prev -- now the element is completely disconnected
///    * set list.len -= 1 -- THIS IS NOT CRASH SAFE. better remove the len altogether
///    * remove the element
///

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
        self.keyed_locks[(coll_ph.signature() & self.keyed_locks_mask) as usize]
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

    fn _insert_to_collection(
        &self,
        coll_key: &[u8],
        item_key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<SetStatus> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key);
        let (item_ph, item_key) = self.make_item_key(coll_ph, item_key);

        let _guard = self.lock_collection(coll_ph);

        // if the item already exists, it means it belongs to this list. we just need to update the value and
        // keep the existing chain part
        if let Some(mut old_val) = self.get_raw(&item_key)? {
            if matches!(mode, InsertMode::GetOrCreate) {
                // don't replace the existing value
                old_val.truncate(old_val.len() - size_of::<Chain>());
                return Ok(SetStatus::PrevValue(old_val));
            }

            let mut new_val = val.as_ref().to_owned();
            new_val.extend_from_slice(&old_val[old_val.len() - size_of::<Chain>()..]);
            match self.replace_raw(&item_key, &new_val)? {
                ReplaceStatus::DoesNotExist => {
                    corrupted_list!("failed replacing existing item");
                }
                ReplaceStatus::PrevValue(mut v) => {
                    v.truncate(v.len() - size_of::<Chain>());
                    return Ok(SetStatus::PrevValue(v));
                }
            }
        }

        if matches!(mode, InsertMode::Replace) {
            // not allowed to create
            return Ok(SetStatus::CreatedNew);
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
            corrupted_unless!(curr_list.tail, item_ph, "head != tail");
            corrupted_unless!(curr_list.len, 1, "len != 1");
            this_val.extend_from_slice(bytes_of(&Chain::INVALID));
            return self.set_raw(&item_key, &this_val);
        }

        // the list already existed, which means we need to read the last element (tail) and update its pointers
        let Some((last_k, last_v)) = self._get_from_collection(coll_ph, curr_list.tail)? else {
            corrupted_list!("tail {:?} does not exist", curr_list.tail);
        };

        //  list [h  t]                           list [h  t]
        //        |  \_____             \\              |  \________________
        //        |        \             \\             |                   \
        //       item1     item2         //            item1     item2      item3
        //        n---------^ n--X      //              n---------^ n--------^ n--X
        //    X__p \________p                       X__p \________p \_________p

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
            corrupted_list!("tail element {item_key:?} already exists");
        }

        if !self
            .modify_inplace_raw(
                &last_k,
                bytes_of(&last_chain),
                last_v.len() - size_of::<Chain>(),
                Some(&last_v[last_v.len() - size_of::<Chain>()..]),
            )?
            .was_replaced()
        {
            corrupted_list!(
                "failed to update previous element {last_k:?} to point to this one {item_key:?}"
            );
        }

        if !self
            .modify_inplace_raw(
                &coll_key,
                bytes_of(&new_list),
                0,
                Some(bytes_of(&curr_list)),
            )?
            .was_replaced()
        {
            corrupted_list!("failed to update list tail to point to {item_key:?}");
        }
        Ok(SetStatus::CreatedNew)
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
        self._insert_to_collection(
            coll_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
            InsertMode::Set,
        )
    }

    pub fn replace_in_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<ReplaceStatus> {
        match self._insert_to_collection(
            coll_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
            InsertMode::Replace,
        )? {
            SetStatus::CreatedNew => Ok(ReplaceStatus::DoesNotExist),
            SetStatus::PrevValue(v) => Ok(ReplaceStatus::PrevValue(v)),
        }
    }

    pub fn get_or_create_in_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<GetOrCreateStatus> {
        match self._insert_to_collection(
            coll_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
            InsertMode::GetOrCreate,
        )? {
            SetStatus::CreatedNew => Ok(GetOrCreateStatus::CreatedNew(val.as_ref().to_owned())),
            SetStatus::PrevValue(v) => Ok(GetOrCreateStatus::ExistingValue(v)),
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
        corrupted_unless!(list.len, 0, "expected list to be empty {item_key:?}");
        corrupted_unless!(
            chain.next,
            PartedHash::INVALID,
            "chain.next must be invalid"
        );
        corrupted_unless!(
            chain.prev,
            PartedHash::INVALID,
            "chain.prev must be invalid"
        );

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
        corrupted_unless!(
            chain.next,
            PartedHash::INVALID,
            "last element must not have a valid next"
        );
        corrupted_if!(
            chain.prev,
            PartedHash::INVALID,
            "last element must have a valid prev"
        );
        list.tail = chain.prev;

        let Some((prev_k, prev_v)) = self._get_from_collection(coll_ph, chain.prev)? else {
            corrupted_list!("missing prev element {item_key:?}");
        };

        if !self
            .modify_inplace_raw(&coll_key, bytes_of(&list), 0, Some(&list_buf))?
            .was_replaced()
        {
            corrupted_list!("failed updating list tail to point to prev");
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
        corrupted_unless!(
            chain.prev,
            PartedHash::INVALID,
            "first element must not have a valid prev {item_key:?}"
        );
        corrupted_if!(
            chain.next,
            PartedHash::INVALID,
            "first element must have a valid next {item_key:?}"
        );
        list.head = chain.next;

        let Some((next_k, next_v)) = self._get_from_collection(coll_ph, chain.next)? else {
            corrupted_list!("failed getting next of {item_key:?}");
        };

        if !self
            .modify_inplace_raw(&coll_key, bytes_of(&list), 0, Some(&list_buf))?
            .was_replaced()
        {
            corrupted_list!("failed updating list head to point to next");
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
            corrupted_list!("failed updating prev=INVALID on the now-first element");
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
        corrupted_if!(
            chain.next,
            PartedHash::INVALID,
            "a middle element must have a valid prev"
        );
        corrupted_if!(
            chain.prev,
            PartedHash::INVALID,
            "a middle element must have a valid next"
        );

        let Some((prev_k, prev_v)) = self._get_from_collection(coll_ph, chain.prev)? else {
            corrupted_list!("missing prev element of {item_key:?}");
        };
        let Some((next_k, next_v)) = self._get_from_collection(coll_ph, chain.next)? else {
            corrupted_list!("missing next element of {item_key:?}");
        };

        if !self
            .modify_inplace_raw(&coll_key, bytes_of(&list), 0, Some(&list_buf))?
            .was_replaced()
        {
            corrupted_list!("failed updating list length");
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
            corrupted_list!("failed updating prev.next on {prev_k:?}");
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
            corrupted_list!("failed updating next.prev on {next_k:?}");
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
            corrupted_list!("list element exists but list does not {item_key:?}");
        };
        let mut list = *from_bytes::<LinkedList>(&list_buf);
        let chain = chain_of(&v);

        if list.len == 0 {
            corrupted_list!("list has elements but has len=0");
        }
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

    pub fn collection_len<B: AsRef<[u8]> + ?Sized>(&self, coll_key: &B) -> Result<usize> {
        let (_coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());
        let Some(list_buf) = self.get_raw(&coll_key)? else {
            return Ok(0);
        };
        let list = from_bytes::<LinkedList>(&list_buf);
        Ok(list.len as usize)
    }

    /// Discards the given list (removes all elements). This also works for corrupt lists, in case they
    /// need to be dropped.
    pub fn discard_collection<B: AsRef<[u8]> + ?Sized>(&self, coll_key: &B) -> Result<()> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());

        let _guard = self.lock_collection(coll_ph);

        let Some(list_buf) = self.remove_raw(&coll_key)? else {
            return Ok(());
        };
        let list = *from_bytes::<LinkedList>(&list_buf);
        let mut curr = list.head;

        while curr != PartedHash::INVALID {
            let Some((k, v)) = self._get_from_collection(coll_ph, curr)? else {
                break;
            };

            curr = chain_of(&v).next;
            self.remove_raw(&k)?;
        }

        Ok(())
    }
}
