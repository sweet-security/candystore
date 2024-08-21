use crate::{
    encodable::EncodableUuid,
    hashing::PartedHash,
    shard::{InsertMode, KVPair},
    store::{CHAIN_NAMESPACE, ITEM_NAMESPACE, LIST_NAMESPACE},
    CandyStore, GetOrCreateStatus, ReplaceStatus, Result, SetStatus,
};

use bytemuck::{bytes_of, from_bytes, Pod, Zeroable};
use parking_lot::MutexGuard;
use uuid::Uuid;

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct List {
    head_idx: u64, // inclusive
    tail_idx: u64, // exclusive
    holes: u64,
}

impl std::fmt::Debug for List {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "List(0x{:016x}..0x{:016x} len={} holes={})",
            self.head_idx,
            self.tail_idx,
            self.tail_idx - self.head_idx,
            self.holes
        )
    }
}

impl List {
    fn len(&self) -> u64 {
        self.tail_idx - self.head_idx
    }
    fn is_empty(&self) -> bool {
        self.head_idx == self.tail_idx
    }
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C, packed)]
struct ChainKey {
    list_ph: PartedHash,
    idx: u64,
    namespace: u8,
}

#[derive(Debug)]
pub struct ListCompactionParams {
    pub min_length: u64,
    pub min_holes_ratio: f64,
}

impl Default for ListCompactionParams {
    fn default() -> Self {
        Self {
            min_length: 100,
            min_holes_ratio: 0.25,
        }
    }
}

pub struct ListIterator<'a> {
    store: &'a CandyStore,
    list_key: Vec<u8>,
    list_ph: PartedHash,
    list: Option<List>,
    idx: u64,
    fwd: bool,
}

impl<'a> Iterator for ListIterator<'a> {
    type Item = Result<KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.list.is_none() {
            let _guard = self.store.lock_list(self.list_ph);
            let list_bytes = match self.store.get_raw(&self.list_key) {
                Ok(Some(list_bytes)) => list_bytes,
                Ok(None) => return None,
                Err(e) => return Some(Err(e)),
            };
            let list = *from_bytes::<List>(&list_bytes);
            self.list = Some(list);
            self.idx = if self.fwd {
                list.head_idx
            } else {
                list.tail_idx - 1
            };
        }
        let Some(list) = self.list else {
            return None;
        };

        while if self.fwd {
            self.idx < list.tail_idx
        } else {
            self.idx >= list.head_idx
        } {
            let idx = self.idx;
            if self.fwd {
                self.idx += 1;
            } else {
                self.idx -= 1;
            }

            match self.store.get_from_list_at_index(self.list_ph, idx, true) {
                Err(e) => return Some(Err(e)),
                Ok(Some((_, k, v))) => return Some(Ok((k, v))),
                Ok(None) => {
                    // try next index
                }
            }
        }

        None
    }
}

impl<'a> DoubleEndedIterator for ListIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}

#[derive(Debug)]
enum InsertToListStatus {
    Created(Vec<u8>),
    DoesNotExist,
    WrongValue(Vec<u8>),
    ExistingValue(Vec<u8>),
    Replaced(Vec<u8>),
}

enum InsertToListPos {
    Head,
    Tail,
}

impl CandyStore {
    const FIRST_IDX: u64 = 0x8000_0000_0000_0000;

    fn make_list_key(&self, mut list_key: Vec<u8>) -> (PartedHash, Vec<u8>) {
        list_key.extend_from_slice(LIST_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &list_key), list_key)
    }

    fn make_item_key(&self, list_ph: PartedHash, mut item_key: Vec<u8>) -> (PartedHash, Vec<u8>) {
        item_key.extend_from_slice(bytes_of(&list_ph));
        item_key.extend_from_slice(ITEM_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &item_key), item_key)
    }

    fn lock_list(&self, list_ph: PartedHash) -> MutexGuard<()> {
        self.keyed_locks[(list_ph.signature() & self.keyed_locks_mask) as usize].lock()
    }

    fn _insert_to_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        mut val: Vec<u8>,
        mode: InsertMode,
        pos: InsertToListPos,
    ) -> Result<InsertToListStatus> {
        let (list_ph, list_key) = self.make_list_key(list_key);
        let (item_ph, item_key) = self.make_item_key(list_ph, item_key);

        let _guard = self.lock_list(list_ph);

        // if the item already exists, it's already part of the list. just update it and preserve the index
        if let Some(mut existing_val) = self.get_raw(&item_key)? {
            match mode {
                InsertMode::GetOrCreate => {
                    existing_val.truncate(existing_val.len() - size_of::<u64>());
                    return Ok(InsertToListStatus::ExistingValue(existing_val));
                }
                InsertMode::Replace(expected_val) => {
                    if let Some(expected_val) = expected_val {
                        if expected_val != &existing_val[existing_val.len() - size_of::<u64>()..] {
                            existing_val.truncate(existing_val.len() - size_of::<u64>());
                            return Ok(InsertToListStatus::WrongValue(existing_val));
                        }
                    }
                    // fall through
                }
                InsertMode::Set => {
                    // fall through
                }
            }

            val.extend_from_slice(&existing_val[existing_val.len() - size_of::<u64>()..]);
            self.replace_raw(&item_key, &val, None)?;
            existing_val.truncate(existing_val.len() - size_of::<u64>());
            return Ok(InsertToListStatus::Replaced(existing_val));
        }

        if matches!(mode, InsertMode::Replace(_)) {
            // not allowed to create
            return Ok(InsertToListStatus::DoesNotExist);
        }

        // get of create the list
        let res = self.get_or_create_raw(
            &list_key,
            bytes_of(&List {
                head_idx: Self::FIRST_IDX,
                tail_idx: Self::FIRST_IDX + 1,
                holes: 0,
            })
            .to_owned(),
        )?;

        match res {
            crate::GetOrCreateStatus::CreatedNew(_) => {
                // list was just created. create chain
                self.set_raw(
                    bytes_of(&ChainKey {
                        list_ph,
                        idx: Self::FIRST_IDX,
                        namespace: CHAIN_NAMESPACE,
                    }),
                    bytes_of(&item_ph),
                )?;

                // create item
                val.extend_from_slice(&Self::FIRST_IDX.to_le_bytes());
                self.set_raw(&item_key, &val)?;
            }
            crate::GetOrCreateStatus::ExistingValue(list_bytes) => {
                let mut list = *from_bytes::<List>(&list_bytes);

                let item_idx = match pos {
                    InsertToListPos::Tail => {
                        let idx = list.tail_idx;
                        list.tail_idx += 1;
                        idx
                    }
                    InsertToListPos::Head => {
                        list.head_idx -= 1;
                        list.head_idx
                    }
                };

                // update list
                self.set_raw(&list_key, bytes_of(&list))?;

                // create chain
                self.set_raw(
                    bytes_of(&ChainKey {
                        list_ph,
                        idx: item_idx,
                        namespace: CHAIN_NAMESPACE,
                    }),
                    bytes_of(&item_ph),
                )?;

                // create item
                val.extend_from_slice(&item_idx.to_le_bytes());
                self.set_raw(&item_key, &val)?;
            }
        }

        val.truncate(val.len() - size_of::<u64>());
        Ok(InsertToListStatus::Created(val))
    }

    /// Inserts or updates an element `item_key` that belongs to list `list_key`. Returns [SetStatus::CreatedNew] if
    /// the item did not exist, or [SetStatus::PrevValue] with the previous value of the item.
    ///
    /// See also [Self::set].
    pub fn set_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<SetStatus> {
        self.owned_set_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
            false,
        )
    }

    /// Like [Self::set_in_list] but "promotes" the element to the tail of the list: it's basically a
    /// remove + insert operation. This can be usede to implement LRUs, where older elements are at the
    /// beginning and newer ones at the end.
    ///
    /// Note: **not crash-safe**
    pub fn set_in_list_promoting<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<SetStatus> {
        self.owned_set_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
            true,
        )
    }

    /// Owned version of [Self::set_in_list], which also takes promote as a parameter
    pub fn owned_set_in_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        val: Vec<u8>,
        promote: bool,
    ) -> Result<SetStatus> {
        if promote {
            self.owned_remove_from_list(list_key.clone(), item_key.clone())?;
        }
        match self._insert_to_list(
            list_key,
            item_key,
            val,
            InsertMode::Set,
            InsertToListPos::Tail,
        )? {
            InsertToListStatus::Created(_v) => Ok(SetStatus::CreatedNew),
            InsertToListStatus::Replaced(v) => Ok(SetStatus::PrevValue(v)),
            _ => unreachable!(),
        }
    }

    /// Like [Self::set_in_list], but will only replace (update) an existing item, i.e., it will never create the
    /// key
    pub fn replace_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
        expected_val: Option<&B3>,
    ) -> Result<ReplaceStatus> {
        self.owned_replace_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            val.as_ref().to_owned(),
            expected_val.map(|ev| ev.as_ref()),
        )
    }

    /// Owned version of [Self::replace_in_list]
    pub fn owned_replace_in_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        val: Vec<u8>,
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        match self._insert_to_list(
            list_key,
            item_key,
            val,
            InsertMode::Replace(expected_val),
            InsertToListPos::Tail,
        )? {
            InsertToListStatus::DoesNotExist => Ok(ReplaceStatus::DoesNotExist),
            InsertToListStatus::Replaced(v) => Ok(ReplaceStatus::PrevValue(v)),
            InsertToListStatus::WrongValue(v) => Ok(ReplaceStatus::WrongValue(v)),
            _ => unreachable!(),
        }
    }

    /// Like [Self::set_in_list] but will not replace (update) the element if it already exists - it will only
    /// create the element with the default value if it did not exist.
    pub fn get_or_create_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        default_val: &B3,
    ) -> Result<GetOrCreateStatus> {
        self.owned_get_or_create_in_list(
            list_key.as_ref().to_owned(),
            item_key.as_ref().to_owned(),
            default_val.as_ref().to_owned(),
        )
    }

    /// Owned version of [Self::get_or_create_in_list]
    pub fn owned_get_or_create_in_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
        default_val: Vec<u8>,
    ) -> Result<GetOrCreateStatus> {
        match self._insert_to_list(
            list_key,
            item_key,
            default_val,
            InsertMode::GetOrCreate,
            InsertToListPos::Tail,
        )? {
            InsertToListStatus::ExistingValue(v) => Ok(GetOrCreateStatus::ExistingValue(v)),
            InsertToListStatus::Created(v) => Ok(GetOrCreateStatus::CreatedNew(v)),
            _ => unreachable!(),
        }
    }

    /// Gets a list element identified by `list_key` and `item_key`. This is an O(1) operation.
    ///
    /// See also: [Self::get]
    pub fn get_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.owned_get_from_list(list_key.as_ref().to_owned(), item_key.as_ref().to_owned())
    }

    /// Owned version of [Self::get_from_list]
    pub fn owned_get_from_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let (list_ph, _) = self.make_list_key(list_key);
        let (_, item_key) = self.make_item_key(list_ph, item_key);
        let Some(mut val) = self.get_raw(&item_key)? else {
            return Ok(None);
        };
        val.truncate(val.len() - size_of::<u64>());
        Ok(Some(val))
    }

    /// Removes a element from the list, identified by `list_key` and `item_key. The element can be
    /// at any position in the list, not just the head or the tail, but in this case, it will create a "hole".
    /// This means that iterations will go over the missing element's index every time, until the list is compacted.
    ///
    /// See also [Self::remove], [Self::compact_list_if_needed]
    pub fn remove_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.owned_remove_from_list(list_key.as_ref().to_owned(), item_key.as_ref().to_owned())
    }

    /// Owned version of [Self::remove_from_list]
    pub fn owned_remove_from_list(
        &self,
        list_key: Vec<u8>,
        item_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let (list_ph, list_key) = self.make_list_key(list_key);
        let (_, item_key) = self.make_item_key(list_ph, item_key);

        let _guard = self.lock_list(list_ph);

        let Some(mut existing_val) = self.get_raw(&item_key)? else {
            return Ok(None);
        };

        let item_idx = u64::from_le_bytes(
            (&existing_val[existing_val.len() - size_of::<u64>()..])
                .try_into()
                .unwrap(),
        );
        existing_val.truncate(existing_val.len() - size_of::<u64>());

        // update list, if the item was the head/tail
        let list_bytes = self.get_raw(&list_key)?.unwrap();
        let mut list = *from_bytes::<List>(&list_bytes);

        if list.head_idx == item_idx || list.tail_idx == item_idx + 1 {
            if list.head_idx == item_idx {
                list.head_idx += 1;
            } else if list.tail_idx == item_idx + 1 {
                list.tail_idx -= 1;
            }
            if list.is_empty() {
                self.remove_raw(&list_key)?;
            } else {
                self.set_raw(&list_key, bytes_of(&list))?;
            }
        } else {
            list.holes += 1;
            self.set_raw(&list_key, bytes_of(&list))?;
        }

        // remove chain
        self.remove_raw(bytes_of(&ChainKey {
            list_ph,
            idx: item_idx,
            namespace: CHAIN_NAMESPACE,
        }))?;

        // remove item
        self.remove_raw(&item_key)?;

        Ok(Some(existing_val))
    }

    fn get_from_list_at_index(
        &self,
        list_ph: PartedHash,
        idx: u64,
        truncate: bool,
    ) -> Result<Option<(PartedHash, Vec<u8>, Vec<u8>)>> {
        let Some(item_ph_bytes) = self.get_raw(bytes_of(&ChainKey {
            idx,
            list_ph,
            namespace: CHAIN_NAMESPACE,
        }))?
        else {
            return Ok(None);
        };
        let item_ph = *from_bytes::<PartedHash>(&item_ph_bytes);

        let mut suffix = [0u8; size_of::<PartedHash>() + ITEM_NAMESPACE.len()];
        suffix[0..size_of::<PartedHash>()].copy_from_slice(bytes_of(&list_ph));
        suffix[size_of::<PartedHash>()..].copy_from_slice(ITEM_NAMESPACE);

        for (mut k, mut v) in self.get_by_hash(item_ph)? {
            if k.ends_with(&suffix) && v.ends_with(&idx.to_le_bytes()) {
                if truncate {
                    v.truncate(v.len() - size_of::<u64>());
                    k.truncate(k.len() - suffix.len());
                }
                return Ok(Some((item_ph, k, v)));
            }
        }

        Ok(None)
    }

    /// Compacts (rewrites) the list such that there will be no holes. Holes are created when removing an
    /// element from the middle of the list (not the head or tail), which makes iteration less efficient.
    /// You should call this function every so often if you're removing elements from lists at random locations.
    /// The function takes parameters that control when to compact: the list has to be of a minimal length and
    /// have a minimal holes-to-length ratio. The default values are expected to be okay for most use cases.
    /// Returns true if the list was compacted, false otherwise.
    ///
    /// Note: **Not crash-safe**
    pub fn compact_list_if_needed<B: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B,
        params: ListCompactionParams,
    ) -> Result<bool> {
        let (list_ph, list_key) = self.make_list_key(list_key.as_ref().to_owned());
        let _guard = self.lock_list(list_ph);

        let Some(list_bytes) = self.get_raw(&list_key)? else {
            return Ok(false);
        };
        let list = *from_bytes::<List>(&list_bytes);
        if list.len() < params.min_length {
            return Ok(false);
        }
        if (list.holes as f64) < (list.len() as f64) * params.min_holes_ratio {
            return Ok(false);
        }

        let mut new_idx = list.tail_idx;
        for idx in list.head_idx..list.tail_idx {
            let Some((item_ph, full_k, mut full_v)) =
                self.get_from_list_at_index(list_ph, idx, false)?
            else {
                continue;
            };

            // create new chain
            self.set_raw(
                bytes_of(&ChainKey {
                    idx: new_idx,
                    list_ph,
                    namespace: CHAIN_NAMESPACE,
                }),
                bytes_of(&item_ph),
            )?;

            // update item's index suffix
            let offset = full_v.len() - size_of::<u64>();
            full_v[offset..].copy_from_slice(&new_idx.to_le_bytes());
            self.set_raw(&full_k, &full_v)?;

            // remove old chain
            self.remove_raw(bytes_of(&ChainKey {
                idx,
                list_ph,
                namespace: CHAIN_NAMESPACE,
            }))?;

            new_idx += 1;
        }

        if list.tail_idx == new_idx {
            // list is now empty
            self.remove_raw(&list_key)?;
        } else {
            // update list head and tail, set holes=0
            self.set_raw(
                &list_key,
                bytes_of(&List {
                    head_idx: list.tail_idx,
                    tail_idx: new_idx,
                    holes: 0,
                }),
            )?;
        }

        Ok(true)
    }

    /// Iterates over the elements of the list (identified by `list_key`) from the beginning (head)
    /// to the end (tail). Note that if items are removed at random locations in the list, the iterator
    /// will need to skip these holes. If you remove elements from the middle (not head/tail) of the list
    /// frequently, and wish to use iteration, consider compacting the list every so often using
    /// [Self::compact_list_if_needed]
    pub fn iter_list<'a, B: AsRef<[u8]> + ?Sized>(&'a self, list_key: &B) -> ListIterator {
        self.owned_iter_list(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::iter_list]
    pub fn owned_iter_list<'a>(&'a self, list_key: Vec<u8>) -> ListIterator {
        let (list_ph, list_key) = self.make_list_key(list_key);
        ListIterator {
            store: &self,
            list_key,
            list_ph,
            list: None,
            idx: 0,
            fwd: true,
        }
    }

    /// Same as [Self::iter_list] but iterates from the end (tail) to the beginning (head)
    pub fn iter_list_backwards<'a, B: AsRef<[u8]> + ?Sized>(
        &'a self,
        list_key: &B,
    ) -> ListIterator {
        self.owned_iter_list_backwards(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::iter_list_backwards]
    pub fn owned_iter_list_backwards<'a>(&'a self, list_key: Vec<u8>) -> ListIterator {
        let (list_ph, list_key) = self.make_list_key(list_key);
        ListIterator {
            store: &self,
            list_key,
            list_ph,
            list: None,
            idx: 0,
            fwd: false,
        }
    }

    /// Discards the given list, removing all elements it contains and dropping the list itself.
    /// This is more efficient than iteration + removal of each element.
    pub fn discard_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<()> {
        self.owned_discard_list(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::discard_list]
    pub fn owned_discard_list(&self, list_key: Vec<u8>) -> Result<()> {
        let (list_ph, list_key) = self.make_list_key(list_key);
        let _guard = self.lock_list(list_ph);

        let Some(list_bytes) = self.get_raw(&list_key)? else {
            return Ok(());
        };
        let list = *from_bytes::<List>(&list_bytes);
        for idx in list.head_idx..list.tail_idx {
            let Some((_, full_key, _)) = self.get_from_list_at_index(list_ph, idx, false)? else {
                continue;
            };
            self.remove_raw(bytes_of(&ChainKey {
                list_ph,
                idx,
                namespace: CHAIN_NAMESPACE,
            }))?;
            self.remove_raw(&full_key)?;
        }
        self.remove_raw(&list_key)?;

        Ok(())
    }

    /// Returns the first (head) element of the list
    pub fn peek_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_peek_list_head(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::peek_list_head]
    pub fn owned_peek_list_head(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        let Some(kv) = self.owned_iter_list(list_key).next() else {
            return Ok(None);
        };
        Ok(Some(kv?))
    }

    /// Returns the last (tail) element of the list
    pub fn peek_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_peek_list_tail(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::peek_list_tail]
    pub fn owned_peek_list_tail(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        for kv in self.owned_iter_list_backwards(list_key) {
            return Ok(Some(kv?));
        }
        Ok(None)
    }

    /// Removes and returns the first (head) element of the list
    pub fn pop_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_pop_list_head(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::peek_list_tail]
    pub fn owned_pop_list_head(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        for kv in self.owned_iter_list(list_key.clone()) {
            let (k, v) = kv?;
            if let Some(_) = self.owned_remove_from_list(list_key.clone(), k.clone())? {
                return Ok(Some((k, v)));
            }
        }
        Ok(None)
    }

    /// Removes and returns the last (tail) element of the list
    pub fn pop_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_pop_list_tail(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::peek_list_tail]
    pub fn owned_pop_list_tail(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        for kv in self.owned_iter_list_backwards(list_key.clone()) {
            let (k, v) = kv?;
            if let Some(_) = self.owned_remove_from_list(list_key.clone(), k.clone())? {
                return Ok(Some((k, v)));
            }
        }
        Ok(None)
    }

    fn owned_push_to_list(
        &self,
        list_key: Vec<u8>,
        val: Vec<u8>,
        pos: InsertToListPos,
    ) -> Result<EncodableUuid> {
        let uuid = Uuid::from_bytes(rand::random());
        let res = self._insert_to_list(
            list_key,
            uuid.as_bytes().to_vec(),
            val,
            InsertMode::GetOrCreate,
            pos,
        )?;
        debug_assert!(matches!(res, InsertToListStatus::Created(_)));
        Ok(EncodableUuid::from(uuid))
    }

    /// Pushed "value only" at the beginning (head) of the list. The key is actually a randomly-generated UUID,
    /// which is returned to the caller and can be acted up like a regular list item. This is used to implement
    /// double-ended queues, where elements are pushed/popped at the ends, thus the key is not meaningful.
    pub fn push_to_list_head<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        val: &B2,
    ) -> Result<EncodableUuid> {
        self.owned_push_to_list_head(list_key.as_ref().to_owned(), val.as_ref().to_owned())
    }

    /// Owned version of [Self::push_to_list_head]
    pub fn owned_push_to_list_head(
        &self,
        list_key: Vec<u8>,
        val: Vec<u8>,
    ) -> Result<EncodableUuid> {
        self.owned_push_to_list(list_key, val, InsertToListPos::Head)
    }

    /// Pushed "value only" at the end (tail) of the list. The key is actually a randomly-generated UUID,
    /// which is returned to the caller and can be acted up like a regular list item. This is used to implement
    /// double-ended queues, where elements are pushed/popped at the ends, thus the key is not meaningful.
    pub fn push_to_list_tail<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        val: &B2,
    ) -> Result<EncodableUuid> {
        self.owned_push_to_list_tail(list_key.as_ref().to_owned(), val.as_ref().to_owned())
    }

    /// Owned version of [Self::push_to_list_tail]
    pub fn owned_push_to_list_tail(
        &self,
        list_key: Vec<u8>,
        val: Vec<u8>,
    ) -> Result<EncodableUuid> {
        self.owned_push_to_list(list_key, val, InsertToListPos::Tail)
    }
}
