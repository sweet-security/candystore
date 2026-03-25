use siphasher::sip::SipHasher13;
use smallvec::SmallVec;

use std::{hash::Hasher, ops::Range};

use crate::{
    internal::{KeyNamespace, RangeMetadata, aligned_data_entry_size},
    store::CandyStore,
    types::{
        Error, GetOrCreateStatus, ListCompactionParams, MAX_USER_KEY_SIZE, MAX_USER_VALUE_SIZE,
        ReplaceStatus, Result, SetStatus,
    },
};

/// A list item as `(item_key, value)`.
pub type KVPair = (Vec<u8>, Vec<u8>);

#[derive(Clone, Copy)]
pub(super) struct ListNamespaces {
    pub(super) meta: KeyNamespace,
    pub(super) index: KeyNamespace,
    pub(super) data: KeyNamespace,
}

const LIST_NS: ListNamespaces = ListNamespaces {
    meta: KeyNamespace::ListMeta,
    index: KeyNamespace::ListIndex,
    data: KeyNamespace::ListData,
};

/// Double-ended iterator over live list items in logical order.
pub struct ListIterator<'a> {
    store: &'a CandyStore,
    list: Vec<u8>,
    ns: ListNamespaces,
    next_idx: u64,
    end_idx: u64,
    initial_next_idx: u64,
    initial_end_idx: u64,
}

type ListMetadata = RangeMetadata;

impl ListIterator<'_> {
    fn heal_head(&self, new_head: u64) {
        let _ = self.try_heal_head(new_head);
    }

    fn try_heal_head(&self, new_head: u64) -> Result<()> {
        let _lock = self.store.logical_write_guard(self.ns.meta, &self.list);
        let mut meta = get_list_meta(self.store, self.ns, &self.list)?;
        if meta.head >= self.initial_next_idx && meta.head < new_head {
            meta.head = new_head;
            set_list_meta(self.store, self.ns, &self.list, meta)?;
        }
        Ok(())
    }

    fn heal_tail(&self, new_tail: u64) {
        let _ = self.try_heal_tail(new_tail);
    }

    fn try_heal_tail(&self, new_tail: u64) -> Result<()> {
        let _lock = self.store.logical_write_guard(self.ns.meta, &self.list);
        let mut meta = get_list_meta(self.store, self.ns, &self.list)?;
        if meta.tail <= self.initial_end_idx && meta.tail > new_tail {
            meta.tail = new_tail;
            set_list_meta(self.store, self.ns, &self.list, meta)?;
        }
        Ok(())
    }
}

impl Iterator for ListIterator<'_> {
    type Item = Result<KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_idx <= self.end_idx {
            let idx = self.next_idx;
            self.next_idx += 1;

            if idx > self.initial_next_idx + 1000 {
                self.heal_head(idx);
                self.initial_next_idx = idx;
            }

            let idx_key = make_list_index_key(&self.list, idx);
            let key = match self.store.get_ns(self.ns.index, &idx_key) {
                Ok(Some(key)) => key,
                Ok(None) => continue,
                Err(err) => return Some(Err(err)),
            };

            let data_key = make_list_data_key(&self.list, &key);
            let value = match self.store.get_ns(self.ns.data, &data_key) {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(err) => return Some(Err(err)),
            };

            return Some(Ok((key, strip_idx_suffix(value))));
        }

        None
    }
}

impl DoubleEndedIterator for ListIterator<'_> {
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        while self.next_idx <= self.end_idx {
            let idx = self.end_idx;
            if self.end_idx == 0 {
                self.next_idx = 1;
            } else {
                self.end_idx -= 1;
            }

            if idx + 1000 < self.initial_end_idx {
                self.heal_tail(idx);
                self.initial_end_idx = idx;
            }

            let idx_key = make_list_index_key(&self.list, idx);
            let key = match self.store.get_ns(self.ns.index, &idx_key) {
                Ok(Some(key)) => key,
                Ok(None) => continue,
                Err(err) => return Some(Err(err)),
            };

            let data_key = make_list_data_key(&self.list, &key);
            let value = match self.store.get_ns(self.ns.data, &data_key) {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(err) => return Some(Err(err)),
            };

            return Some(Ok((key, strip_idx_suffix(value))));
        }

        None
    }
}

impl CandyStore {
    /// Inserts or replaces `item_key` in `list_key`, placing the item at the tail.
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
        let previous = self.list_set_at_tail_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
        )?;
        Ok(match previous {
            Some(previous) => SetStatus::PrevValue(previous),
            None => SetStatus::CreatedNew,
        })
    }

    /// Inserts or replaces `item_key` in `list_key`, moving it to the logical tail.
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
        let previous =
            self.list_promote_with_ns(LIST_NS, list_key.as_ref(), item_key.as_ref(), val.as_ref())?;
        Ok(match previous {
            Some(previous) => SetStatus::PrevValue(previous),
            None => SetStatus::CreatedNew,
        })
    }

    /// Replaces a list item only if its current value matches `expected_val` when provided.
    pub fn replace_in_list<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
        B4: AsRef<[u8]> + ?Sized,
    >(
        &self,
        list_key: &B1,
        item_key: &B2,
        val: &B3,
        expected_val: Option<&B4>,
    ) -> Result<ReplaceStatus> {
        self.list_replace_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
            expected_val.map(|expected| expected.as_ref()),
        )
    }

    /// Returns the current list item value, or inserts `default_val` if the item is missing.
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
        self.list_get_or_create_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            default_val.as_ref(),
        )
    }

    /// Returns the current value for `item_key` in `list_key`, if present.
    pub fn get_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.list_get_with_ns(LIST_NS, list_key.as_ref(), item_key.as_ref())
    }

    /// Removes `item_key` from `list_key` and returns its previous value if it existed.
    pub fn remove_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.list_remove_with_ns(LIST_NS, list_key.as_ref(), item_key.as_ref())
    }

    /// Compacts list storage when `params` indicate enough holes exist to justify rewriting it.
    pub fn compact_list_if_needed<B: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B,
        params: ListCompactionParams,
    ) -> Result<bool> {
        self.list_compact_with_ns(LIST_NS, list_key.as_ref(), params)
    }

    /// Iterates over live items in `list_key` from head to tail.
    pub fn iter_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> ListIterator<'_> {
        self.list_iter_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Removes all items in `list_key`.
    pub fn discard_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<bool> {
        self.list_discard_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Returns the head item of `list_key` without removing it.
    pub fn peek_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.peek_list_head_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Returns the tail item of `list_key` without removing it.
    pub fn peek_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.peek_list_tail_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Removes and returns the head item of `list_key`.
    pub fn pop_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.pop_list_head_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Removes and returns the tail item of `list_key`.
    pub fn pop_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.pop_list_tail_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Returns the number of live items in `list_key`.
    pub fn list_len<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<usize> {
        self.list_len_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Retains only items for which `func` returns `true`, preserving list order.
    pub fn retain_in_list<B: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B,
        func: impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        self.list_retain_with_ns(LIST_NS, list_key.as_ref(), func)
    }

    pub(super) fn list_retain_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
        mut func: impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        let _lock = self.logical_write_guard(ns.meta, list_key);
        let mut meta = get_list_meta(self, ns, list_key)?;
        if meta.count == 0 {
            return Ok(());
        }

        let original_head = meta.head;
        let original_tail = meta.tail;
        let mut new_tail = meta.tail;
        let mut retained_count = 0u64;

        for idx in original_head..=original_tail {
            let idx_key = make_list_index_key(list_key, idx);
            let key = match self.get_ns(ns.index, &idx_key)? {
                Some(key) => key,
                None => continue,
            };

            let data_key = make_list_data_key(list_key, &key);
            let val_with_idx = match self.get_ns(ns.data, &data_key)? {
                Some(value) => value,
                None => {
                    self.remove_ns(ns.index, &idx_key)?;
                    continue;
                }
            };
            let value = strip_idx_suffix(val_with_idx);

            self.remove_ns(ns.index, &idx_key)?;

            if func(&key, &value)? {
                new_tail += 1;
                let new_value = append_idx_suffix(&value, new_tail);
                self.set_ns(ns.data, &data_key, &new_value)?;

                let new_idx_key = make_list_index_key(list_key, new_tail);
                self.set_ns(ns.index, &new_idx_key, &key)?;
                retained_count += 1;
            } else {
                self.remove_ns(ns.data, &data_key)?;
            }
        }

        if retained_count == 0 {
            meta = ListMetadata::new();
        } else {
            meta.head = original_tail + 1;
            meta.tail = new_tail;
            meta.count = retained_count;
        }

        set_list_meta(self, ns, list_key, meta)?;
        Ok(())
    }

    pub(super) fn list_set_at_tail_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.validate_list_item_sizes(list, key, value)?;
        let _lock = self.logical_write_guard(ns.meta, list);

        let mut meta = get_list_meta(self, ns, list)?;
        let data_key = make_list_data_key(list, key);

        if let Some(existing) = self.get_ns(ns.data, &data_key)? {
            let idx = extract_idx_suffix(&existing);
            let new_value = append_idx_suffix(value, idx);
            let old_with_idx = self.set_ns(ns.data, &data_key, &new_value)?;

            // Always write the index entry: after a crash the OS may have
            // flushed the metadata update but not the corresponding index
            // write, leaving the entry invisible in the list.
            let idx_key = make_list_index_key(list, idx);
            self.set_ns(ns.index, &idx_key, key)?;

            if meta.count == 0 || idx > meta.tail {
                if meta.count == 0 {
                    meta.head = idx;
                }
                if idx > meta.tail {
                    meta.tail = idx;
                }
                meta.count += 1;
                set_list_meta(self, ns, list, meta)?;
            }

            return Ok(old_with_idx.map(strip_idx_suffix));
        }

        let new_idx = meta.tail + 1;
        let value_with_idx = append_idx_suffix(value, new_idx);
        self.set_ns(ns.data, &data_key, &value_with_idx)?;

        let idx_key = make_list_index_key(list, new_idx);
        self.set_ns(ns.index, &idx_key, key)?;

        if meta.count == 0 {
            meta.head = new_idx;
        }
        meta.tail = new_idx;
        meta.count += 1;
        set_list_meta(self, ns, list, meta)?;

        Ok(None)
    }

    pub(super) fn list_replace_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
        expected: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        self.validate_list_item_sizes(list, key, value)?;
        let _lock = self.logical_write_guard(ns.meta, list);

        let data_key = make_list_data_key(list, key);
        let Some(existing_value) = self.get_ns(ns.data, &data_key)? else {
            return Ok(ReplaceStatus::DoesNotExist);
        };

        let previous = strip_idx_suffix(existing_value.clone());
        if let Some(expected) = expected
            && previous != expected
        {
            return Ok(ReplaceStatus::WrongValue(previous));
        }

        let idx = extract_idx_suffix(&existing_value);
        let new_value = append_idx_suffix(value, idx);
        self.set_ns(ns.data, &data_key, &new_value)?;
        Ok(ReplaceStatus::PrevValue(previous))
    }

    pub(super) fn list_get_or_create_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<GetOrCreateStatus> {
        self.validate_list_item_sizes(list, key, value)?;
        let _lock = self.logical_write_guard(ns.meta, list);

        let data_key = make_list_data_key(list, key);
        if let Some(existing) = self.get_ns(ns.data, &data_key)? {
            return Ok(GetOrCreateStatus::ExistingValue(strip_idx_suffix(existing)));
        }

        let mut meta = get_list_meta(self, ns, list)?;
        let new_idx = meta.tail + 1;
        let value_with_idx = append_idx_suffix(value, new_idx);
        self.set_ns(ns.data, &data_key, &value_with_idx)?;

        let idx_key = make_list_index_key(list, new_idx);
        self.set_ns(ns.index, &idx_key, key)?;

        if meta.count == 0 {
            meta.head = new_idx;
        }
        meta.tail = new_idx;
        meta.count += 1;
        set_list_meta(self, ns, list, meta)?;

        Ok(GetOrCreateStatus::CreatedNew(value.to_vec()))
    }

    pub(super) fn list_promote_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.validate_list_item_sizes(list, key, value)?;
        let _lock = self.logical_write_guard(ns.meta, list);
        let mut meta = get_list_meta(self, ns, list)?;
        let data_key = make_list_data_key(list, key);

        let mut old_value = None;
        let mut old_idx_key = None;
        if let Some(existing) = self.get_ns(ns.data, &data_key)? {
            let idx = extract_idx_suffix(&existing);
            old_idx_key = Some(make_list_index_key(list, idx));
            old_value = Some(strip_idx_suffix(existing));
        } else {
            meta.count += 1;
        }

        let new_idx = meta.tail + 1;
        let value_with_idx = append_idx_suffix(value, new_idx);
        self.set_ns(ns.data, &data_key, &value_with_idx)?;

        let idx_key = make_list_index_key(list, new_idx);
        self.set_ns(ns.index, &idx_key, key)?;

        if let Some(old_idx_key) = old_idx_key {
            self.remove_ns(ns.index, &old_idx_key)?;
        }

        if meta.count == 1 {
            meta.head = new_idx;
        }
        meta.tail = new_idx;
        set_list_meta(self, ns, list, meta)?;

        Ok(old_value)
    }

    pub(super) fn list_get_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_read_guard(ns.meta, list);
        let data_key = make_list_data_key(list, key);
        Ok(self.get_ns(ns.data, &data_key)?.map(strip_idx_suffix))
    }

    pub(super) fn list_remove_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_write_guard(ns.meta, list);
        self._list_remove_with_ns(ns, list, key)
    }

    fn _list_remove_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut meta = get_list_meta(self, ns, list)?;
        let data_key = make_list_data_key(list, key);
        let removed = match self.remove_ns(ns.data, &data_key)? {
            Some(value) => value,
            None => return Ok(None),
        };

        let idx = extract_idx_suffix(&removed);
        let idx_key = make_list_index_key(list, idx);
        self.remove_ns(ns.index, &idx_key)?;

        let old_value = Some(strip_idx_suffix(removed));
        meta.count = meta.count.saturating_sub(1);

        if meta.count == 0 {
            meta = ListMetadata::new();
        } else {
            let mut check_head = idx == meta.head;
            if !check_head {
                let head_key = make_list_index_key(list, meta.head);
                if self.get_ns(ns.index, &head_key)?.is_none() {
                    check_head = true;
                }
            }

            if check_head {
                let mut new_head = meta.head;
                loop {
                    if new_head > meta.tail {
                        meta = ListMetadata::new();
                        break;
                    }
                    if new_head == idx {
                        new_head += 1;
                        continue;
                    }
                    let probe_idx_key = make_list_index_key(list, new_head);
                    if self.get_ns(ns.index, &probe_idx_key)?.is_some() {
                        meta.head = new_head;
                        break;
                    }
                    new_head += 1;
                }
            }

            if meta.count > 0 {
                let mut check_tail = idx == meta.tail;
                if !check_tail {
                    let tail_key = make_list_index_key(list, meta.tail);
                    if self.get_ns(ns.index, &tail_key)?.is_none() {
                        check_tail = true;
                    }
                }

                if check_tail {
                    let mut new_tail = meta.tail;
                    loop {
                        if new_tail < meta.head {
                            meta = ListMetadata::new();
                            break;
                        }
                        if new_tail == idx {
                            if new_tail == 0 {
                                break;
                            }
                            new_tail -= 1;
                            continue;
                        }
                        let probe_idx_key = make_list_index_key(list, new_tail);
                        if self.get_ns(ns.index, &probe_idx_key)?.is_some() {
                            meta.tail = new_tail;
                            break;
                        }
                        if new_tail == 0 {
                            meta = ListMetadata::new();
                            break;
                        }
                        new_tail -= 1;
                    }
                }
            }
        }

        set_list_meta(self, ns, list, meta)?;
        Ok(old_value)
    }

    pub(super) fn list_discard_with_ns(&self, ns: ListNamespaces, list: &[u8]) -> Result<bool> {
        let _lock = self.logical_write_guard(ns.meta, list);
        let meta = get_list_meta(self, ns, list)?;
        if meta.count == 0 {
            return Ok(false);
        }

        for idx in meta.head..=meta.tail {
            let idx_key = make_list_index_key(list, idx);
            if let Some(key) = self.remove_ns(ns.index, &idx_key)? {
                let data_key = make_list_data_key(list, &key);
                self.remove_ns(ns.data, &data_key)?;
            }
        }

        self.remove_ns(ns.meta, list)?;
        Ok(true)
    }

    pub(super) fn list_compact_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        params: ListCompactionParams,
    ) -> Result<bool> {
        let _lock = self.logical_write_guard(ns.meta, list);
        let mut meta = get_list_meta(self, ns, list)?;
        if meta.count == 0 {
            return Ok(false);
        }

        let span = if meta.tail >= meta.head {
            meta.tail - meta.head + 1
        } else {
            0
        };
        if span == 0 || span < params.min_length {
            return Ok(false);
        }

        let holes_ratio = (span - meta.count) as f64 / span as f64;
        if holes_ratio < params.min_holes_ratio {
            return Ok(false);
        }

        let limit = meta.tail;
        while meta.head <= limit {
            let idx_key = make_list_index_key(list, meta.head);
            if let Some(key) = self.get_ns(ns.index, &idx_key)? {
                let data_key = make_list_data_key(list, &key);
                if let Some(value_with_idx) = self.get_ns(ns.data, &data_key)? {
                    let value = strip_idx_suffix(value_with_idx);

                    meta.tail += 1;
                    let new_idx = meta.tail;
                    let new_value_with_idx = append_idx_suffix(&value, new_idx);
                    // Overwrite data in-place (not remove+set) to avoid data
                    // loss if a crash occurs between the two operations
                    self.set_ns(ns.data, &data_key, &new_value_with_idx)?;

                    // Write new index before removing old so the entry is
                    // always reachable via at least one index position
                    let new_idx_key = make_list_index_key(list, new_idx);
                    self.set_ns(ns.index, &new_idx_key, &key)?;
                }
                self.remove_ns(ns.index, &idx_key)?;
            }
            meta.head += 1;
            set_list_meta(self, ns, list, meta)?;
        }

        Ok(true)
    }

    fn peek_list_head_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        self.list_iter_with_ns(ns, list_key).next().transpose()
    }

    fn peek_list_tail_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        self.list_iter_with_ns(ns, list_key).next_back().transpose()
    }

    pub(super) fn pop_list_head_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        let _lock = self.logical_write_guard(ns.meta, list_key);
        let head = self.peek_list_head_with_ns(ns, list_key)?;
        if let Some((key, _)) = head
            && let Some(value) = self._list_remove_with_ns(ns, list_key, &key)?
        {
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    pub(super) fn pop_list_tail_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        let _lock = self.logical_write_guard(ns.meta, list_key);
        let tail = self.peek_list_tail_with_ns(ns, list_key)?;
        if let Some((key, _)) = tail
            && let Some(value) = self._list_remove_with_ns(ns, list_key, &key)?
        {
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    pub(super) fn list_iter_with_ns<'a>(
        &'a self,
        ns: ListNamespaces,
        list: &[u8],
    ) -> ListIterator<'a> {
        let meta = get_list_meta(self, ns, list).unwrap_or_else(|_| ListMetadata::new());
        ListIterator {
            store: self,
            list: list.to_vec(),
            ns,
            next_idx: meta.head,
            end_idx: meta.tail,
            initial_next_idx: meta.head,
            initial_end_idx: meta.tail,
        }
    }

    pub(super) fn list_len_with_ns(&self, ns: ListNamespaces, list: &[u8]) -> Result<usize> {
        Ok(get_list_meta(self, ns, list)?.count as usize)
    }

    pub(super) fn list_range_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
    ) -> Result<Range<usize>> {
        let meta = get_list_meta(self, ns, list)?;
        if meta.head > meta.tail {
            return Ok(0..0);
        }
        Ok(meta.head as usize..meta.tail.saturating_add(1) as usize)
    }

    fn validate_list_item_sizes(&self, list: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        let data_key_len = make_list_data_key(list, key).len();
        let data_value_len = value.len() + size_of::<u64>();
        validate_internal_entry(self, data_key_len, data_value_len)?;

        let index_key_len = make_list_index_key(list, 0).len();
        validate_internal_entry(self, index_key_len, key.len())
    }
}

fn validate_internal_entry(store: &CandyStore, key_len: usize, value_len: usize) -> Result<()> {
    let entry_size = aligned_data_entry_size(key_len, value_len) as usize;
    if key_len > MAX_USER_KEY_SIZE
        || value_len > MAX_USER_VALUE_SIZE
        || entry_size > store.inner.config.max_data_file_size as usize
    {
        return Err(Error::PayloadTooLarge(entry_size));
    }
    Ok(())
}

fn get_list_meta(store: &CandyStore, ns: ListNamespaces, list: &[u8]) -> Result<ListMetadata> {
    if let Some(value) = store.get_ns(ns.meta, list)?
        && let Some(meta) = ListMetadata::from_bytes(&value)
    {
        return Ok(meta);
    }
    Ok(ListMetadata::new())
}

fn set_list_meta(
    store: &CandyStore,
    ns: ListNamespaces,
    list: &[u8],
    meta: ListMetadata,
) -> Result<()> {
    store.set_ns(ns.meta, list, &meta.to_bytes())?;
    Ok(())
}

fn hash_list_key(list: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(0x7ac1485be800c70e, 0x22ac1dcc7992c592);
    hasher.write(list);
    hasher.finish()
}

fn make_list_data_key(list: &[u8], key: &[u8]) -> SmallVec<[u8; 128]> {
    let hash = hash_list_key(list);
    let mut out = SmallVec::<[u8; 128]>::with_capacity(8 + key.len());
    out.extend_from_slice(&hash.to_le_bytes());
    out.extend_from_slice(key);
    out
}

fn make_list_index_key(list: &[u8], idx: u64) -> SmallVec<[u8; 16]> {
    let hash = hash_list_key(list);
    let mut out = SmallVec::<[u8; 16]>::with_capacity(16);
    out.extend_from_slice(&hash.to_le_bytes());
    out.extend_from_slice(&idx.to_be_bytes());
    out
}

fn append_idx_suffix(value: &[u8], idx: u64) -> SmallVec<[u8; 128]> {
    let mut out = SmallVec::<[u8; 128]>::with_capacity(value.len() + size_of::<u64>());
    out.extend_from_slice(value);
    out.extend_from_slice(&idx.to_le_bytes());
    out
}

fn strip_idx_suffix(mut value: Vec<u8>) -> Vec<u8> {
    if value.len() >= size_of::<u64>() {
        value.truncate(value.len() - size_of::<u64>());
    }
    value
}

fn extract_idx_suffix(value: &[u8]) -> u64 {
    let n = value.len();
    if n < size_of::<u64>() {
        return 0;
    }
    u64::from_le_bytes(value[n - size_of::<u64>()..n].try_into().unwrap())
}
