use siphasher::sip::SipHasher13;
use std::hash::Hasher;
use std::ops::Range;

use crate::store::CandyStore;
use crate::types::{LIST_NS, ListNamespaces, Result};
use crate::{GetOrCreateStatus, ReplaceStatus, SetStatus};

pub type KVPair = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone)]
/// Parameters for list compaction.
pub struct ListCompactionParams {
    /// Minimum number of items in the list to consider compaction.
    pub min_length: u64,
    /// Minimum ratio of holes (deleted items) to total span to trigger compaction.
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

impl CandyStore {
    /// Sets an item in a list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item within the list.
    /// * `val` - The value to store.
    ///
    /// # Returns
    ///
    /// The status of the operation (whether a new item was created or an existing one replaced).
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
        self.ensure_user_value_len(val.as_ref().len())?;
        let prev = self.list_set_at_tail_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
        )?;
        Ok(match prev {
            Some(p) => SetStatus::PrevValue(p),
            None => SetStatus::CreatedNew,
        })
    }

    /// Sets an item in a list and promotes it to the head (most recently used).
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    /// * `val` - The value to store.
    ///
    /// # Returns
    ///
    /// The status of the operation (whether a new item was created or an existing one replaced).
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
        self.ensure_user_value_len(val.as_ref().len())?;
        let prev = self.list_set_at_head_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
        )?;
        Ok(match prev {
            Some(p) => SetStatus::PrevValue(p),
            None => SetStatus::CreatedNew,
        })
    }

    /// Replaces an item in a list only if it matches an expected value.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    /// * `val` - The new value.
    /// * `expected_val` - If provided, replacement only happens if the current value matches this.
    ///
    /// # Returns
    ///
    /// The status of the replacement operation.
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
        self.ensure_user_value_len(val.as_ref().len())?;
        self.list_replace_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            val.as_ref(),
            expected_val.map(|v| v.as_ref()),
        )
    }

    /// Retrieves an item from a list, or creates it if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    /// * `default_val` - The value to set if the item does not exist.
    ///
    /// # Returns
    ///
    /// The status of the operation, containing the value (either existing or newly created).
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
        self.ensure_user_value_len(default_val.as_ref().len())?;
        self.list_get_or_create_with_ns(
            LIST_NS,
            list_key.as_ref(),
            item_key.as_ref(),
            default_val.as_ref(),
        )
    }

    /// Retrieves an item from a list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if it does not exist.
    pub fn get_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.list_get_with_ns(LIST_NS, list_key.as_ref(), item_key.as_ref())
    }

    /// Removes an item from a list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `item_key` - The key identifying the item.
    ///
    /// # Returns
    ///
    /// The value of the removed item, or `None` if it did not exist.
    pub fn remove_from_list<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        self.list_remove_with_ns(LIST_NS, list_key.as_ref(), item_key.as_ref())
    }

    /// Compacts a list if it meets the criteria specified in `params`.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `params` - Parameters controlling when compaction should occur.
    ///
    /// # Returns
    ///
    /// `true` if compaction was performed, `false` otherwise.
    pub fn compact_list_if_needed<B: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B,
        params: ListCompactionParams,
    ) -> Result<bool> {
        self.list_compact_with_ns(LIST_NS, list_key.as_ref(), params)
    }

    /// Iterates over a list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// An iterator over the items in the list.
    pub fn iter_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> ListIterator<'_> {
        self.list_iter_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Discards a list, removing all its items.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// `true` if the list existed and was removed, `false` otherwise.
    pub fn discard_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<bool> {
        self.list_discard_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Peeks at the head (oldest item) of the list without removing it.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the head of the list, or `None` if the list is empty.
    pub fn peek_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.peek_list_head_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Peeks at the tail (newest item) of the list without removing it.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the tail of the list, or `None` if the list is empty.
    pub fn peek_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.peek_list_tail_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Removes and returns the head (oldest item) of the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the head of the list, or `None` if the list is empty.
    pub fn pop_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.pop_list_head_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Removes and returns the tail (newest item) of the list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The key-value pair at the tail of the list, or `None` if the list is empty.
    pub fn pop_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.pop_list_tail_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Returns the number of items in a list.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    ///
    /// # Returns
    ///
    /// The number of items in the list.
    pub fn list_len<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<usize> {
        self.list_len_with_ns(LIST_NS, list_key.as_ref())
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// # Arguments
    ///
    /// * `list_key` - The key identifying the list.
    /// * `func` - A closure that returns `true` if the element should be retained, `false` otherwise.
    ///   The closure receives the key and value of the item.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    pub fn retain_in_list<B: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B,
        func: impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        self.list_retain_with_ns(LIST_NS, list_key.as_ref(), func)
    }

    pub(crate) fn list_retain_with_ns(
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

        let mut new_count = 0;
        let mut first_retained_idx = None;
        let mut last_retained_idx = None;
        let mut removed_count_since_last_save = 0;
        let mut last_save_idx = meta.head;

        for idx in meta.head..=meta.tail {
            let idx_key = make_list_index_key(list_key, idx);
            let key = match self._get(ns.index, &idx_key)? {
                Some(k) => k,
                None => continue,
            };

            let data_key = make_list_data_key(list_key, &key);
            let val_with_idx = match self._get(ns.data, &data_key)? {
                Some(v) => v,
                None => {
                    // Inconsistent state: index exists but data missing.
                    // Clean up index and treat as hole.
                    self._remove(ns.index, &idx_key)?;
                    continue;
                }
            };
            let val = strip_idx_suffix(val_with_idx);

            if func(&key, &val)? {
                new_count += 1;
                if first_retained_idx.is_none() {
                    first_retained_idx = Some(idx);
                }
                last_retained_idx = Some(idx);
            } else {
                self._remove(ns.index, &idx_key)?;
                self._remove(ns.data, &data_key)?;
                if first_retained_idx.is_none() {
                    removed_count_since_last_save += 1;
                }
            }

            if first_retained_idx.is_none() && idx > last_save_idx + 1000 {
                meta.head = idx + 1;
                meta.count = meta.count.saturating_sub(removed_count_since_last_save);
                set_list_meta(self, ns, list_key, meta)?;
                removed_count_since_last_save = 0;
                last_save_idx = idx;
            }
        }

        if new_count == 0 {
            meta = ListMetadata::new();
        } else {
            meta.count = new_count;
            if let Some(head) = first_retained_idx {
                meta.head = head;
            }
            if let Some(tail) = last_retained_idx {
                meta.tail = tail;
            }
        }
        set_list_meta(self, ns, list_key, meta)?;
        Ok(())
    }

    pub(crate) fn list_set_at_tail_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_write_guard(ns.meta, list);

        let mut meta = get_list_meta(self, ns, list)?;
        let data_key = make_list_data_key(list, key);

        if let Some(existing) = self._get(ns.data, &data_key)? {
            let idx = extract_idx_suffix(&existing);
            let new_val = append_idx_suffix(value, idx);
            let old_with = self._set(ns.data, &data_key, &new_val)?;

            if meta.count == 0 || idx > meta.tail {
                let idx_key = make_list_index_key(list, idx);
                self._set(ns.index, &idx_key, key)?;

                if meta.count == 0 {
                    meta.head = idx;
                }
                if idx > meta.tail {
                    meta.tail = idx;
                }
                meta.count += 1;
                set_list_meta(self, ns, list, meta)?;
            }

            return Ok(old_with.map(strip_idx_suffix));
        }

        let new_idx = meta.tail + 1;
        let val_with_idx = append_idx_suffix(value, new_idx);
        self._set(ns.data, &data_key, &val_with_idx)?;

        let idx_key = make_list_index_key(list, new_idx);
        self._set(ns.index, &idx_key, key)?;

        if meta.count == 0 {
            meta.head = new_idx;
        }
        meta.tail = new_idx;
        meta.count += 1;
        set_list_meta(self, ns, list, meta)?;

        Ok(None)
    }

    pub(crate) fn list_replace_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
        expected: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        let _lock = self.logical_write_guard(ns.meta, list);

        let data_key = make_list_data_key(list, key);
        let existing = self._get(ns.data, &data_key)?;

        let Some(existing_val_with_idx) = existing else {
            return Ok(ReplaceStatus::DoesNotExist);
        };

        let prev = strip_idx_suffix(existing_val_with_idx.clone());

        if let Some(expected_val) = expected
            && prev != expected_val
        {
            return Ok(ReplaceStatus::WrongValue(prev));
        }

        let idx = extract_idx_suffix(&existing_val_with_idx);
        let new_val_with_idx = append_idx_suffix(value, idx);
        self._set(ns.data, &data_key, &new_val_with_idx)?;

        Ok(ReplaceStatus::PrevValue(prev))
    }

    pub(crate) fn list_get_or_create_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<GetOrCreateStatus> {
        let _lock = self.logical_write_guard(ns.meta, list);

        let data_key = make_list_data_key(list, key);

        if let Some(existing) = self._get(ns.data, &data_key)? {
            return Ok(GetOrCreateStatus::ExistingValue(strip_idx_suffix(existing)));
        }

        let mut meta = get_list_meta(self, ns, list)?;

        let new_idx = meta.tail + 1;
        let val_with_idx = append_idx_suffix(value, new_idx);
        self._set(ns.data, &data_key, &val_with_idx)?;

        let idx_key = make_list_index_key(list, new_idx);
        self._set(ns.index, &idx_key, key)?;

        if meta.count == 0 {
            meta.head = new_idx;
        }
        meta.tail = new_idx;
        meta.count += 1;
        set_list_meta(self, ns, list, meta)?;

        Ok(GetOrCreateStatus::CreatedNew(value.to_vec()))
    }

    pub(crate) fn list_set_at_head_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_write_guard(ns.meta, list);
        self._list_set_at_head_with_ns(ns, list, key, value)
    }

    fn _list_set_at_head_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut meta = get_list_meta(self, ns, list)?;
        let data_key = make_list_data_key(list, key);

        let mut old_val = None;
        if let Some(existing) = self._get(ns.data, &data_key)? {
            let idx = extract_idx_suffix(&existing);
            let idx_key = make_list_index_key(list, idx);
            self._remove(ns.index, &idx_key)?;
            old_val = Some(strip_idx_suffix(existing));
        } else {
            meta.count += 1;
        }

        let new_idx = meta.head - 1;
        let val_with_idx = append_idx_suffix(value, new_idx);
        self._set(ns.data, &data_key, &val_with_idx)?;

        let idx_key = make_list_index_key(list, new_idx);
        self._set(ns.index, &idx_key, key)?;

        meta.head = new_idx;
        if meta.count == 1 {
            meta.tail = new_idx;
        }
        set_list_meta(self, ns, list, meta)?;

        Ok(old_val)
    }

    pub(crate) fn list_get_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_read_guard(ns.meta, list);

        let data_key = make_list_data_key(list, key);
        Ok(self._get(ns.data, &data_key)?.map(strip_idx_suffix))
    }

    pub(crate) fn list_remove_with_ns(
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
        let removed = match self._remove(ns.data, &data_key)? {
            Some(v) => v,
            None => return Ok(None),
        };

        let idx = extract_idx_suffix(&removed);
        let idx_key = make_list_index_key(list, idx);
        let _ = self._remove(ns.index, &idx_key)?;

        let old = Some(strip_idx_suffix(removed));

        if meta.count > 0 {
            meta.count -= 1;
        }

        if meta.count == 0 {
            meta = ListMetadata::new();
        } else {
            let mut check_head = idx == meta.head;
            if !check_head {
                let head_key = make_list_index_key(list, meta.head);
                if self._get(ns.index, &head_key)?.is_none() {
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
                    if self._get(ns.index, &probe_idx_key)?.is_some() {
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
                    if self._get(ns.index, &tail_key)?.is_none() {
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
                        if self._get(ns.index, &probe_idx_key)?.is_some() {
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
        Ok(old)
    }

    pub(crate) fn list_discard_with_ns(&self, ns: ListNamespaces, list: &[u8]) -> Result<bool> {
        let _lock = self.logical_write_guard(ns.meta, list);
        let meta = get_list_meta(self, ns, list)?;
        if meta.count == 0 {
            return Ok(false);
        }

        for idx in meta.head..=meta.tail {
            let idx_key = make_list_index_key(list, idx);
            if let Some(key) = self._remove(ns.index, &idx_key)? {
                let data_key = make_list_data_key(list, &key);
                let _ = self._remove(ns.data, &data_key)?;
            }
        }

        let _ = self._remove(ns.meta, list)?;
        Ok(true)
    }

    pub(crate) fn list_compact_with_ns(
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

        if meta.count < params.min_length {
            return Ok(false);
        }

        let span = if meta.tail >= meta.head {
            meta.tail - meta.head + 1
        } else {
            0
        };
        if span == 0 {
            return Ok(false);
        }

        let occupancy = meta.count as f64 / span as f64;
        let min_occupancy = 1.0 - params.min_holes_ratio;

        if occupancy >= min_occupancy {
            // Heal metadata even if we decide not to compact
            let mut first = None;
            let mut last = None;
            let mut count = 0u64;
            for idx in meta.head..=meta.tail {
                let idx_key = make_list_index_key(list, idx);
                let key = match self._get(ns.index, &idx_key)? {
                    Some(k) => k,
                    None => continue,
                };

                let data_key = make_list_data_key(list, &key);
                if self._get(ns.data, &data_key)?.is_none() {
                    continue;
                }

                if first.is_none() {
                    first = Some(idx);
                }
                last = Some(idx);
                count += 1;
            }

            let mut new_meta = meta;
            if count == 0 {
                new_meta = ListMetadata::new();
            } else {
                new_meta.head = first.unwrap();
                new_meta.tail = last.unwrap();
                new_meta.count = count;
            }

            if new_meta.head != meta.head
                || new_meta.tail != meta.tail
                || new_meta.count != meta.count
            {
                set_list_meta(self, ns, list, new_meta)?;
                return Ok(true);
            }

            return Ok(false);
        }

        let limit = meta.tail;
        while meta.head <= limit {
            let idx_key = make_list_index_key(list, meta.head);
            if let Some(key) = self._remove(ns.index, &idx_key)? {
                let data_key = make_list_data_key(list, &key);
                if let Some(val_with_idx) = self._remove(ns.data, &data_key)? {
                    let val = strip_idx_suffix(val_with_idx);

                    meta.tail += 1;
                    let new_idx = meta.tail;

                    let new_val_with_idx = append_idx_suffix(&val, new_idx);
                    self._set(ns.data, &data_key, &new_val_with_idx)?;

                    let new_idx_key = make_list_index_key(list, new_idx);
                    self._set(ns.index, &new_idx_key, &key)?;
                }
            }
            meta.head += 1;
            set_list_meta(self, ns, list, meta)?;
        }

        Ok(true)
    }

    pub(crate) fn list_promote_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
        key: &[u8],
    ) -> Result<bool> {
        let _lock = self.logical_write_guard(ns.meta, list);
        let data_key = make_list_data_key(list, key);
        if let Some(val_with_idx) = self._get(ns.data, &data_key)? {
            let val = strip_idx_suffix(val_with_idx);
            self._list_set_at_head_with_ns(ns, list, key, &val)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn peek_list_head_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        self.list_iter_with_ns(ns, list_key).next().transpose()
    }

    pub(crate) fn peek_list_tail_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        self.list_iter_with_ns(ns, list_key).next_back().transpose()
    }

    pub(crate) fn pop_list_head_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        let _lock = self.logical_write_guard(ns.meta, list_key);
        let head = self.peek_list_head_with_ns(ns, list_key)?;
        if let Some((k, _)) = head
            && let Some(val) = self._list_remove_with_ns(ns, list_key, &k)?
        {
            return Ok(Some((k, val)));
        }
        Ok(None)
    }

    pub(crate) fn pop_list_tail_with_ns(
        &self,
        ns: ListNamespaces,
        list_key: &[u8],
    ) -> Result<Option<KVPair>> {
        let _lock = self.logical_write_guard(ns.meta, list_key);
        let tail = self.peek_list_tail_with_ns(ns, list_key)?;
        if let Some((k, _)) = tail
            && let Some(val) = self._list_remove_with_ns(ns, list_key, &k)?
        {
            return Ok(Some((k, val)));
        }
        Ok(None)
    }

    pub(crate) fn list_iter_with_ns<'a>(
        &'a self,
        ns: ListNamespaces,
        list: &[u8],
    ) -> ListIterator<'a> {
        // no need to lock for iteration
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

    pub(crate) fn list_len_with_ns(&self, ns: ListNamespaces, list: &[u8]) -> Result<usize> {
        Ok(get_list_meta(self, ns, list)?.count as usize)
    }

    pub(crate) fn list_range_with_ns(
        &self,
        ns: ListNamespaces,
        list: &[u8],
    ) -> Result<Range<usize>> {
        let meta = get_list_meta(self, ns, list)?;
        if meta.count == 0 {
            return Ok(0..0);
        }
        Ok(meta.head as usize..meta.tail as usize + 1)
    }
}

/// An iterator over the items in a list.
pub struct ListIterator<'a> {
    store: &'a CandyStore,
    list: Vec<u8>,
    ns: ListNamespaces,
    next_idx: u64,
    end_idx: u64,
    initial_next_idx: u64,
    initial_end_idx: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ListMetadata {
    pub head: u64,
    pub tail: u64,
    pub count: u64,
}

impl ListMetadata {
    pub fn new() -> Self {
        let head = 1u64 << 63;
        Self {
            head,
            tail: head - 1,
            count: 0,
        }
    }

    pub fn to_bytes(self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.head.to_le_bytes());
        buf[8..16].copy_from_slice(&self.tail.to_le_bytes());
        buf[16..24].copy_from_slice(&self.count.to_le_bytes());
        buf
    }

    pub fn from_bytes<B: AsRef<[u8]> + ?Sized>(bytes: &B) -> Option<Self> {
        let bytes = bytes.as_ref();
        if bytes.len() != 24 {
            return None;
        }
        let head = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let tail = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let count = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        Some(Self { head, tail, count })
    }
}

impl<'a> ListIterator<'a> {
    fn heal_head(&self, new_head: u64) {
        let _ = self.try_heal_head(new_head);
    }

    fn try_heal_head(&self, new_head: u64) -> Result<()> {
        let _lock = self.store.logical_write_guard(self.ns.meta, &self.list);
        let mut meta = get_list_meta(self.store, self.ns, &self.list)?;

        // We scanned [initial_next_idx, new_head). They are holes.
        // If meta.head is within [initial_next_idx, new_head), we can advance it to new_head.
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

        // We scanned (new_tail, initial_end_idx]. They are holes.
        // If meta.tail is within (new_tail, initial_end_idx], we can decrease it to new_tail.
        if meta.tail <= self.initial_end_idx && meta.tail > new_tail {
            meta.tail = new_tail;
            set_list_meta(self.store, self.ns, &self.list, meta)?;
        }
        Ok(())
    }
}

impl<'a> Iterator for ListIterator<'a> {
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
            let key = match self.store._get(self.ns.index, &idx_key) {
                Ok(Some(k)) => k,
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            };

            let data_key = make_list_data_key(&self.list, &key);
            let val_with_idx = match self.store._get(self.ns.data, &data_key) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            };

            return Some(Ok((key, strip_idx_suffix(val_with_idx))));
        }
        None
    }
}

impl<'a> DoubleEndedIterator for ListIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.next_idx <= self.end_idx {
            let idx = self.end_idx;
            if self.end_idx == 0 {
                self.next_idx = 1; // ensure termination even if idx is 0
            } else {
                self.end_idx -= 1;
            }

            if idx + 1000 < self.initial_end_idx {
                self.heal_tail(idx);
                self.initial_end_idx = idx;
            }

            let idx_key = make_list_index_key(&self.list, idx);
            let key = match self.store._get(self.ns.index, &idx_key) {
                Ok(Some(k)) => k,
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            };

            let data_key = make_list_data_key(&self.list, &key);
            let val_with_idx = match self.store._get(self.ns.data, &data_key) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            };

            return Some(Ok((key, strip_idx_suffix(val_with_idx))));
        }
        None
    }
}

fn get_list_meta(store: &CandyStore, ns: ListNamespaces, list: &[u8]) -> Result<ListMetadata> {
    if let Some(val) = store._get(ns.meta, list)?
        && let Some(meta) = ListMetadata::from_bytes(&val)
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
    store._set(ns.meta, list, &meta.to_bytes())?;
    Ok(())
}

fn hash_list_key(list: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(0x7ac1485be800c70e, 0x22ac1dcc7992c592);
    hasher.write(list);
    hasher.finish()
}

fn make_list_data_key(list: &[u8], key: &[u8]) -> Vec<u8> {
    let hash = hash_list_key(list);
    let mut k = Vec::with_capacity(8 + key.len());
    k.extend_from_slice(&hash.to_le_bytes());
    k.extend_from_slice(key);
    k
}

fn make_list_index_key(list: &[u8], idx: u64) -> Vec<u8> {
    let hash = hash_list_key(list);
    let mut k = Vec::with_capacity(16);
    k.extend_from_slice(&hash.to_le_bytes());
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

fn append_idx_suffix(value: &[u8], idx: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 8);
    out.extend_from_slice(value);
    out.extend_from_slice(&idx.to_le_bytes());
    out
}

fn strip_idx_suffix(mut value: Vec<u8>) -> Vec<u8> {
    if value.len() >= 8 {
        value.truncate(value.len() - 8);
    }
    value
}

fn extract_idx_suffix(value: &[u8]) -> u64 {
    let n = value.len();
    if n < 8 {
        return 0;
    }
    u64::from_le_bytes(value[n - 8..n].try_into().unwrap())
}
