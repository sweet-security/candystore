use siphasher::sip::SipHasher13;
use std::hash::Hasher;
use std::ops::Range;

use crate::MAX_VALUE_LEN;
use crate::store::CandyStore;
use crate::types::{QUEUE_NS, QueueNamespaces, Result};

// Public queue API
impl CandyStore {
    /// Pushes an item to the head of a queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    /// * `val` - The value to push.
    ///
    /// # Returns
    ///
    /// The index of the newly pushed item.
    pub fn push_to_queue_head<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B1,
        val: &B2,
    ) -> Result<usize> {
        self.ensure_user_value_len(val.as_ref().len())?;
        self.queue_push_head_with_ns(QUEUE_NS, queue_key.as_ref(), val.as_ref())
            .map(|idx| idx as usize)
    }

    /// Pushes an item to the tail of a queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    /// * `val` - The value to push.
    ///
    /// # Returns
    ///
    /// The index of the newly pushed item.
    pub fn push_to_queue_tail<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B1,
        val: &B2,
    ) -> Result<usize> {
        self.ensure_user_value_len(val.as_ref().len())?;
        self.queue_push_tail_with_ns(QUEUE_NS, queue_key.as_ref(), val.as_ref())
            .map(|idx| idx as usize)
    }

    /// Removes and returns the item at the head of the queue, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn pop_queue_head_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_pop_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, v)| (idx as usize, v)))
    }

    /// Removes and returns the item at the head of the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn pop_queue_head<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_pop_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, v)| v))
    }

    /// Removes and returns the item at the tail of the queue, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn pop_queue_tail_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_pop_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, v)| (idx as usize, v)))
    }

    /// Removes and returns the item at the tail of the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn pop_queue_tail<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_pop_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, v)| v))
    }

    /// Removes a specific item from the queue by its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    /// * `idx` - The index of the item to remove.
    ///
    /// # Returns
    ///
    /// The value of the removed item, or `None` if the item does not exist.
    pub fn remove_from_queue<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
        idx: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.queue_remove_with_ns(QUEUE_NS, queue_key.as_ref(), idx as u64)
    }

    /// Discards a queue, removing all its items.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// `true` if the queue existed and was removed, `false` otherwise.
    pub fn discard_queue<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<bool> {
        self.queue_discard_with_ns(QUEUE_NS, queue_key.as_ref())
    }

    /// Extends a queue by pushing multiple items to the tail.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    /// * `items` - An iterator of items to push.
    ///
    /// # Returns
    ///
    /// The range of indices assigned to the new items.
    pub fn extend_queue<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
        items: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Range<usize>> {
        self.queue_extend_with_ns(QUEUE_NS, queue_key.as_ref(), items)
    }

    /// Returns the item at the head of the queue without removing it, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn peek_queue_head_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_peek_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, v)| (idx as usize, v)))
    }

    /// Returns the item at the head of the queue without removing it.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn peek_queue_head<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_peek_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, v)| v))
    }

    /// Returns the item at the tail of the queue without removing it, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn peek_queue_tail_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_peek_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, v)| (idx as usize, v)))
    }

    /// Returns the item at the tail of the queue without removing it.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn peek_queue_tail<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_peek_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, v)| v))
    }

    /// Iterates over a queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// An iterator over the items in the queue.
    pub fn iter_queue<'a, B: AsRef<[u8]> + ?Sized>(&'a self, queue_key: &B) -> QueueIterator<'a> {
        self.queue_iter_with_ns(QUEUE_NS, queue_key.as_ref())
    }

    /// Returns the number of items in a queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The number of items in the queue.
    pub fn queue_len<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<usize> {
        Ok(self.queue_len_with_ns(QUEUE_NS, queue_key.as_ref())? as usize)
    }

    /// Returns the range of indices currently in use by the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The range of indices currently in use by the queue.
    pub fn queue_range<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<Range<usize>> {
        self.queue_range_with_ns(QUEUE_NS, queue_key.as_ref())
    }
}

pub struct QueueIterator<'a> {
    store: &'a CandyStore,
    queue: Vec<u8>,
    ns: QueueNamespaces,
    next_idx: u64,
    end_idx: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueMetadata {
    pub head: u64,
    pub tail: u64,
    pub count: u64,
}

impl QueueMetadata {
    pub fn new() -> Self {
        // Start in the middle to allow expansion in both directions
        Self {
            head: 1u64 << 63,
            tail: (1u64 << 63) - 1,
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

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 24 {
            return None;
        }
        let head = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let tail = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let count = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        Some(Self { head, tail, count })
    }
}

impl<'a> Iterator for QueueIterator<'a> {
    type Item = Result<(usize, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_idx <= self.end_idx {
            let idx = self.next_idx;
            self.next_idx += 1;

            let key = make_queue_data_key(&self.queue, idx);
            match self.store._get(self.ns.data, &key) {
                Ok(Some(v)) => return Some(Ok((idx as usize, v))),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for QueueIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.next_idx <= self.end_idx {
            let idx = self.end_idx;
            if self.end_idx == 0 {
                self.next_idx = 1; // ensure termination if idx is 0
            } else {
                self.end_idx -= 1;
            }

            let key = make_queue_data_key(&self.queue, idx);
            match self.store._get(self.ns.data, &key) {
                Ok(Some(v)) => return Some(Ok((idx as usize, v))),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

impl CandyStore {
    pub(crate) fn queue_push_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        value: &[u8],
    ) -> Result<u64> {
        let _lock = self.logical_write_guard(ns.meta, queue);
        self._queue_push_tail_with_ns(ns, queue, value)
    }

    fn _queue_push_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        value: &[u8],
    ) -> Result<u64> {
        let mut meta = get_queue_meta(self, ns, queue)?;
        let new_tail = meta.tail + 1;
        let key = make_queue_data_key(queue, new_tail);
        self._set(ns.data, &key, value)?;
        meta.tail = new_tail;
        meta.count += 1;
        if meta.head > meta.tail {
            meta.head = new_tail;
        }
        set_queue_meta(self, ns, queue, meta)?;
        Ok(new_tail)
    }

    pub(crate) fn queue_push_head_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        value: &[u8],
    ) -> Result<u64> {
        let _lock = self.logical_write_guard(ns.meta, queue);

        let mut meta = get_queue_meta(self, ns, queue)?;
        let new_head = meta.head - 1;
        let key = make_queue_data_key(queue, new_head);
        self._set(ns.data, &key, value)?;
        meta.head = new_head;
        meta.count += 1;
        set_queue_meta(self, ns, queue, meta)?;
        Ok(new_head)
    }

    pub(crate) fn queue_pop_head_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.logical_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;
        loop {
            if meta.head > meta.tail {
                return Ok(None);
            }

            let key = make_queue_data_key(queue, meta.head);
            let val = self._remove(ns.data, &key)?;

            // Advance head
            meta.head += 1;

            if let Some(v) = val {
                meta.count -= 1;
                set_queue_meta(self, ns, queue, meta)?;
                return Ok(Some((meta.head - 1, v)));
            }

            // Persist the advanced head so we don't get stuck on holes after a crash
            set_queue_meta(self, ns, queue, meta)?;
        }
    }

    pub(crate) fn queue_pop_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.logical_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;
        loop {
            if meta.head > meta.tail {
                return Ok(None);
            }

            let key = make_queue_data_key(queue, meta.tail);
            let val = self._remove(ns.data, &key)?;

            // Retreat tail
            meta.tail -= 1;

            if let Some(v) = val {
                meta.count -= 1;
                set_queue_meta(self, ns, queue, meta)?;
                return Ok(Some((meta.tail + 1, v)));
            }

            // Persist the moved tail so we don't get stuck on holes after a crash
            set_queue_meta(self, ns, queue, meta)?;
        }
    }

    pub(crate) fn queue_peek_head_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.logical_read_guard(ns.meta, queue);

        let meta = get_queue_meta(self, ns, queue)?;
        if meta.head > meta.tail {
            return Ok(None);
        }
        let key = make_queue_data_key(queue, meta.head);
        Ok(self._get(ns.data, &key)?.map(|v| (meta.head, v)))
    }

    pub(crate) fn queue_peek_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.logical_read_guard(ns.meta, queue);

        let meta = get_queue_meta(self, ns, queue)?;
        if meta.head > meta.tail {
            return Ok(None);
        }
        let key = make_queue_data_key(queue, meta.tail);
        Ok(self._get(ns.data, &key)?.map(|v| (meta.tail, v)))
    }

    pub(crate) fn queue_len_with_ns(&self, ns: QueueNamespaces, queue: &[u8]) -> Result<u64> {
        let meta = get_queue_meta(self, ns, queue)?;
        Ok(meta.count)
    }

    pub(crate) fn queue_discard_with_ns(&self, ns: QueueNamespaces, queue: &[u8]) -> Result<bool> {
        let _lock = self.logical_write_guard(ns.meta, queue);
        self._queue_discard_with_ns(ns, queue)
    }

    fn _queue_discard_with_ns(&self, ns: QueueNamespaces, queue: &[u8]) -> Result<bool> {
        let mut meta = get_queue_meta(self, ns, queue)?;
        let had_items = meta.head <= meta.tail;
        while meta.head <= meta.tail {
            let key = make_queue_data_key(queue, meta.head);
            _ = self._remove(ns.data, &key)?;
            meta.head += 1;
        }

        self._remove(ns.meta, queue)?;
        Ok(had_items)
    }

    pub(crate) fn queue_iter_with_ns<'a>(
        &'a self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> QueueIterator<'a> {
        // no locking needed
        let meta = get_queue_meta(self, ns, queue).unwrap_or_else(|_| QueueMetadata::new());
        QueueIterator {
            store: self,
            queue: queue.to_vec(),
            ns,
            next_idx: meta.head,
            end_idx: meta.tail,
        }
    }

    pub(crate) fn queue_remove_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;

        let key = make_queue_data_key(queue, idx);
        let removed = match self._remove(ns.data, &key)? {
            Some(v) => v,
            None => return Ok(None),
        };

        meta.count -= 1;

        if idx == meta.head {
            while meta.head <= meta.tail {
                let k = make_queue_data_key(queue, meta.head);
                if self._get(ns.data, &k)?.is_some() {
                    break;
                }
                meta.head += 1;
            }
        }

        if idx == meta.tail {
            while meta.tail >= meta.head {
                let k = make_queue_data_key(queue, meta.tail);
                if self._get(ns.data, &k)?.is_some() {
                    break;
                }
                if meta.tail == 0 {
                    break;
                }
                meta.tail -= 1;
            }
        }

        if meta.head > meta.tail {
            meta = QueueMetadata::new();
        }

        set_queue_meta(self, ns, queue, meta)?;
        Ok(Some(removed))
    }

    pub(crate) fn queue_extend_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        items: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Range<usize>> {
        let _lock = self.logical_write_guard(ns.meta, queue);
        self._queue_extend_with_ns(ns, queue, items)
    }

    fn _queue_extend_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        items: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Range<usize>> {
        let mut meta = get_queue_meta(self, ns, queue)?;

        let start = meta.tail + 1;
        let mut end = start;
        let mut wrote = false;

        for item in items {
            self.ensure_user_value_len(item.as_ref().len())?;

            let idx = end;
            let key = make_queue_data_key(queue, idx);
            self._set(ns.data, &key, item.as_ref())?;
            end += 1;
            wrote = true;
            meta.count += 1;
        }

        if wrote {
            if meta.head > meta.tail {
                meta.head = start;
            }
            meta.tail = end - 1;
            set_queue_meta(self, ns, queue, meta)?;
            Ok(start as usize..end as usize)
        } else {
            Ok(start as usize..start as usize)
        }
    }

    pub(crate) fn queue_range_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Range<usize>> {
        let meta = get_queue_meta(self, ns, queue)?;
        if meta.head > meta.tail {
            return Ok(0..0);
        }
        Ok(meta.head as usize..meta.tail.saturating_add(1) as usize)
    }

    pub(crate) fn queue_set_big_with_ns(
        &self,
        ns: QueueNamespaces,
        key: &[u8],
        val: &[u8],
    ) -> Result<bool> {
        let _lock = self.logical_write_guard(ns.meta, key);
        let existed = self._queue_discard_with_ns(ns, key)?;

        self._queue_extend_with_ns(ns, key, val.chunks(MAX_VALUE_LEN))?;

        let len_bytes = val.len().to_le_bytes();
        let _ = self._queue_push_tail_with_ns(ns, key, &len_bytes)?;

        Ok(existed)
    }

    pub(crate) fn queue_get_big_with_ns(
        &self,
        ns: QueueNamespaces,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.logical_write_guard(ns.meta, key);
        let expected_chunks = self.queue_len_with_ns(ns, key)?;
        if expected_chunks == 0 {
            return Ok(None);
        }

        let mut collected = Vec::new();
        let mut seen = 0u64;
        // does not hold any locks
        for res in self.queue_iter_with_ns(ns, key) {
            let (_, chunk) = res?;
            seen += 1;
            if seen == expected_chunks
                && chunk.len() == size_of::<usize>()
                && let Ok(len_arr) = chunk.as_slice().try_into()
            {
                let recorded_len = usize::from_le_bytes(len_arr);
                if recorded_len == collected.len() {
                    return Ok(Some(collected));
                }
            } else {
                collected.extend_from_slice(&chunk);
            }
            if seen == expected_chunks {
                return Ok(None);
            }
        }

        Ok(None)
    }
}

fn get_queue_meta(store: &CandyStore, ns: QueueNamespaces, queue: &[u8]) -> Result<QueueMetadata> {
    if let Some(val) = store._get(ns.meta, queue)?
        && let Some(meta) = QueueMetadata::from_bytes(&val)
    {
        return Ok(meta);
    }
    Ok(QueueMetadata::new())
}

fn set_queue_meta(
    store: &CandyStore,
    ns: QueueNamespaces,
    queue: &[u8],
    meta: QueueMetadata,
) -> Result<()> {
    store._set(ns.meta, queue, &meta.to_bytes())?;
    Ok(())
}

fn make_queue_data_key(queue: &[u8], seq: u64) -> Vec<u8> {
    let mut hasher = SipHasher13::new_with_keys(0xb1ccc559a9924eaa, 0x1b1a682059c2d599);
    hasher.write(queue);
    let hash = hasher.finish();

    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&hash.to_le_bytes());
    key.extend_from_slice(&seq.to_be_bytes());
    key
}
