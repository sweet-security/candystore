use siphasher::sip::SipHasher13;

use std::{hash::Hasher, mem::size_of, ops::Range};

use crate::{
    internal::{KeyNamespace, RangeMetadata, aligned_data_entry_size, invalid_data_error},
    store::CandyStore,
    types::{Error, MAX_USER_KEY_SIZE, MAX_USER_VALUE_SIZE, Result},
};

#[derive(Clone, Copy)]
pub(super) struct QueueNamespaces {
    pub(super) meta: KeyNamespace,
    pub(super) data: KeyNamespace,
}

const QUEUE_NS: QueueNamespaces = QueueNamespaces {
    meta: KeyNamespace::QueueMeta,
    data: KeyNamespace::QueueData,
};

const BIG_NS: QueueNamespaces = QueueNamespaces {
    meta: KeyNamespace::BigMeta,
    data: KeyNamespace::BigData,
};

/// Double-ended iterator over live queue items and their logical indices.
pub struct QueueIterator<'a> {
    store: &'a CandyStore,
    queue: Vec<u8>,
    ns: QueueNamespaces,
    initial_error: Option<Error>,
    next_idx: u64,
    end_idx: u64,
    initial_next_idx: u64,
    initial_end_idx: u64,
}

type QueueMetadata = RangeMetadata;

impl<'a> QueueIterator<'a> {
    fn try_heal_head(&self, new_head: u64) -> Result<()> {
        self.store.try_heal_range_head(
            self.ns.meta,
            &self.queue,
            self.initial_next_idx,
            new_head,
            |store, queue| get_queue_meta(store, self.ns, queue),
            |store, queue, meta| set_queue_meta(store, self.ns, queue, meta),
        )
    }

    fn try_heal_tail(&self, new_tail: u64) -> Result<()> {
        self.store.try_heal_range_tail(
            self.ns.meta,
            &self.queue,
            self.initial_end_idx,
            new_tail,
            |store, queue| get_queue_meta(store, self.ns, queue),
            |store, queue, meta| set_queue_meta(store, self.ns, queue, meta),
        )
    }
}

impl Iterator for QueueIterator<'_> {
    type Item = Result<(usize, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.initial_error.take() {
            return Some(Err(err));
        }
        while self.next_idx <= self.end_idx {
            let idx = self.next_idx;
            self.next_idx += 1;

            if idx > self.initial_next_idx + 1000 {
                let _ = self.try_heal_head(idx);
                self.initial_next_idx = idx;
            }

            let key = make_queue_data_key(&self.queue, idx);
            match self.store.get_ns(self.ns.data, &key) {
                Ok(Some(v)) => return Some(Ok((idx as usize, v))),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

impl DoubleEndedIterator for QueueIterator<'_> {
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some(err) = self.initial_error.take() {
            return Some(Err(err));
        }
        while self.next_idx <= self.end_idx {
            let idx = self.end_idx;
            if self.end_idx == 0 {
                self.next_idx = 1;
            } else {
                self.end_idx -= 1;
            }

            if idx + 1000 < self.initial_end_idx {
                let _ = self.try_heal_tail(idx);
                self.initial_end_idx = idx;
            }

            let key = make_queue_data_key(&self.queue, idx);
            match self.store.get_ns(self.ns.data, &key) {
                Ok(Some(v)) => return Some(Ok((idx as usize, v))),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

impl CandyStore {
    /// Pushes `val` to the head of `queue_key` and returns its logical index.
    pub fn push_to_queue_head<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B1,
        val: &B2,
    ) -> Result<usize> {
        self.queue_push_head_with_ns(QUEUE_NS, queue_key.as_ref(), val.as_ref())
            .map(|idx| idx as usize)
    }

    /// Pushes `val` to the tail of `queue_key` and returns its logical index.
    pub fn push_to_queue_tail<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B1,
        val: &B2,
    ) -> Result<usize> {
        self.queue_push_tail_with_ns(QUEUE_NS, queue_key.as_ref(), val.as_ref())
            .map(|idx| idx as usize)
    }

    /// Removes and returns the head value of `queue_key`.
    pub fn pop_queue_head<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_pop_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, value)| value))
    }

    /// Removes and returns the head item of `queue_key` together with its logical index.
    pub fn pop_queue_head_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_pop_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, value)| (idx as usize, value)))
    }

    /// Removes and returns the tail value of `queue_key`.
    pub fn pop_queue_tail<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_pop_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, value)| value))
    }

    /// Removes and returns the tail item of `queue_key` together with its logical index.
    pub fn pop_queue_tail_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_pop_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, value)| (idx as usize, value)))
    }

    /// Returns the head value of `queue_key` without removing it.
    pub fn peek_queue_head<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_peek_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, value)| value))
    }

    /// Returns the head item of `queue_key` and its logical index without removing it.
    pub fn peek_queue_head_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_peek_head_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, value)| (idx as usize, value)))
    }

    /// Returns the tail value of `queue_key` without removing it.
    pub fn peek_queue_tail<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            .queue_peek_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(_, value)| value))
    }

    /// Returns the tail item of `queue_key` and its logical index without removing it.
    pub fn peek_queue_tail_with_idx<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        Ok(self
            .queue_peek_tail_with_ns(QUEUE_NS, queue_key.as_ref())?
            .map(|(idx, value)| (idx as usize, value)))
    }

    /// Removes and returns the item at logical index `idx`, if it exists.
    pub fn remove_from_queue<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
        idx: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.queue_remove_with_ns(QUEUE_NS, queue_key.as_ref(), idx as u64)
    }

    /// Removes all items from `queue_key`.
    pub fn discard_queue<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<bool> {
        self.queue_discard_with_ns(QUEUE_NS, queue_key.as_ref())
    }

    /// Appends all provided values to the tail of `queue_key`.
    pub fn extend_queue<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
        items: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Range<usize>> {
        let mut start = None;
        let mut end = None;

        for item in items {
            let idx = self.push_to_queue_tail(queue_key, &item)?;
            if start.is_none() {
                start = Some(idx);
            }
            end = Some(idx + 1);
        }

        Ok(match (start, end) {
            (Some(start), Some(end)) => start..end,
            _ => {
                let range = self.queue_range(queue_key)?;
                range.start..range.start
            }
        })
    }

    /// Returns the number of live items in `queue_key`.
    pub fn queue_len<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<usize> {
        Ok(self.queue_len_with_ns(QUEUE_NS, queue_key.as_ref())? as usize)
    }

    /// Returns the current inclusive-exclusive logical index span for `queue_key`.
    pub fn queue_range<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<Range<usize>> {
        self.queue_range_with_ns(QUEUE_NS, queue_key.as_ref())
    }

    /// Iterates over live items in `queue_key` from head to tail.
    pub fn iter_queue<'a, B: AsRef<[u8]> + ?Sized>(&'a self, queue_key: &B) -> QueueIterator<'a> {
        self.queue_iter_with_ns(QUEUE_NS, queue_key.as_ref())
    }

    /// Stores a large value under `key`, chunking it across queue-backed data entries if needed.
    pub fn set_big<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        value: &B2,
    ) -> Result<bool> {
        self.queue_set_big_with_ns(BIG_NS, key.as_ref(), value.as_ref())
    }

    /// Loads a value previously stored with [`CandyStore::set_big`].
    pub fn get_big<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        self.queue_get_big_with_ns(BIG_NS, key.as_ref())
    }

    /// Removes a value previously stored with [`CandyStore::set_big`].
    pub fn remove_big<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<bool> {
        self.queue_discard_with_ns(BIG_NS, key.as_ref())
    }

    pub(super) fn queue_push_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        value: &[u8],
    ) -> Result<u64> {
        self.validate_queue_item_sizes(queue, value)?;
        let _lock = self.list_write_guard(ns.meta, queue);
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
        self.set_ns(ns.data, &key, value)?;
        meta.tail = new_tail;
        meta.count += 1;
        if meta.head > meta.tail {
            meta.head = new_tail;
        }
        set_queue_meta(self, ns, queue, meta)?;
        Ok(new_tail)
    }

    pub(super) fn queue_push_head_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        value: &[u8],
    ) -> Result<u64> {
        self.validate_queue_item_sizes(queue, value)?;
        let _lock = self.list_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;
        let new_head = meta.head - 1;
        let key = make_queue_data_key(queue, new_head);
        self.set_ns(ns.data, &key, value)?;
        meta.head = new_head;
        meta.count += 1;
        if meta.tail < meta.head {
            meta.tail = new_head;
        }
        set_queue_meta(self, ns, queue, meta)?;
        Ok(new_head)
    }

    pub(super) fn queue_pop_head_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.list_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;
        loop {
            if meta.head > meta.tail {
                return Ok(None);
            }

            let idx = meta.head;
            let key = make_queue_data_key(queue, idx);
            let value = self.remove_ns(ns.data, &key)?;
            meta.head += 1;

            if let Some(value) = value {
                meta.count = meta.count.saturating_sub(1);
                if meta.head > meta.tail {
                    meta = QueueMetadata::new();
                }
                set_queue_meta(self, ns, queue, meta)?;
                return Ok(Some((idx, value)));
            }

            set_queue_meta(self, ns, queue, meta)?;
        }
    }

    pub(super) fn queue_pop_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.list_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;
        loop {
            if meta.head > meta.tail {
                return Ok(None);
            }

            let idx = meta.tail;
            let key = make_queue_data_key(queue, idx);
            let value = self.remove_ns(ns.data, &key)?;
            meta.tail = meta.tail.saturating_sub(1);

            if let Some(value) = value {
                meta.count = meta.count.saturating_sub(1);
                if meta.head > meta.tail {
                    meta = QueueMetadata::new();
                }
                set_queue_meta(self, ns, queue, meta)?;
                return Ok(Some((idx, value)));
            }

            set_queue_meta(self, ns, queue, meta)?;
        }
    }

    pub(super) fn queue_peek_head_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.list_read_guard(ns.meta, queue);
        let meta = get_queue_meta(self, ns, queue)?;
        if meta.head > meta.tail {
            return Ok(None);
        }
        for idx in meta.head..=meta.tail {
            let key = make_queue_data_key(queue, idx);
            if let Some(value) = self.get_ns(ns.data, &key)? {
                return Ok(Some((idx, value)));
            }
        }
        Ok(None)
    }

    pub(super) fn queue_peek_tail_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Option<(u64, Vec<u8>)>> {
        let _lock = self.list_read_guard(ns.meta, queue);
        let meta = get_queue_meta(self, ns, queue)?;
        if meta.head > meta.tail {
            return Ok(None);
        }
        for idx in (meta.head..=meta.tail).rev() {
            let key = make_queue_data_key(queue, idx);
            if let Some(value) = self.get_ns(ns.data, &key)? {
                return Ok(Some((idx, value)));
            }
        }
        Ok(None)
    }

    pub(super) fn queue_len_with_ns(&self, ns: QueueNamespaces, queue: &[u8]) -> Result<u64> {
        Ok(get_queue_meta(self, ns, queue)?.count)
    }

    pub(super) fn queue_discard_with_ns(&self, ns: QueueNamespaces, queue: &[u8]) -> Result<bool> {
        let _lock = self.list_write_guard(ns.meta, queue);
        self._queue_discard_with_ns(ns, queue)
    }

    fn _queue_discard_with_ns(&self, ns: QueueNamespaces, queue: &[u8]) -> Result<bool> {
        let mut meta = get_queue_meta(self, ns, queue)?;
        let had_items = meta.head <= meta.tail;
        while meta.head <= meta.tail {
            let key = make_queue_data_key(queue, meta.head);
            _ = self.remove_ns(ns.data, &key)?;
            meta.head += 1;
        }

        self.remove_ns(ns.meta, queue)?;
        Ok(had_items)
    }

    fn queue_remove_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
        idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.list_write_guard(ns.meta, queue);
        let mut meta = get_queue_meta(self, ns, queue)?;
        let key = make_queue_data_key(queue, idx);
        let removed = match self.remove_ns(ns.data, &key)? {
            Some(value) => value,
            None => return Ok(None),
        };

        meta.count = meta.count.saturating_sub(1);

        if idx == meta.head {
            meta.head += 1;
        }

        if meta.tail == idx {
            meta.tail = meta.tail.saturating_sub(1);
        }

        if meta.head > meta.tail {
            meta = QueueMetadata::new();
        }

        set_queue_meta(self, ns, queue, meta)?;
        Ok(Some(removed))
    }

    pub(super) fn queue_iter_with_ns<'a>(
        &'a self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> QueueIterator<'a> {
        let (meta, initial_error) = match get_queue_meta(self, ns, queue) {
            Ok(meta) => (meta, None),
            Err(err) => (QueueMetadata::new(), Some(err)),
        };
        QueueIterator {
            store: self,
            queue: queue.to_vec(),
            ns,
            initial_error,
            next_idx: meta.head,
            end_idx: meta.tail,
            initial_next_idx: meta.head,
            initial_end_idx: meta.tail,
        }
    }

    pub(super) fn queue_range_with_ns(
        &self,
        ns: QueueNamespaces,
        queue: &[u8],
    ) -> Result<Range<usize>> {
        let meta = get_queue_meta(self, ns, queue)?;
        if meta.count == 0 || meta.head > meta.tail {
            return Ok(0..0);
        }
        Ok(meta.head as usize..meta.tail.saturating_add(1) as usize)
    }

    pub(super) fn queue_set_big_with_ns(
        &self,
        ns: QueueNamespaces,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        self.validate_queue_metadata_key(key)?;
        let max_chunk_len = self.max_big_chunk_len(key)?;
        let _lock = self.list_write_guard(ns.meta, key);
        let existed = self._queue_discard_with_ns(ns, key)?;

        for chunk in value.chunks(max_chunk_len) {
            self._queue_push_tail_with_ns(ns, key, chunk)?;
        }

        self._queue_push_tail_with_ns(ns, key, &value.len().to_le_bytes())?;
        Ok(existed)
    }

    pub(super) fn queue_get_big_with_ns(
        &self,
        ns: QueueNamespaces,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let _lock = self.list_read_guard(ns.meta, key);
        let meta = get_queue_meta(self, ns, key)?;
        let expected_chunks = meta.count;
        if expected_chunks == 0 {
            return Ok(None);
        }

        let mut collected = Vec::new();
        let mut seen = 0u64;
        for idx in meta.head..=meta.tail {
            let item_key = make_queue_data_key(key, idx);
            let Some(chunk) = self.get_ns(ns.data, &item_key)? else {
                continue;
            };

            seen += 1;
            if seen == expected_chunks && chunk.len() == size_of::<usize>() {
                let recorded_len = usize::from_le_bytes(chunk.as_slice().try_into().unwrap());
                if recorded_len == collected.len() {
                    return Ok(Some(collected));
                }
                return Ok(None);
            }

            collected.extend_from_slice(&chunk);
            if seen == expected_chunks {
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn max_big_chunk_len(&self, key: &[u8]) -> Result<usize> {
        let data_key_len = make_queue_data_key(key, 0).len();
        if aligned_data_entry_size(data_key_len, size_of::<usize>()) as usize
            > self.inner.config.max_data_file_size as usize
        {
            return Err(Error::PayloadTooLarge(aligned_data_entry_size(
                data_key_len,
                size_of::<usize>(),
            ) as usize));
        }

        let mut max_chunk_len = MAX_USER_VALUE_SIZE;
        while max_chunk_len > 0
            && aligned_data_entry_size(data_key_len, max_chunk_len) as usize
                > self.inner.config.max_data_file_size as usize
        {
            max_chunk_len -= 1;
        }

        if max_chunk_len == 0 {
            return Err(Error::PayloadTooLarge(
                aligned_data_entry_size(data_key_len, 1) as usize,
            ));
        }

        Ok(max_chunk_len)
    }

    fn validate_queue_metadata_key(&self, queue: &[u8]) -> Result<()> {
        validate_internal_entry(self, queue.len(), size_of::<u64>() * 3)
    }

    fn validate_queue_item_sizes(&self, queue: &[u8], value: &[u8]) -> Result<()> {
        self.validate_queue_metadata_key(queue)?;
        validate_internal_entry(self, make_queue_data_key(queue, 0).len(), value.len())
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

fn get_queue_meta(store: &CandyStore, ns: QueueNamespaces, queue: &[u8]) -> Result<QueueMetadata> {
    if let Some(value) = store.get_ns(ns.meta, queue)? {
        return QueueMetadata::from_bytes(&value)
            .ok_or_else(|| invalid_data_error("invalid queue metadata"));
    }
    Ok(QueueMetadata::new())
}

fn set_queue_meta(
    store: &CandyStore,
    ns: QueueNamespaces,
    queue: &[u8],
    meta: QueueMetadata,
) -> Result<()> {
    store.set_ns(ns.meta, queue, &meta.to_bytes())?;
    Ok(())
}

fn hash_queue_key(queue: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(0xb1ccc559a9924eaa, 0x1b1a682059c2d599);
    hasher.write(queue);
    hasher.finish()
}

fn make_queue_data_key(queue: &[u8], seq: u64) -> [u8; 16] {
    let hash = hash_queue_key(queue);

    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&hash.to_le_bytes());
    key[8..].copy_from_slice(&seq.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::Config;

    #[test]
    fn queue_iterator_surfaces_invalid_metadata() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(dir.path(), Config::default())?;
        db.set_ns(KeyNamespace::QueueMeta, b"broken-queue", b"bad")?;

        let err = db
            .iter_queue(b"broken-queue")
            .next()
            .expect("invalid metadata should produce an iterator item")
            .expect_err("invalid metadata should be reported");
        assert!(matches!(
            err,
            Error::IOError(ref io_err) if io_err.kind() == std::io::ErrorKind::InvalidData
        ));
        Ok(())
    }
}
