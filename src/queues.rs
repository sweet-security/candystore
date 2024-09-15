use std::ops::Range;

use crate::{
    hashing::PartedHash,
    store::{QUEUE_ITEM_NAMESPACE, QUEUE_NAMESPACE},
    CandyStore,
};
use anyhow::Result;
use bytemuck::{bytes_of, checked::from_bytes_mut, from_bytes, Pod, Zeroable};

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct Queue {
    head_idx: u64, // inclusive
    tail_idx: u64, // exclusive
    num_items: u64,
}

impl Queue {
    #[allow(dead_code)]
    fn span_len(&self) -> u64 {
        self.tail_idx - self.head_idx
    }
    #[allow(dead_code)]
    fn holes(&self) -> u64 {
        self.span_len() - self.num_items
    }
    fn is_empty(&self) -> bool {
        self.head_idx == self.tail_idx
    }
}

enum QueuePos {
    Head,
    Tail,
}

pub struct QueueIterator<'a> {
    store: &'a CandyStore,
    queue_key: Vec<u8>,
    range: Option<Range<u64>>,
    fwd: bool,
}

impl<'a> Iterator for QueueIterator<'a> {
    type Item = Result<(usize, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.range.is_none() {
            match self.store.fetch_queue(&self.queue_key) {
                Ok(queue) => match queue {
                    Some(queue) => {
                        self.range = Some(queue.head_idx..queue.tail_idx);
                    }
                    None => return None,
                },
                Err(e) => return Some(Err(e)),
            }
        }

        loop {
            let idx = if self.fwd {
                self.range.as_mut().unwrap().next()
            } else {
                self.range.as_mut().unwrap().next_back()
            };
            let Some(idx) = idx else {
                return None;
            };

            match self
                .store
                .get_raw(&self.store.make_queue_item_key(&self.queue_key, idx))
            {
                Ok(v) => {
                    match v {
                        Some(v) => return Some(Ok((idx as usize, v))),
                        None => {
                            // continue, we might have holes
                        }
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(ref range) = self.range {
            range.size_hint()
        } else {
            (0, None)
        }
    }
}

impl CandyStore {
    const FIRST_QUEUE_IDX: u64 = 0x8000_0000_0000_0000;

    fn make_queue_key(&self, queue_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_queue_key = queue_key.to_owned();
        full_queue_key.extend_from_slice(QUEUE_NAMESPACE);
        (
            PartedHash::new(&self.config.hash_seed, &queue_key),
            full_queue_key,
        )
    }
    fn make_queue_item_key(&self, queue_key: &[u8], idx: u64) -> Vec<u8> {
        let mut item_key = queue_key.to_owned();
        item_key.extend_from_slice(bytes_of(&idx));
        item_key.extend_from_slice(QUEUE_ITEM_NAMESPACE);
        item_key
    }

    fn _push_to_queue(&self, queue_key: &[u8], val: &[u8], pos: QueuePos) -> Result<usize> {
        let (queue_ph, full_queue_key) = self.make_queue_key(queue_key);
        let _guard = self.lock_list(queue_ph);

        let status = self.get_or_create_raw(
            &full_queue_key,
            bytes_of(&Queue {
                head_idx: Self::FIRST_QUEUE_IDX,
                tail_idx: Self::FIRST_QUEUE_IDX + 1,
                num_items: 1,
            })
            .to_owned(),
        )?;

        let item_idx = match status {
            crate::GetOrCreateStatus::CreatedNew(_) => Self::FIRST_QUEUE_IDX,
            crate::GetOrCreateStatus::ExistingValue(mut queue_bytes) => {
                let queue = from_bytes_mut::<Queue>(&mut queue_bytes);
                let item_idx = match pos {
                    QueuePos::Head => {
                        queue.head_idx -= 1;
                        queue.head_idx
                    }
                    QueuePos::Tail => {
                        let item_idx = queue.tail_idx;
                        queue.tail_idx += 1;
                        item_idx
                    }
                };
                queue.num_items += 1;
                self.set_raw(&full_queue_key, &queue_bytes)?;
                item_idx
            }
        };

        self.set_raw(&self.make_queue_item_key(queue_key, item_idx), val)?;
        Ok(item_idx as usize)
    }

    /// Pushed a new element at the front (head) of the queue, returning the element's index in the queue
    pub fn push_to_queue_head<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B1,
        val: &B2,
    ) -> Result<usize> {
        self._push_to_queue(queue_key.as_ref(), val.as_ref(), QueuePos::Head)
    }

    /// Pushed a new element at the end (tail) of the queue, returning the element's index in the queue
    pub fn push_to_queue_tail<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B1,
        val: &B2,
    ) -> Result<usize> {
        self._push_to_queue(queue_key.as_ref(), val.as_ref(), QueuePos::Tail)
    }

    fn _pop_queue(&self, queue_key: &[u8], pos: QueuePos) -> Result<Option<Vec<u8>>> {
        let (queue_ph, full_queue_key) = self.make_queue_key(queue_key);
        let _guard = self.lock_list(queue_ph);

        let Some(mut queue_bytes) = self.get_raw(&full_queue_key)? else {
            return Ok(None);
        };
        let queue = from_bytes_mut::<Queue>(&mut queue_bytes);
        let mut val = None;

        match pos {
            QueuePos::Head => {
                while queue.head_idx < queue.tail_idx {
                    let idx = queue.head_idx;
                    queue.head_idx += 1;
                    if let Some(v) = self.remove_raw(&self.make_queue_item_key(queue_key, idx))? {
                        val = Some(v);
                        queue.num_items -= 1;
                        break;
                    }
                }
            }
            QueuePos::Tail => {
                while queue.tail_idx > queue.head_idx {
                    queue.tail_idx -= 1;
                    let idx = queue.tail_idx;
                    if let Some(v) = self.remove_raw(&self.make_queue_item_key(queue_key, idx))? {
                        val = Some(v);
                        queue.num_items -= 1;
                        break;
                    }
                }
            }
        }

        if queue.is_empty() {
            self.remove_raw(&full_queue_key)?;
        } else {
            self.set_raw(&full_queue_key, &queue_bytes)?;
        }

        Ok(val)
    }

    /// Removes and returns the head element of the queue, or None if the queue is empty
    pub fn pop_queue_head<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        self._pop_queue(queue_key.as_ref(), QueuePos::Head)
    }

    /// Removes and returns the tail element of the queue, or None if the queue is empty
    pub fn pop_queue_tail<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        self._pop_queue(queue_key.as_ref(), QueuePos::Tail)
    }

    /// Removes an element by index from the queue, returning the value it had or None if it did not exist (as well
    /// as if the queue itself does not exist).
    ///
    /// This will leave a "hole" in the queue, which means we will skip over it in future iterations, but this could
    /// lead to inefficienies as if you keep only the head and tail elements of a long queue, while removing elements
    /// from the middle.
    pub fn remove_from_queue<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
        idx: usize,
    ) -> Result<Option<Vec<u8>>> {
        let idx = idx as u64;
        let queue_key = queue_key.as_ref();
        let (queue_ph, full_queue_key) = self.make_queue_key(queue_key);
        let _guard = self.lock_list(queue_ph);

        let Some(val) = self.remove_raw(&self.make_queue_item_key(queue_key, idx as u64))? else {
            return Ok(None);
        };

        if let Some(mut queue_bytes) = self.get_raw(&full_queue_key)? {
            let queue = from_bytes_mut::<Queue>(&mut queue_bytes);
            if queue.head_idx == idx {
                queue.head_idx += 1;
            }
            if queue.tail_idx == idx + 1 {
                queue.tail_idx -= 1;
            }
            queue.num_items -= 1;
            if queue.is_empty() {
                self.remove_raw(&full_queue_key)?;
            } else {
                self.set_raw(&full_queue_key, &queue_bytes)?;
            }
        }

        Ok(Some(val))
    }

    /// Discards the queue (dropping all elements in contains). Returns true if it had existed before, false otherwise
    pub fn discard_queue<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<bool> {
        let queue_key = queue_key.as_ref();
        let (queue_ph, full_queue_key) = self.make_queue_key(queue_key);
        let _guard = self.lock_list(queue_ph);

        let Some(queue_bytes) = self.get_raw(&full_queue_key)? else {
            return Ok(false);
        };
        let queue = from_bytes::<Queue>(&queue_bytes);

        for i in queue.head_idx..queue.tail_idx {
            self.remove_raw(&self.make_queue_item_key(queue_key, i as u64))?;
        }

        self.remove_raw(&full_queue_key)?;
        Ok(true)
    }

    fn fetch_queue(&self, queue_key: &[u8]) -> Result<Option<Queue>> {
        let queue_key = queue_key.as_ref();
        let (queue_ph, full_queue_key) = self.make_queue_key(queue_key);
        let _guard = self.lock_list(queue_ph);
        if let Some(queue_bytes) = self.get_raw(&full_queue_key)? {
            Ok(Some(*from_bytes::<Queue>(&queue_bytes)))
        } else {
            Ok(None)
        }
    }

    /// Extends the queue with elements from the given iterator. The queue will be created if it did not exist before,
    /// and elements are pushed at the tail-end of the queue. This is more efficient than calling
    /// [Self::push_to_queue_tail] in a loop
    ///
    /// Note: this is not an atomic (crash-safe) operation: if your program crashes while extending the queue, it
    /// is possible that only some of the elements will have been appended.
    ///
    /// Returns the indices of the elements added (a range)
    pub fn extend_queue<'a, B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
        items: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Range<usize>> {
        let queue_key = queue_key.as_ref();
        let (queue_ph, full_queue_key) = self.make_queue_key(queue_key);
        let _guard = self.lock_list(queue_ph);

        let mut queue_bytes = &mut self
            .get_or_create_raw(
                &full_queue_key,
                bytes_of(&Queue {
                    head_idx: Self::FIRST_QUEUE_IDX,
                    tail_idx: Self::FIRST_QUEUE_IDX,
                    num_items: 0,
                })
                .to_owned(),
            )?
            .value();

        let queue = from_bytes_mut::<Queue>(&mut queue_bytes);

        let first_idx = queue.tail_idx;
        for item in items {
            self.set_raw(
                &self.make_queue_item_key(queue_key, queue.tail_idx),
                item.as_ref(),
            )?;
            queue.tail_idx += 1;
            queue.num_items += 1;
        }

        let indices = first_idx as usize..queue.tail_idx as usize;
        self.set_raw(&full_queue_key, &queue_bytes)?;

        Ok(indices)
    }

    /// Returns (without removing) the head element of the queue, or None if the queue is empty
    pub fn peek_queue_head<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        for res in self.iter_queue(queue_key) {
            return Ok(Some(res?.1));
        }
        Ok(None)
    }

    /// Returns (without removing) the tail element of the queue, or None if the queue is empty
    pub fn peek_queue_tail<B: AsRef<[u8]> + ?Sized>(
        &self,
        queue_key: &B,
    ) -> Result<Option<Vec<u8>>> {
        for res in self.iter_queue_backwards(queue_key) {
            return Ok(Some(res?.1));
        }
        Ok(None)
    }

    /// Returns a forward iterator (head to tail) over the elements of the queue. If the queue does not exist,
    /// this is an empty iterator.
    ///
    /// Note: the iterator will go over the indices that existed when it was created -- new elements that are
    /// pushed afterwards will not be returned
    pub fn iter_queue<'a, B: AsRef<[u8]> + ?Sized>(&'a self, queue_key: &B) -> QueueIterator<'a> {
        QueueIterator {
            store: &self,
            queue_key: queue_key.as_ref().to_owned(),
            range: None,
            fwd: true,
        }
    }

    /// Returns a backward iterator (tail to head) over the elements of the queue. If the queue does not exist,
    /// this is an empty iterator.
    ///
    /// Note: the iterator will go over the indices that existed when it was created -- new elements that are
    /// pushed afterwards will not be returned
    pub fn iter_queue_backwards<'a, B: AsRef<[u8]> + ?Sized>(
        &'a self,
        queue_key: &B,
    ) -> QueueIterator<'a> {
        QueueIterator {
            store: &self,
            queue_key: queue_key.as_ref().to_owned(),
            range: None,
            fwd: false,
        }
    }

    /// Returns a the length of the given queue (number of elements in the queue) or 0 if the queue does not exist
    pub fn queue_len<B: AsRef<[u8]> + ?Sized>(&self, queue_key: &B) -> Result<usize> {
        let Some(queue) = self.fetch_queue(queue_key.as_ref())? else {
            return Ok(0);
        };
        Ok(queue.num_items as usize)
    }
}
