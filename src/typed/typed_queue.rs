use std::{borrow::Borrow, marker::PhantomData, ops::Range};

use databuf::{DecodeOwned, Encode, config::num::LE};

use crate::types::TYPED_QUEUE_NS;

use super::{CandyTypedKey, StoreRef, append_type_id, decode_from_bytes};

/// A typed wrapper around CandyStore's queue functionality.
///
/// This struct provides a type-safe interface for managing double-ended queues (deques).
/// Queues are identified by a queue key (L), and contain values (V).
pub struct CandyTypedDeque<L, V> {
    store: StoreRef,
    _phantom: PhantomData<(L, V)>,
}

impl<L, V> Clone for CandyTypedDeque<L, V> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            _phantom: Default::default(),
        }
    }
}

impl<L, V> CandyTypedDeque<L, V>
where
    L: CandyTypedKey + Encode,
    V: Encode + DecodeOwned,
{
    /// Creates a new `CandyTypedDeque` wrapping the given `CandyStore`.
    pub fn new(store: StoreRef) -> Self {
        Self {
            store,
            _phantom: Default::default(),
        }
    }

    fn make_queue_key<Q: ?Sized + Encode>(queue_key: &Q) -> Vec<u8>
    where
        L: Borrow<Q>,
    {
        let qbytes = queue_key.to_bytes::<LE>();
        append_type_id(qbytes, L::TYPE_ID)
    }

    /// Pushes a value to the tail of the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    /// * `val` - The value to push.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    pub fn push_tail<Q: ?Sized + Encode, QV: ?Sized + Encode>(
        &self,
        queue_key: &Q,
        val: &QV,
    ) -> crate::Result<()>
    where
        L: Borrow<Q>,
        V: Borrow<QV>,
    {
        let qkey = Self::make_queue_key(queue_key);
        let vbytes = val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;
        self.store
            .queue_push_tail_with_ns(TYPED_QUEUE_NS, &qkey, &vbytes)
            .map(|_| ())
    }

    /// Pushes a value to the head of the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    /// * `val` - The value to push.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    pub fn push_head<Q: ?Sized + Encode, QV: ?Sized + Encode>(
        &self,
        queue_key: &Q,
        val: &QV,
    ) -> crate::Result<()>
    where
        L: Borrow<Q>,
        V: Borrow<QV>,
    {
        let qkey = Self::make_queue_key(queue_key);
        let vbytes = val.to_bytes::<LE>();
        self.store.ensure_user_value_len(vbytes.len())?;
        self.store
            .queue_push_head_with_ns(TYPED_QUEUE_NS, &qkey, &vbytes)?;
        Ok(())
    }

    /// Removes and returns the value at the head of the queue, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn pop_head_with_idx<Q: ?Sized + Encode>(
        &self,
        queue_key: &Q,
    ) -> crate::Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_pop_head_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, v)) => Ok(Some((idx as usize, decode_from_bytes::<V>(&v)?))),
            None => Ok(None),
        }
    }

    /// Removes and returns the value at the head of the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn pop_head<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.pop_head_with_idx(queue_key)?.map(|(_, v)| v))
    }

    /// Removes and returns the value at the tail of the queue, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn pop_tail_with_idx<Q: ?Sized + Encode>(
        &self,
        queue_key: &Q,
    ) -> crate::Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_pop_tail_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, v)) => Ok(Some((idx as usize, decode_from_bytes::<V>(&v)?))),
            None => Ok(None),
        }
    }

    /// Removes and returns the value at the tail of the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn pop_tail<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.pop_tail_with_idx(queue_key)?.map(|(_, v)| v))
    }

    /// Returns the value at the head of the queue without removing it, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn peek_head_with_idx<Q: ?Sized + Encode>(
        &self,
        queue_key: &Q,
    ) -> crate::Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_peek_head_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, v)) => Ok(Some((idx as usize, decode_from_bytes::<V>(&v)?))),
            None => Ok(None),
        }
    }

    /// Returns the value at the head of the queue without removing it.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn peek_head<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.peek_head_with_idx(queue_key)?.map(|(_, v)| v))
    }

    /// Returns the value at the tail of the queue without removing it, along with its index.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// A tuple containing the index and value of the item, or `None` if the queue is empty.
    pub fn peek_tail_with_idx<Q: ?Sized + Encode>(
        &self,
        queue_key: &Q,
    ) -> crate::Result<Option<(usize, V)>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        match self.store.queue_peek_tail_with_ns(TYPED_QUEUE_NS, &qkey)? {
            Some((idx, v)) => Ok(Some((idx as usize, decode_from_bytes::<V>(&v)?))),
            None => Ok(None),
        }
    }

    /// Returns the value at the tail of the queue without removing it.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The value of the item, or `None` if the queue is empty.
    pub fn peek_tail<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<Option<V>>
    where
        L: Borrow<Q>,
    {
        Ok(self.peek_tail_with_idx(queue_key)?.map(|(_, v)| v))
    }

    /// Returns the number of items in the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The number of items in the queue.
    pub fn len<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<usize>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store
            .queue_len_with_ns(TYPED_QUEUE_NS, &qkey)
            .map(|len| len as usize)
    }

    /// Returns the range of indices occupied by the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// The range of indices occupied by the queue.
    pub fn range<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<Range<usize>>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store.queue_range_with_ns(TYPED_QUEUE_NS, &qkey)
    }

    /// Checks if the queue is empty.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// `true` if the queue is empty, `false` otherwise.
    pub fn is_empty<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<bool>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store
            .queue_len_with_ns(TYPED_QUEUE_NS, &qkey)
            .map(|len| len == 0)
    }

    /// Removes all items from the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// `true` if the queue existed and was removed, `false` otherwise.
    pub fn discard<Q: ?Sized + Encode>(&self, queue_key: &Q) -> crate::Result<bool>
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store.queue_discard_with_ns(TYPED_QUEUE_NS, &qkey)
    }
    /// Returns an iterator over the items in the queue.
    ///
    /// # Arguments
    ///
    /// * `queue_key` - The key identifying the queue.
    ///
    /// # Returns
    ///
    /// An iterator over the items in the queue.
    pub fn iter<'a, Q: ?Sized + Encode>(
        &'a self,
        queue_key: &Q,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(usize, V)>> + 'a
    where
        L: Borrow<Q>,
    {
        let qkey = Self::make_queue_key(queue_key);
        self.store
            .queue_iter_with_ns(TYPED_QUEUE_NS, &qkey)
            .map(|res| res.and_then(|(idx, v)| decode_from_bytes::<V>(&v).map(|val| (idx, val))))
    }
}
