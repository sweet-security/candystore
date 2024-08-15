use crate::{
    hashing::PartedHash,
    shard::KVPair,
    store::{ITEM_NAMESPACE, LIST_NAMESPACE},
    CandyStore, Result,
};

use bytemuck::{bytes_of, from_bytes, Pod, Zeroable};
use parking_lot::MutexGuard;

#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub(crate) struct LinkedList {
    pub _head: PartedHash,
    pub _tail: PartedHash,
    pub _head_collidx: u16,
    pub _tail_collidx: u16,
    pub _reserved: u32,
}

impl LinkedList {
    pub fn new(head: FullPartedHash, tail: FullPartedHash) -> Self {
        Self {
            _head: head.ph,
            _head_collidx: head.collidx,
            _tail: tail.ph,
            _tail_collidx: tail.collidx,
            ..Default::default()
        }
    }
    pub fn full_tail(&self) -> FullPartedHash {
        FullPartedHash::new(self._tail, self._tail_collidx)
    }
    pub fn full_head(&self) -> FullPartedHash {
        FullPartedHash::new(self._head, self._head_collidx)
    }
    pub fn set_full_tail(&mut self, fph: FullPartedHash) {
        self._tail = fph.ph;
        self._tail_collidx = fph.collidx;
    }
    pub fn set_full_head(&mut self, fph: FullPartedHash) {
        self._head = fph.ph;
        self._head_collidx = fph.collidx;
    }
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, Default)]
#[repr(C)]
pub(crate) struct Chain {
    pub _prev: PartedHash,
    pub _next: PartedHash,
    pub _prev_collidx: u16,
    pub _next_collidx: u16,
    pub _this_collidx: u16,
    pub _reserved: u16,
}

impl Chain {
    pub fn new(this_collidx: u16, next_fph: FullPartedHash, prev_fph: FullPartedHash) -> Self {
        Self {
            _this_collidx: this_collidx,
            _next: next_fph.ph,
            _next_collidx: next_fph.collidx,
            _prev: prev_fph.ph,
            _prev_collidx: prev_fph.collidx,
            ..Default::default()
        }
    }
    pub fn full_next(&self) -> FullPartedHash {
        FullPartedHash::new(self._next, self._next_collidx)
    }
    pub fn full_prev(&self) -> FullPartedHash {
        FullPartedHash::new(self._prev, self._prev_collidx)
    }
    pub fn set_full_next(&mut self, fph: FullPartedHash) {
        self._next = fph.ph;
        self._next_collidx = fph.collidx;
    }
    pub fn set_full_prev(&mut self, fph: FullPartedHash) {
        self._prev = fph.ph;
        self._prev_collidx = fph.collidx;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FullPartedHash {
    pub ph: PartedHash,
    pub collidx: u16,
}
impl FullPartedHash {
    pub const INVALID: Self = Self {
        ph: PartedHash::INVALID,
        collidx: 0,
    };

    pub fn new(ph: PartedHash, collidx: u16) -> Self {
        Self { ph, collidx }
    }
    pub fn is_valid(&self) -> bool {
        self.ph.is_valid()
    }
}
impl std::fmt::Display for FullPartedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.ph, self.collidx)
    }
}

pub(crate) const ITEM_SUFFIX_LEN: usize = size_of::<PartedHash>() + ITEM_NAMESPACE.len();

pub(crate) fn chain_of(buf: &[u8]) -> Chain {
    bytemuck::pod_read_unaligned(&buf[buf.len() - size_of::<Chain>()..])
}
pub(crate) fn update_chain_prev(val: &mut Vec<u8>, fph: FullPartedHash) {
    let offset = val.len() - size_of::<Chain>();
    let mut chain: Chain = bytemuck::pod_read_unaligned(&val[offset..]);
    chain.set_full_prev(fph);
    val[offset..].copy_from_slice(bytes_of(&chain));
}

pub(crate) fn update_chain_next(val: &mut Vec<u8>, fph: FullPartedHash) {
    let offset = val.len() - size_of::<Chain>();
    let mut chain: Chain = bytemuck::pod_read_unaligned(&val[offset..]);
    chain.set_full_next(fph);
    val[offset..].copy_from_slice(bytes_of(&chain));
}

pub struct LinkedListIterator<'a> {
    store: &'a CandyStore,
    list_key: Vec<u8>,
    list_ph: PartedHash,
    fph: Option<FullPartedHash>,
}

impl<'a> Iterator for LinkedListIterator<'a> {
    type Item = Result<Option<KVPair>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.fph.is_none() {
            let buf = match self.store.get_raw(&self.list_key) {
                Ok(buf) => buf,
                Err(e) => return Some(Err(e)),
            };
            let Some(buf) = buf else {
                return None;
            };
            let list = *from_bytes::<LinkedList>(&buf);
            match self.store.find_true_head(self.list_ph, list.full_head()) {
                Ok((fph, _, _)) => {
                    self.fph = Some(fph);
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
        let Some(fph) = self.fph else {
            return None;
        };
        if fph.ph.is_invalid() {
            return None;
        }
        let kv = match self.store._list_get(self.list_ph, fph) {
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
        self.fph = Some(chain.full_next());
        v.truncate(v.len() - size_of::<Chain>());

        Some(Ok(Some((k, v))))
    }
}

// it doesn't really make sense to implement DoubleEndedIterator here, because we'd have to maintain both
// pointers and the protocol says iteration ends when they meet in the middle
pub struct RevLinkedListIterator<'a> {
    store: &'a CandyStore,
    list_key: Vec<u8>,
    list_ph: PartedHash,
    fph: Option<FullPartedHash>,
}

impl<'a> Iterator for RevLinkedListIterator<'a> {
    type Item = Result<Option<KVPair>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.fph.is_none() {
            let buf = match self.store.get_raw(&self.list_key) {
                Ok(buf) => buf,
                Err(e) => return Some(Err(e)),
            };
            let Some(buf) = buf else {
                return None;
            };
            let list = *from_bytes::<LinkedList>(&buf);
            match self.store.find_true_tail(self.list_ph, list.full_tail()) {
                Ok((fph, _, _)) => {
                    self.fph = Some(fph);
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
        let Some(fph) = self.fph else {
            return None;
        };
        if fph.ph.is_invalid() {
            return None;
        }
        let kv = match self.store._list_get(self.list_ph, fph) {
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
        self.fph = Some(chain.full_prev());
        v.truncate(v.len() - size_of::<Chain>());

        Some(Ok(Some((k, v))))
    }
}

macro_rules! corrupted_list {
    ($($arg:tt)*) => {
        anyhow::bail!(crate::CandyError::CorruptedLinkedList(format!($($arg)*)));
    };
}
pub(crate) use corrupted_list;

pub(crate) enum InsertPosition {
    Head,
    Tail,
}

#[derive(Debug)]
pub(crate) enum InsertToListStatus {
    ExistingValue(Vec<u8>),
    WrongValue(Vec<u8>),
    CreatedNew(Vec<u8>),
    DoesNotExist,
}

impl CandyStore {
    pub(crate) fn make_list_key(&self, mut list_key: Vec<u8>) -> (PartedHash, Vec<u8>) {
        list_key.extend_from_slice(LIST_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &list_key), list_key)
    }

    pub(crate) fn make_item_key(
        &self,
        list_ph: PartedHash,
        mut item_key: Vec<u8>,
    ) -> (PartedHash, Vec<u8>) {
        item_key.extend_from_slice(bytes_of(&list_ph));
        item_key.extend_from_slice(ITEM_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &item_key), item_key)
    }

    pub(crate) fn _list_lock(&self, list_ph: PartedHash) -> MutexGuard<()> {
        self.keyed_locks[(list_ph.signature() & self.keyed_locks_mask) as usize].lock()
    }

    pub(crate) fn _list_get_free_collidx(
        &self,
        list_ph: PartedHash,
        item_ph: PartedHash,
    ) -> Result<u16> {
        let mut suffix = [0u8; ITEM_SUFFIX_LEN];
        suffix[0..PartedHash::LEN].copy_from_slice(bytes_of(&list_ph));
        suffix[PartedHash::LEN..].copy_from_slice(ITEM_NAMESPACE);
        let mut max_seen = None;

        for res in self.get_by_hash(item_ph)? {
            let (k, v) = res?;
            if k.ends_with(&suffix) {
                let chain = chain_of(&v);
                if max_seen.is_none()
                    || max_seen.is_some_and(|max_seen| chain._this_collidx > max_seen)
                {
                    max_seen = Some(chain._this_collidx);
                }
            }
        }
        Ok(max_seen.map(|max_seen| max_seen + 1).unwrap_or(0))
    }

    pub(crate) fn _list_get(
        &self,
        list_ph: PartedHash,
        item_fph: FullPartedHash,
    ) -> Result<Option<KVPair>> {
        let mut suffix = [0u8; ITEM_SUFFIX_LEN];
        suffix[0..PartedHash::LEN].copy_from_slice(bytes_of(&list_ph));
        suffix[PartedHash::LEN..].copy_from_slice(ITEM_NAMESPACE);

        for res in self.get_by_hash(item_fph.ph)? {
            let (k, v) = res?;
            if k.ends_with(&suffix) {
                let chain = chain_of(&v);
                if chain._this_collidx == item_fph.collidx {
                    return Ok(Some((k, v)));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn find_true_tail(
        &self,
        list_ph: PartedHash,
        fph: FullPartedHash,
    ) -> Result<(FullPartedHash, Vec<u8>, Vec<u8>)> {
        let mut curr = fph;
        let mut last_valid = None;

        loop {
            if let Some((k, v)) = self._list_get(list_ph, curr)? {
                let chain = chain_of(&v);

                if chain._next.is_invalid() {
                    // curr is the true tail
                    return Ok((curr, k, v));
                }
                last_valid = Some((curr, k, v));
                curr = chain.full_next();

                assert!(curr != fph, "loop detected");
            } else if let Some(last_valid) = last_valid {
                // last_valid is the true tail
                assert_ne!(curr, fph);
                return Ok(last_valid);
            } else {
                // if prev=None, it means we weren't able to find list.tail. this should never happen
                assert_eq!(curr, fph);
                corrupted_list!("list {list_ph} tail {fph} does not exist");
            }
        }
    }

    pub(crate) fn find_true_head(
        &self,
        list_ph: PartedHash,
        fph: FullPartedHash,
    ) -> Result<(FullPartedHash, Vec<u8>, Vec<u8>)> {
        let mut curr = fph;
        let mut last_valid = None;

        loop {
            if let Some((k, v)) = self._list_get(list_ph, curr)? {
                let chain = chain_of(&v);
                if chain._prev.is_invalid() {
                    // curr is the true head
                    return Ok((curr, k, v));
                }
                last_valid = Some((curr, k, v));
                curr = chain.full_prev();
                assert!(curr != fph, "loop detected");
            } else if let Some(last_valid) = last_valid {
                // last_valid is the true head
                assert_ne!(curr, fph);
                return Ok(last_valid);
            } else {
                // if prev=None, it means we weren't able to find list.head. this should never happen
                assert_eq!(curr, fph);
                corrupted_list!("list {list_ph} head {fph} does not exist");
            }
        }
    }

    ///
    /// See also [Self::get]
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
        let Some(mut v) = self.get_raw(&item_key)? else {
            return Ok(None);
        };
        v.truncate(v.len() - size_of::<Chain>());
        Ok(Some(v))
    }

    /// Iterates over the elements of the linked list (identified by `list_key`) from the beginning (head)
    /// to the end (tail). Note that if items are removed at random locations in the list, the iterator
    /// may not be able to progress and will return an early stop.
    pub fn iter_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> LinkedListIterator {
        self.owned_iter_list(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::iter_list]
    pub fn owned_iter_list(&self, list_key: Vec<u8>) -> LinkedListIterator {
        let (list_ph, list_key) = self.make_list_key(list_key);
        LinkedListIterator {
            store: &self,
            list_key,
            list_ph,
            fph: None,
        }
    }

    /// Same as [Self::iter_list], but goes from the end (tail) to the beginning (head)
    pub fn iter_list_backwards<B: AsRef<[u8]> + ?Sized>(
        &self,
        list_key: &B,
    ) -> RevLinkedListIterator {
        self.owned_iter_list_backwards(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::iter_list_backwards]
    pub fn owned_iter_list_backwards(&self, list_key: Vec<u8>) -> RevLinkedListIterator {
        let (list_ph, list_key) = self.make_list_key(list_key);
        RevLinkedListIterator {
            store: &self,
            list_key,
            list_ph,
            fph: None,
        }
    }

    /// Discards the given list (removes all elements). This also works for corrupt lists, in case they
    /// need to be dropped.
    pub fn discard_list<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<()> {
        self.owned_discard_list(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::discard_list]
    pub fn owned_discard_list(&self, list_key: Vec<u8>) -> Result<()> {
        let (list_ph, list_key) = self.make_list_key(list_key);

        let _guard = self._list_lock(list_ph);

        let Some(list_buf) = self.remove_raw(&list_key)? else {
            return Ok(());
        };
        let list = *from_bytes::<LinkedList>(&list_buf);
        let mut fph = list.full_head();

        while fph.is_valid() {
            let Some((k, v)) = self._list_get(list_ph, fph)? else {
                break;
            };

            let chain = chain_of(&v);
            fph = chain.full_next();
            self.remove_raw(&k)?;
        }

        Ok(())
    }

    // optimization: add singly-linked lists that allow removing only from the head and inserting
    // only at the tail, but support O(1) access (by index) and update of existing elements.
    // this would require only 2 IOs instead of 3 when inserting new elements.

    /// Returns the first (head) element of the list. Note that it's prone to spurious false positives
    /// (returning an element that no longer exists) or false negatives (returning `None` when an element
    /// exists) in case different threads pop the head
    pub fn peek_list_head<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_peek_list_head(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::peek_list_head]
    pub fn owned_peek_list_head(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        self.owned_iter_list(list_key).next().unwrap_or(Ok(None))
    }

    /// Returns the last (tail) element of the list. Note that it's prone to spurious false positives
    /// (returning an element that no longer exists) or false negatives (returning `None` when an element
    /// exists) in case different threads pop the tail
    pub fn peek_list_tail<B: AsRef<[u8]> + ?Sized>(&self, list_key: &B) -> Result<Option<KVPair>> {
        self.owned_peek_list_tail(list_key.as_ref().to_owned())
    }

    /// Owned version of [Self::peek_list_tail]
    pub fn owned_peek_list_tail(&self, list_key: Vec<u8>) -> Result<Option<KVPair>> {
        self.owned_iter_list_backwards(list_key)
            .next()
            .unwrap_or(Ok(None))
    }
}
