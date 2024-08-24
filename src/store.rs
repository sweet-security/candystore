use anyhow::{anyhow, bail};
use fslock::LockFile;
use parking_lot::Mutex;
use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::shard::{NUM_ROWS, ROW_WIDTH};
use crate::{
    hashing::PartedHash,
    router::ShardRouter,
    shard::{InsertMode, InsertStatus, KVPair, HEADER_SIZE},
    HashSeed,
};

use crate::{CandyError, Config, Result, MAX_TOTAL_KEY_SIZE, MAX_VALUE_SIZE};

pub(crate) const USER_NAMESPACE: &[u8] = &[1];
pub(crate) const TYPED_NAMESPACE: &[u8] = &[2];
pub(crate) const LIST_NAMESPACE: &[u8] = &[3];
pub(crate) const ITEM_NAMESPACE: &[u8] = &[4];
pub(crate) const CHAIN_NAMESPACE: u8 = 5;

/// Stats from CandyStore
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Stats {
    /// number of shards in the store
    pub num_shards: usize,
    /// number of items inserted (including ones that were later removed)
    pub num_inserted: usize,
    /// number of items removed
    pub num_removed: usize,
    /// total number of bytes wasted by overwrites or removed items, before being compacted
    pub wasted_bytes: usize,
    /// total number of bytes used for the data, including wasted bytes but not including file headers
    pub used_bytes: usize,
}

impl Stats {
    /// the size of the shard file's headers
    pub const FILE_HEADER_SIZE: usize = HEADER_SIZE as usize;

    /// the number of items in the store
    pub fn len(&self) -> usize {
        self.num_inserted - self.num_removed
    }

    /// the total number of bytes used by the items in the store
    pub fn data_bytes(&self) -> usize {
        self.used_bytes - self.wasted_bytes
    }

    /// compute the average entry size
    pub fn average_entry_size(&self) -> usize {
        self.data_bytes().checked_div(self.len()).unwrap_or(0)
    }

    /// total bytes consumed by the store (including headers and wasted bytes)
    pub fn total_bytes(&self) -> usize {
        self.used_bytes + Self::FILE_HEADER_SIZE * self.num_shards
    }
}

/// A histogram of inserted entry sizes, in three bucket sizes:
/// * up to 1KB we keep 64-byte resolution
/// * from 1KB-16K, we keep in 1KB resolution
/// * over 16K, we keep in 16K resolution
///
/// Notes:
/// * Entry sizes are rounded down to the nearest bucket, e.g., 100 goes to the bucket of [64..128)
/// * Counts are updated on insert, and are unchanged by removals. They represent the entry sizes "seen" by this
///   store, not the currently existing ones. When a shard is split or compacted, only the existing entries remain
///   in the histogram.
/// * Use [Self::iter] to get a user-friendly representation of the histogram
#[derive(Clone, Debug, Default)]
pub struct SizeHistogram {
    pub counts_64b: [usize; 16],
    pub counts_1kb: [usize; 15],
    pub counts_16kb: [usize; 4],
}

/// A coarse version of [SizeHistogram]
#[derive(Clone, Debug, Default)]
pub struct CoarseHistogram {
    pub under512: usize,
    pub under1k: usize,
    pub under4k: usize,
    pub under16k: usize,
    pub under32k: usize,
    pub over32k: usize,
}

impl SizeHistogram {
    /// return the count of the bucket for the given `sz`
    pub fn get(&self, sz: usize) -> usize {
        if sz < 1024 {
            self.counts_64b[sz / 64]
        } else if sz < 16 * 1024 {
            self.counts_1kb[(sz - 1024) / 1024]
        } else {
            self.counts_16kb[(sz - 16 * 1024) / (16 * 1024)]
        }
    }

    pub fn to_coarse(&self) -> CoarseHistogram {
        let mut coarse = CoarseHistogram::default();
        for (r, c) in self.iter() {
            let which = match r.end {
                0..=512 => &mut coarse.under512,
                513..=1024 => &mut coarse.under1k,
                1025..=4096 => &mut coarse.under4k,
                4097..=16384 => &mut coarse.under16k,
                16385..=32768 => &mut coarse.under32k,
                _ => &mut coarse.over32k,
            };
            *which += c;
        }

        coarse
    }

    /// iterate over all non-empty buckets, and return their spans and counts
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (Range<usize>, usize)> + 'a {
        self.counts_64b
            .iter()
            .enumerate()
            .filter_map(|(i, &c)| {
                if c == 0 {
                    return None;
                }
                Some((i * 64..(i + 1) * 64, c))
            })
            .chain(self.counts_1kb.iter().enumerate().filter_map(|(i, &c)| {
                if c == 0 {
                    return None;
                }
                Some(((i + 1) * 1024..(i + 2) * 1024, c))
            }))
            .chain(self.counts_16kb.iter().enumerate().filter_map(|(i, &c)| {
                if c == 0 {
                    return None;
                }
                Some(((i + 1) * 16 * 1024..(i + 2) * 16 * 1024, c))
            }))
    }
}

impl std::fmt::Display for SizeHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (r, c) in self.iter() {
            if r.end == usize::MAX {
                write!(f, "[{}..): {c}\n", r.start)?;
            } else {
                write!(f, "[{}..{}): {c}\n", r.start, r.end)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InternalConfig {
    pub dir_path: PathBuf,
    pub max_shard_size: u32,
    pub min_compaction_threashold: u32,
    pub hash_seed: HashSeed,
    pub expected_number_of_keys: usize,
    pub max_concurrent_list_ops: u32,
    pub truncate_up: bool,
    pub clear_on_unsupported_version: bool,
    pub mlock_headers: bool,
    #[cfg(feature = "flush_aggregation")]
    pub flush_aggregation_delay: Option<std::time::Duration>,
    pub num_of_compaction_stats: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceStatus {
    PrevValue(Vec<u8>),
    WrongValue(Vec<u8>),
    DoesNotExist,
}
impl ReplaceStatus {
    pub fn was_replaced(&self) -> bool {
        matches!(*self, Self::PrevValue(_))
    }
    pub fn failed(&self) -> bool {
        !matches!(*self, Self::PrevValue(_))
    }
    pub fn is_key_missing(&self) -> bool {
        matches!(*self, Self::DoesNotExist)
    }
    pub fn is_wrong_value(&self) -> bool {
        matches!(*self, Self::WrongValue(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetStatus {
    PrevValue(Vec<u8>),
    CreatedNew,
}
impl SetStatus {
    pub fn was_created(&self) -> bool {
        matches!(*self, Self::CreatedNew)
    }
    pub fn was_replaced(&self) -> bool {
        matches!(*self, Self::PrevValue(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetOrCreateStatus {
    ExistingValue(Vec<u8>),
    CreatedNew(Vec<u8>),
}
impl GetOrCreateStatus {
    pub fn was_created(&self) -> bool {
        matches!(*self, Self::CreatedNew(_))
    }
    pub fn already_exists(&self) -> bool {
        matches!(*self, Self::ExistingValue(_))
    }
    pub fn value(self) -> Vec<u8> {
        match self {
            Self::CreatedNew(val) => val,
            Self::ExistingValue(val) => val,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompactionKind {
    // reclaimed size (bytes)
    Compaction(u32),
    // sizes of bottom and top shards in bytes
    Split(u32, u32),
}

pub(crate) struct CompactionStats {
    last_fetch: AtomicUsize,
    counter: AtomicUsize,
    mask: usize,
    stats: Mutex<Vec<(CompactionKind, Duration)>>,
}

impl CompactionStats {
    fn new(size: usize) -> Self {
        let mask = if size == 0 {
            0
        } else if size.is_power_of_two() {
            size - 1
        } else {
            (1 << (size.ilog2() + 1)) - 1
        };
        Self {
            last_fetch: Default::default(),
            counter: Default::default(),
            mask,
            stats: Mutex::new(vec![
                (CompactionKind::Compaction(0), Duration::ZERO);
                mask + 1
            ]),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.mask != 0
    }

    pub(crate) fn push_stat(&self, t0: Instant, kind: CompactionKind) {
        debug_assert!(self.is_enabled());
        let dur = Instant::now().duration_since(t0);
        let mut stats = self.stats.lock();
        let cnt = self.counter.fetch_add(1, Ordering::SeqCst);
        stats[cnt & self.mask] = (kind, dur);
    }

    pub fn fetch(&self) -> (usize, Vec<(CompactionKind, Duration)>) {
        let stats = self.stats.lock();
        let cnt = self.counter.load(Ordering::Relaxed);
        let last_fetch = self.last_fetch.load(Ordering::Relaxed);
        self.last_fetch.store(cnt, Ordering::Relaxed);
        let mut durs = Vec::with_capacity(stats.len());

        if self.is_enabled() {
            for i in last_fetch.max(cnt.checked_sub(stats.len()).unwrap_or(last_fetch))..cnt {
                durs.push(stats[i & self.mask]);
            }
        }
        (cnt, durs)
    }
}

/// The CandyStore object. Note that it's fully sync'ed, so can be shared between threads using `Arc`
pub struct CandyStore {
    pub(crate) root: ShardRouter,
    pub(crate) config: Arc<InternalConfig>,
    // stats
    pub(crate) num_entries: AtomicUsize,
    pub(crate) num_compactions: Arc<AtomicUsize>,
    pub(crate) num_splits: Arc<AtomicUsize>,
    // locks for complicated operations
    pub(crate) keyed_locks_mask: u32,
    pub(crate) keyed_locks: Vec<Mutex<()>>,
    _lockfile: LockFile,
    compaction_stats: Arc<CompactionStats>,
}

/// An iterator over a CandyStore. Note that it's safe to modify (insert/delete) keys while iterating,
/// but the results of the iteration may or may not include these changes. This is considered a
/// well-defined behavior of the store.
pub struct CandyStoreIterator<'a> {
    store: &'a CandyStore,
    shard_selector: u32,
    row_idx: usize,
    entry_idx: usize,
    raw: bool,
}

impl<'a> CandyStoreIterator<'a> {
    fn new(store: &'a CandyStore, raw: bool) -> Self {
        Self {
            store,
            shard_selector: 0,
            row_idx: 0,
            entry_idx: 0,
            raw,
        }
    }

    /// Returns the cookie of the next item in the store. This can be used later to construct an iterator
    /// that starts at the given point.
    pub fn cookie(&self) -> u64 {
        ((self.shard_selector as u64 & 0xffff) << 32)
            | ((self.row_idx as u64 & 0xffff) << 16)
            | (self.entry_idx as u64 & 0xffff)
    }

    // Constructs an iterator starting at the given cookie
    pub fn from_cookie(store: &'a CandyStore, cookie: u64, raw: bool) -> Self {
        Self {
            store,
            shard_selector: ((cookie >> 32) & 0xffff) as u32,
            row_idx: ((cookie >> 16) & 0xffff) as usize,
            entry_idx: (cookie & 0xffff) as usize,
            raw,
        }
    }
}

impl<'a> Iterator for CandyStoreIterator<'a> {
    type Item = Result<KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.shard_selector < ShardRouter::END_OF_SHARDS {
            let res = self.store.root.shared_op(self.shard_selector, |sh| {
                while self.row_idx < NUM_ROWS {
                    let row_idx = self.row_idx;
                    let entry_idx = self.entry_idx;

                    self.entry_idx += 1;
                    if self.entry_idx >= ROW_WIDTH {
                        self.entry_idx = 0;
                        self.row_idx += 1;
                    }

                    let Some((mut k, v)) = sh.read_at(row_idx, entry_idx)? else {
                        continue;
                    };
                    if self.raw {
                        return Ok((sh.span.start, Some((k, v))));
                    } else if k.ends_with(USER_NAMESPACE) {
                        k.truncate(k.len() - USER_NAMESPACE.len());
                        return Ok((sh.span.start, Some((k, v))));
                    }
                }

                self.entry_idx = 0;
                self.row_idx = 0;
                Ok((sh.span.end, None))
            });

            match res {
                Ok((shard_selector, kv)) => {
                    self.shard_selector = shard_selector;
                    if let Some(kv) = kv {
                        return Some(Ok(kv));
                    }
                    // continue
                }
                Err(e) => return Some(Err(e)),
            }
        }

        None
    }
}

impl CandyStore {
    /// Opens or creates a new CandyStore.
    /// * dir_path - the directory where shards will be kept
    /// * config - the configuration options for the store
    pub fn open(dir_path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let config = Arc::new(InternalConfig {
            dir_path: dir_path.as_ref().to_path_buf(),
            expected_number_of_keys: config.expected_number_of_keys,
            hash_seed: config.hash_seed,
            max_concurrent_list_ops: config.max_concurrent_list_ops,
            max_shard_size: config.max_shard_size,
            min_compaction_threashold: config.min_compaction_threashold,
            truncate_up: config.truncate_up,
            clear_on_unsupported_version: config.clear_on_unsupported_version,
            mlock_headers: config.mlock_headers,
            #[cfg(feature = "flush_aggregation")]
            flush_aggregation_delay: config.flush_aggregation_delay,
            num_of_compaction_stats: config.num_of_compaction_stats,
        });

        std::fs::create_dir_all(dir_path)?;
        let lockfilename = config.dir_path.join(".lock");
        let mut lockfile = fslock::LockFile::open(&lockfilename)?;
        if !lockfile.try_lock_with_pid()? {
            bail!("Lock file {lockfilename:?} is used by another process");
        }

        let mut num_keyed_locks = config.max_concurrent_list_ops.max(4);
        if !num_keyed_locks.is_power_of_two() {
            num_keyed_locks = 1 << (num_keyed_locks.ilog2() + 1);
        }

        let mut keyed_locks = vec![];
        for _ in 0..num_keyed_locks {
            keyed_locks.push(Mutex::new(()));
        }

        let num_compactions = Arc::new(AtomicUsize::new(0));
        let num_splits = Arc::new(AtomicUsize::new(0));

        let root = ShardRouter::new(config.clone(), num_compactions.clone(), num_splits.clone())?;
        let compaction_stats = Arc::new(CompactionStats::new(config.num_of_compaction_stats));

        Ok(Self {
            config,
            root,
            num_entries: Default::default(),
            num_compactions,
            num_splits,
            keyed_locks_mask: num_keyed_locks - 1,
            keyed_locks,
            _lockfile: lockfile,
            compaction_stats,
        })
    }

    /// Syncs all in-memory changes of all shards to disk. Concurrent changes are allowed while
    /// flushing, and may result in partially-sync'ed store. Use sparingly, as this is a costly operaton.
    pub fn flush(&self) -> Result<()> {
        self.root.call_on_all_shards(|sh| sh.flush())?;
        Ok(())
    }

    /// Clears the store (erasing all keys), and removing all shard files
    pub fn clear(&self) -> Result<()> {
        self.root.clear()?;

        self.num_entries.store(0, Ordering::Relaxed);
        self.num_compactions.store(0, Ordering::Relaxed);
        self.num_splits.store(0, Ordering::Relaxed);

        Ok(())
    }

    pub(crate) fn make_user_key(&self, mut key: Vec<u8>) -> Vec<u8> {
        key.extend_from_slice(USER_NAMESPACE);
        key
    }

    pub(crate) fn get_by_hash(&self, ph: PartedHash) -> Result<Vec<KVPair>> {
        debug_assert!(ph.is_valid());
        self.root
            .shared_op(ph.shard_selector() as u32, |sh| sh.get_by_hash(ph))
    }

    pub(crate) fn get_raw(&self, full_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);
        self.root
            .shared_op(ph.shard_selector() as u32, |sh| sh.get(ph, &full_key))
    }

    /// Gets the value of a key from the store. If the key does not exist, `None` will be returned.
    /// The data is fully-owned, no references are returned.
    pub fn get<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        self.owned_get(key.as_ref().to_owned())
    }

    /// Same as [Self::get] but takes an owned key
    pub fn owned_get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.get_raw(&self.make_user_key(key))
    }

    /// Checks whether the given key exists in the store
    pub fn contains<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<bool> {
        self.owned_contains(key.as_ref().to_owned())
    }

    /// Same as [Self::contains] but takes an owned key
    pub fn owned_contains(&self, key: Vec<u8>) -> Result<bool> {
        Ok(self.get_raw(&self.make_user_key(key))?.is_some())
    }

    pub(crate) fn remove_raw(&self, full_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);

        let val = self
            .root
            .shared_op(ph.shard_selector() as u32, |sh| sh.remove(ph, &full_key))?;
        if val.is_some() {
            self.num_entries.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(val)
    }

    /// Removes a key-value pair from the store, returning `None` if the key did not exist,
    /// or `Some(old_value)` if it did
    pub fn remove<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        self.owned_remove(key.as_ref().to_owned())
    }

    /// Same as [Self::remove] but takes an owned key
    pub fn owned_remove(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.remove_raw(&self.make_user_key(key))
    }

    pub(crate) fn insert_internal(
        &self,
        full_key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<InsertStatus> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);

        if full_key.len() > MAX_TOTAL_KEY_SIZE as usize {
            return Err(anyhow!(CandyError::KeyTooLong(full_key.len())));
        }
        if val.len() > MAX_VALUE_SIZE as usize {
            return Err(anyhow!(CandyError::ValueTooLong(val.len())));
        }
        if full_key.len() + val.len() > self.config.max_shard_size as usize {
            return Err(anyhow!(CandyError::EntryCannotFitInShard(
                full_key.len() + val.len(),
                self.config.max_shard_size as usize
            )));
        }

        loop {
            let status =
                self.root
                    .insert(ph, full_key, val, mode, self.compaction_stats.clone())?;

            match status {
                InsertStatus::Added => {
                    self.num_entries.fetch_add(1, Ordering::SeqCst);
                    return Ok(status);
                }
                InsertStatus::KeyDoesNotExist
                | InsertStatus::Replaced(_)
                | InsertStatus::AlreadyExists(_) => {
                    return Ok(status);
                }
                InsertStatus::CompactionNeeded(_) | InsertStatus::SplitNeeded => {
                    unreachable!();
                }
            }
        }
    }

    pub(crate) fn set_raw(&self, full_key: &[u8], val: &[u8]) -> Result<SetStatus> {
        match self.insert_internal(full_key, val, InsertMode::Set)? {
            InsertStatus::Added => Ok(SetStatus::CreatedNew),
            InsertStatus::Replaced(v) => Ok(SetStatus::PrevValue(v)),
            InsertStatus::AlreadyExists(v) => Ok(SetStatus::PrevValue(v)),
            InsertStatus::KeyDoesNotExist => unreachable!(),
            InsertStatus::CompactionNeeded(_) => unreachable!(),
            InsertStatus::SplitNeeded => unreachable!(),
        }
    }

    /// Inserts a key-value pair, creating it or replacing an existing pair. Note that if the program crashed
    /// while or "right after" this operation, or if the operating system is unable to flush the page cache,
    /// you may lose some data. However, you will still be in a consistent state, where you will get a previous
    /// version of the state.
    ///
    /// While this method is O(1) amortized, every so often it will trigger either a shard compaction or a
    /// shard split, which requires rewriting the whole shard. However, unlike LSM trees, this operation is
    /// constant in size
    pub fn set<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<SetStatus> {
        self.owned_set(key.as_ref().to_owned(), val.as_ref())
    }

    /// Same as [Self::set], but the key passed owned to this function
    pub fn owned_set(&self, key: Vec<u8>, val: &[u8]) -> Result<SetStatus> {
        self.set_raw(&self.make_user_key(key), val)
    }

    pub(crate) fn replace_raw(
        &self,
        full_key: &[u8],
        val: &[u8],
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        match self.insert_internal(full_key, val, InsertMode::Replace(expected_val))? {
            InsertStatus::Added => unreachable!(),
            InsertStatus::Replaced(v) => Ok(ReplaceStatus::PrevValue(v)),
            InsertStatus::AlreadyExists(v) => Ok(ReplaceStatus::WrongValue(v)),
            InsertStatus::KeyDoesNotExist => Ok(ReplaceStatus::DoesNotExist),
            InsertStatus::CompactionNeeded(_) => unreachable!(),
            InsertStatus::SplitNeeded => unreachable!(),
        }
    }

    /// Replaces the value of an existing key with a new value. If the key existed, returns
    /// `PrevValue(value)` with its old value, and if it did not, returns `DoesNotExist` but
    /// does not create the key.
    ///
    /// See [Self::set] for more details
    pub fn replace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
        expected_val: Option<&B2>,
    ) -> Result<ReplaceStatus> {
        self.owned_replace(
            key.as_ref().to_owned(),
            val.as_ref(),
            expected_val.map(|ev| ev.as_ref()),
        )
    }

    /// Same as [Self::replace], but the key passed owned to this function
    pub fn owned_replace(
        &self,
        key: Vec<u8>,
        val: &[u8],
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        self.replace_raw(&self.make_user_key(key), val.as_ref(), expected_val)
    }

    pub(crate) fn get_or_create_raw(
        &self,
        full_key: &[u8],
        default_val: Vec<u8>,
    ) -> Result<GetOrCreateStatus> {
        match self.insert_internal(full_key, &default_val, InsertMode::GetOrCreate)? {
            InsertStatus::Added => Ok(GetOrCreateStatus::CreatedNew(default_val)),
            InsertStatus::AlreadyExists(v) => Ok(GetOrCreateStatus::ExistingValue(v)),
            InsertStatus::Replaced(_) => unreachable!(),
            InsertStatus::KeyDoesNotExist => unreachable!(),
            InsertStatus::CompactionNeeded(_) => unreachable!(),
            InsertStatus::SplitNeeded => unreachable!(),
        }
    }

    /// Gets the value of the given key or creates it with the given default value. If the key did not exist,
    /// returns `CreatedNew(default_val)`, and if it did, returns `ExistingValue(value)`.
    /// This is done atomically, so it can be used to create a key only if it did not exist before,
    /// like `open` with `O_EXCL`.
    ///
    /// See [Self::set] for more details
    pub fn get_or_create<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        default_val: &B2,
    ) -> Result<GetOrCreateStatus> {
        self.owned_get_or_create(key.as_ref().to_owned(), default_val.as_ref().to_owned())
    }

    /// Same as [Self::get_or_create], but the `key` and `default_val` are passed owned to this function
    pub fn owned_get_or_create(
        &self,
        key: Vec<u8>,
        default_val: Vec<u8>,
    ) -> Result<GetOrCreateStatus> {
        self.get_or_create_raw(&self.make_user_key(key), default_val)
    }

    /// Ephemeral stats: number of inserts
    pub fn _num_entries(&self) -> usize {
        self.num_entries.load(Ordering::Acquire)
    }
    /// Ephemeral stats: number of compactions performed
    pub fn _num_compactions(&self) -> usize {
        self.num_compactions.load(Ordering::Acquire)
    }
    /// Ephemeral stats: number of splits performed
    pub fn _num_splits(&self) -> usize {
        self.num_splits.load(Ordering::Acquire)
    }

    /// Fetch compaction and split stats, returns the number of compactions/splits and the recent
    pub fn fetch_compaction_stats(&self) -> (usize, Vec<(CompactionKind, Duration)>) {
        self.compaction_stats.fetch()
    }

    /// Returns useful stats about the store
    pub fn stats(&self) -> Stats {
        let stats_vec = self
            .root
            .call_on_all_shards(|sh| Ok(sh.get_stats()))
            .unwrap();
        let mut stats = Stats::default();
        for (num_inserted, num_removed, used_bytes, wasted_bytes) in stats_vec {
            stats.num_shards += 1;
            stats.num_inserted += num_inserted;
            stats.num_removed += num_removed;
            stats.used_bytes += used_bytes;
            stats.wasted_bytes += wasted_bytes;
        }
        stats
    }

    pub fn size_histogram(&self) -> SizeHistogram {
        let mut hist = SizeHistogram::default();
        let hist_vec = self
            .root
            .call_on_all_shards(|sh| Ok(sh.get_size_histogram()))
            .unwrap();

        for v in hist_vec {
            for (i, c) in v.counts_64b.into_iter().enumerate() {
                hist.counts_64b[i] += c;
            }
            for (i, c) in v.counts_1kb.into_iter().enumerate() {
                hist.counts_1kb[i] += c;
            }
            for (i, c) in v.counts_16kb.into_iter().enumerate() {
                hist.counts_16kb[i] += c;
            }
        }
        hist
    }

    /// Returns an iterator over the whole store (skipping lists or typed items)
    pub fn iter(&self) -> CandyStoreIterator {
        CandyStoreIterator::new(self, false)
    }

    pub fn iter_raw(&self) -> CandyStoreIterator {
        CandyStoreIterator::new(self, true)
    }

    /// Returns an iterator starting from the specified cookie (obtained via [CandyStoreIterator::cookie])
    pub fn iter_from_cookie(&self, cookie: u64) -> CandyStoreIterator {
        CandyStoreIterator::from_cookie(self, cookie, false)
    }
}
