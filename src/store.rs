use anyhow::{anyhow, bail};
use fslock::LockFile;
use parking_lot::Mutex;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    hashing::{HashSeed, PartedHash},
    router::ShardRouter,
    shard::{CompactionThreadPool, InsertMode, InsertStatus, KVPair},
    Stats,
};
use crate::{
    shard::{NUM_ROWS, ROW_WIDTH},
    stats::InternalStats,
};

use crate::{CandyError, Config, Result, MAX_TOTAL_KEY_SIZE, MAX_VALUE_SIZE};

pub(crate) const USER_NAMESPACE: &[u8] = &[1];
pub(crate) const TYPED_NAMESPACE: &[u8] = &[2];
pub(crate) const LIST_NAMESPACE: &[u8] = &[3];
pub(crate) const ITEM_NAMESPACE: &[u8] = &[4];
pub(crate) const CHAIN_NAMESPACE: u8 = 5;

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
    pub num_compaction_threads: usize,
    #[cfg(feature = "flush_aggregation")]
    pub flush_aggregation_delay: Option<std::time::Duration>,
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
/// The CandyStore object. Note that it's fully sync'ed, so can be shared between threads using `Arc`
pub struct CandyStore {
    pub(crate) root: ShardRouter,
    pub(crate) config: Arc<InternalConfig>,
    // locks for complicated operations
    pub(crate) keyed_locks_mask: u32,
    pub(crate) keyed_locks: Vec<Mutex<()>>,
    _lockfile: LockFile,
    stats: Arc<InternalStats>,
    //threadpool: Arc<CompactionThreadPool>,
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
            num_compaction_threads: config.num_compaction_threads,
            #[cfg(feature = "flush_aggregation")]
            flush_aggregation_delay: config.flush_aggregation_delay,
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

        let stats = Arc::new(InternalStats::default());
        let threadpool = Arc::new(CompactionThreadPool::new(config.num_compaction_threads));
        let root = ShardRouter::new(config.clone(), stats.clone(), threadpool.clone())?;

        Ok(Self {
            config,
            root,
            keyed_locks_mask: num_keyed_locks - 1,
            keyed_locks,
            _lockfile: lockfile,
            stats,
            //threadpool,
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
        self.stats.clear();

        Ok(())
    }

    pub(crate) fn make_user_key(&self, mut key: Vec<u8>) -> Vec<u8> {
        key.extend_from_slice(USER_NAMESPACE);
        key
    }

    pub(crate) fn get_by_hash(&self, ph: PartedHash) -> Result<Vec<KVPair>> {
        debug_assert!(ph.is_valid());
        self.root
            .shared_op(ph.shard_selector(), |sh| sh.get_by_hash(ph))
    }

    pub(crate) fn get_raw(&self, full_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);
        self.root
            .shared_op(ph.shard_selector(), |sh| sh.get(ph, &full_key))
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
        self.root
            .shared_op(ph.shard_selector(), |sh| sh.remove(ph, &full_key))
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

        self.root.insert(ph, full_key, val, mode)
    }

    pub(crate) fn set_raw(&self, full_key: &[u8], val: &[u8]) -> Result<SetStatus> {
        match self.insert_internal(full_key, val, InsertMode::Set)? {
            InsertStatus::Added => Ok(SetStatus::CreatedNew),
            InsertStatus::Replaced(v) => Ok(SetStatus::PrevValue(v)),
            InsertStatus::AlreadyExists(v) => Ok(SetStatus::PrevValue(v)),
            InsertStatus::KeyDoesNotExist => unreachable!(),
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

    /// Returns useful stats about the store
    pub fn stats(&self) -> Stats {
        let shard_stats = self.root.call_on_all_shards(|sh| sh.get_stats()).unwrap();

        let mut stats = Stats::default();
        self.stats.fill_stats(&mut stats);

        for stats2 in shard_stats {
            stats.num_shards += 1;
            stats.occupied_bytes += stats2.write_offset;
            stats.wasted_bytes += stats2.wasted_bytes;
            stats.num_inserts += stats2.num_inserts;
            stats.num_removals += stats2.num_removals;
        }
        stats
    }

    /// Merges small shards (shards with a used capacity of less than `max_fill_level`), `max_fill_level` should
    /// be a number between 0 and 0.5, the reasonable choice is 0.25.
    ///
    /// Note 1: this is an expensive operation that takes a global lock on the store (no other operations can
    /// take place while merging is in progress). Only use it if you expect the number of items to be at half or
    /// less than what it was (i.e., after a peak period)
    ///
    /// Note 2: merging will stop once we reach the number of shards required for [Config::expected_number_of_keys],
    /// if configured
    ///
    /// Returns true if any shards were merged, false otherwise
    pub fn merge_small_shards(&self, max_fill_level: f32) -> Result<bool> {
        self.root.merge_small_shards(max_fill_level)
    }
}

// impl Drop for CandyStore {
//     fn drop(&mut self) {
//         _ = self.threadpool.terminate();
//     }
// }
