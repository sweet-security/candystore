use anyhow::{anyhow, Context};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::BTreeMap,
    ops::Bound,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    hashing::PartedHash,
    shard::{KVPair, HEADER_SIZE},
};
use crate::{
    shard::{Shard, ShardRow, NUM_ROWS, ROW_WIDTH},
    VickyError,
};
use crate::{Config, Result};

pub(crate) const USER_NAMESPACE: &[u8] = &[1];
pub(crate) const TYPED_NAMESPACE: &[u8] = &[2];
pub(crate) const LIST_NAMESPACE: &[u8] = &[3];
pub(crate) const ITEM_NAMESPACE: &[u8] = &[4];

/// Stats from VickyStore
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Stats {
    pub num_shards: usize,
    pub num_inserted: usize,
    pub num_deleted: usize,
    pub total_bytes: usize,
    pub wasted_bytes: usize,
}

impl Stats {
    pub fn len(&self) -> usize {
        self.num_inserted - self.num_deleted
    }
}

/// The VickyStore object. Note that it's fully sync'ed, so can be shared between threads using `Arc`
pub struct VickyStore {
    pub(crate) shards: RwLock<BTreeMap<u32, Shard>>,
    pub(crate) config: Arc<Config>,
    pub(crate) dir_path: PathBuf,
    // stats
    pub(crate) num_entries: AtomicUsize,
    pub(crate) num_compactions: AtomicUsize,
    pub(crate) num_splits: AtomicUsize,
    // locks for complicated operations
    pub(crate) keyed_locks_mask: u32,
    pub(crate) keyed_locks: Vec<Mutex<()>>,
}

/// An iterator over a VickyStore. Note that it's safe to modify (insert/delete) keys while iterating,
/// but the results of the iteration may or may not include these changes. This is considered a
/// well-defined behavior of the store.
pub struct VickyStoreIterator<'a> {
    db: &'a VickyStore,
    shard_idx: u32,
    row_idx: usize,
    entry_idx: usize,
}

impl<'a> VickyStoreIterator<'a> {
    fn new(db: &'a VickyStore) -> Self {
        Self {
            db,
            shard_idx: 0,
            row_idx: 0,
            entry_idx: 0,
        }
    }

    /// Returns the cookie of the next item in the store. This can be used later to construct an iterator
    /// that starts at the given point.
    pub fn cookie(&self) -> u64 {
        ((self.shard_idx as u64 & 0xffff) << 32)
            | ((self.row_idx as u64 & 0xffff) << 16)
            | (self.entry_idx as u64 & 0xffff)
    }

    // Constructs an iterator starting at the given cookie
    pub fn from_cookie(db: &'a VickyStore, cookie: u64) -> Self {
        Self {
            db,
            shard_idx: ((cookie >> 32) & 0xffff) as u32,
            row_idx: ((cookie >> 16) & 0xffff) as usize,
            entry_idx: (cookie & 0xffff) as usize,
        }
    }
}

impl<'a> Iterator for VickyStoreIterator<'a> {
    type Item = Result<KVPair>;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = self.db.shards.read();
        for (curr_shard_idx, shard) in guard.range(self.shard_idx..) {
            self.shard_idx = *curr_shard_idx;
            loop {
                let kvres = shard.read_at(self.row_idx, self.entry_idx);

                // advance
                let mut should_break = false;
                self.entry_idx += 1;
                if self.entry_idx >= ROW_WIDTH {
                    self.entry_idx = 0;
                    self.row_idx += 1;
                    if self.row_idx >= NUM_ROWS {
                        self.row_idx = 0;
                        self.shard_idx += 1;
                        should_break = true;
                    }
                }

                if let Some(res) = kvres {
                    match res {
                        Ok((mut k, v)) => {
                            // filter anything other than USER_NAMESPACE
                            if k.ends_with(USER_NAMESPACE) {
                                k.truncate(k.len() - USER_NAMESPACE.len());
                                return Some(Ok((k, v)));
                            }
                        }
                        Err(e) => {
                            return Some(Err(e));
                        }
                    }
                }
                if should_break {
                    break;
                }
            }
        }
        None
    }
}

impl VickyStore {
    const END_OF_SHARDS: u32 = 1u32 << 16;

    /// Opens or creates a new VickyStore.
    /// * dir_path - the directory where shards will be kept
    /// * config - the configuration options for the store
    pub fn open(dir_path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let mut shards: BTreeMap<u32, Shard> = BTreeMap::new();
        let config = Arc::new(config);
        let dir_path: PathBuf = dir_path.as_ref().into();

        std::fs::create_dir_all(&dir_path)?;
        Self::load_existing_dir(&dir_path, &config, &mut shards)?;
        if shards.is_empty() {
            Self::create_first_shards(&dir_path, &config, &mut shards)?;
        }

        let mut num_keyed_locks = config.max_concurrent_list_ops.max(4);
        if !num_keyed_locks.is_power_of_two() {
            num_keyed_locks = 1 << (num_keyed_locks.ilog2() + 1);
        }

        let mut keyed_locks = vec![];
        for _ in 0..num_keyed_locks {
            keyed_locks.push(Mutex::new(()));
        }

        Ok(Self {
            config,
            dir_path,
            shards: RwLock::new(shards),
            num_entries: 0.into(),
            num_compactions: 0.into(),
            num_splits: 0.into(),
            keyed_locks_mask: num_keyed_locks - 1,
            keyed_locks,
        })
    }

    fn load_existing_dir(
        dir_path: &PathBuf,
        config: &Arc<Config>,
        shards: &mut BTreeMap<u32, Shard>,
    ) -> Result<()> {
        for res in std::fs::read_dir(&dir_path)? {
            let entry = res?;
            let filename = entry.file_name();
            let Some(filename) = filename.to_str() else {
                continue;
            };
            let Ok(filetype) = entry.file_type() else {
                continue;
            };
            if !filetype.is_file() {
                continue;
            }
            if filename.starts_with("compact_")
                || filename.starts_with("bottom_")
                || filename.starts_with("top_")
            {
                std::fs::remove_file(entry.path())?;
                continue;
            } else if !filename.starts_with("shard_") {
                continue;
            }
            let Some((_, span)) = filename.split_once("_") else {
                continue;
            };
            let Some((start, end)) = span.split_once("-") else {
                continue;
            };
            let start = u32::from_str_radix(start, 16).expect(filename);
            let end = u32::from_str_radix(end, 16).expect(filename);

            if start > end || end > Self::END_OF_SHARDS {
                return Err(anyhow!(VickyError::LoadingFailed(format!(
                    "Bad span for {filename}"
                ))));
            }

            if let Some(existing) = shards.get(&end) {
                // this means we hit an uncompleted split - we need to take the wider of the two shards
                // and delete the narrower one
                if existing.span.start < start {
                    // keep existing, remove this one
                    std::fs::remove_file(entry.path())?;
                    continue;
                } else {
                    // remove existing one
                    std::fs::remove_file(dir_path.join(format!(
                        "shard_{:04x}-{:04x}",
                        existing.span.start, existing.span.end
                    )))?;
                }
            }
            shards.insert(
                end,
                Shard::open(entry.path(), start..end, false, config.clone())?,
            );
        }

        // remove any split midpoints. we may come across one of [start..mid), [mid..end), [start..end)
        // the case of [mid..end) and [start..end) (same end) is already handled above. so we need to
        // detect two shards where first.start == second.start and remove the shorter one
        let mut spans = vec![];
        for (end, shard) in shards.range(0..) {
            spans.push((shard.span.start, *end));
        }
        let mut to_remove = vec![];
        for (i, (start, end)) in spans.iter().enumerate() {
            if i < spans.len() - 1 {
                let (next_start, next_end) = spans[i + 1];
                if *start == next_start {
                    if next_end <= *end {
                        return Err(anyhow!(VickyError::LoadingFailed(format!(
                            "Removing in-progress split with start={} end={} next_start={} next_end={}",
                            *start, *end, next_start, next_end
                        ))));
                    }

                    to_remove.push((*start, *end))
                }
            }
        }
        for (start, end) in to_remove {
            let bottomfile = dir_path.join(format!("shard_{start:04x}-{end:04x}"));
            std::fs::remove_file(bottomfile)?;
            shards.remove(&end);
        }

        Ok(())
    }

    fn create_first_shards(
        dir_path: &PathBuf,
        config: &Arc<Config>,
        shards: &mut BTreeMap<u32, Shard>,
    ) -> Result<()> {
        let step = (Self::END_OF_SHARDS as f64)
            / (config.expected_number_of_keys as f64 / Shard::EXPECTED_CAPACITY as f64).max(1.0);
        let step = 1 << (step as u32).ilog2();

        let mut start = 0;
        while start < Self::END_OF_SHARDS {
            let end = start + step;
            let shard = Shard::open(
                dir_path.join(format!("shard_{:04x}-{:04x}", start, end)),
                0..Self::END_OF_SHARDS,
                false,
                config.clone(),
            )?;
            shards.insert(end, shard);
            start = end;
        }

        Ok(())
    }

    /// Attempts for sync all in-memory changes of all shards to disk. Concurrent changes are allowed while
    /// flushing, and may result in partially-sync'ed store. Use sparingly, as this is a costly operaton.
    pub fn flush(&self) -> Result<()> {
        let guard = self.shards.read();
        for (_, shard) in guard.iter() {
            shard.flush()?;
        }
        Ok(())
    }

    /// Clears the store (erasing all keys)
    pub fn clear(&self) -> Result<()> {
        let mut guard = self.shards.write();

        for res in std::fs::read_dir(&self.dir_path)? {
            let entry = res?;
            let filename = entry.file_name();
            let Some(filename) = filename.to_str() else {
                continue;
            };
            let Ok(filetype) = entry.file_type() else {
                continue;
            };
            if !filetype.is_file() {
                continue;
            }
            if filename.starts_with("shard_")
                || filename.starts_with("compact_")
                || filename.starts_with("bottom_")
                || filename.starts_with("top_")
            {
                std::fs::remove_file(entry.path())?;
            }
        }

        self.num_entries.store(0, Ordering::Relaxed);
        self.num_compactions.store(0, Ordering::Relaxed);
        self.num_splits.store(0, Ordering::Relaxed);

        guard.clear();
        Self::create_first_shards(&self.dir_path, &self.config, &mut guard)?;

        Ok(())
    }

    pub(crate) fn make_user_key(&self, key: &[u8]) -> Vec<u8> {
        let mut full_key = key.to_owned();
        full_key.extend_from_slice(USER_NAMESPACE);
        full_key
    }

    pub(crate) fn get_by_hash(&self, ph: PartedHash) -> Result<Vec<Result<KVPair>>> {
        debug_assert_ne!(ph, PartedHash::INVALID);
        Ok(self
            .shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for {}", ph.shard_selector()))?
            .1
            .iter_by_hash(ph)
            .collect::<Vec<_>>())
    }

    pub(crate) fn operate_on_key_mut<T>(
        &self,
        key: &[u8],
        func: impl FnOnce(
            &Shard,
            &mut ShardRow,
            PartedHash,
            Option<(usize, Vec<u8>, Vec<u8>)>,
        ) -> Result<T>,
    ) -> Result<T> {
        let ph = PartedHash::new(&self.config.hash_seed, key);
        self.shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for {}", ph.shard_selector()))?
            .1
            .operate_on_key_mut(ph, key, func)
    }

    pub(crate) fn get_raw(&self, full_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);
        self.shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for {}", ph.shard_selector()))?
            .1
            .get(ph, &full_key)
    }

    /// Gets the value of a key from the store. If the key does not exist, `None` will be returned.
    /// The data is fully-owned, no references are returned.
    pub fn get<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        self.get_raw(&self.make_user_key(key.as_ref()))
    }

    /// Checks whether the given key exists in the store
    pub fn contains<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<bool> {
        Ok(self.get_raw(&self.make_user_key(key.as_ref()))?.is_some())
    }

    pub(crate) fn remove_raw(&self, full_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);

        let val = self
            .shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for {}", ph.shard_selector()))?
            .1
            .remove(ph, &full_key)?;
        if val.is_some() {
            self.num_entries.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(val)
    }

    /// Removes a key-value pair from the store, returning `None` if the key did not exist,
    /// or `Some(old_value)` if it did
    pub fn remove<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        self.remove_raw(&self.make_user_key(key.as_ref()))
    }

    /// Returns some stats, useful for debugging. Note that stats are local to the VickyStore instance and
    /// are not persisted, so closing and opening the store will reset the stats.
    pub fn _num_entries(&self) -> usize {
        self.num_entries.load(Ordering::Acquire)
    }
    pub fn _num_compactions(&self) -> usize {
        self.num_compactions.load(Ordering::Acquire)
    }
    pub fn _num_splits(&self) -> usize {
        self.num_splits.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> Stats {
        let guard = self.shards.read();
        let mut stats = Stats {
            num_shards: guard.len(),
            ..Default::default()
        };
        for (_, shard) in guard.iter() {
            stats.num_inserted += shard.header.num_inserted.load(Ordering::Relaxed) as usize;
            stats.num_deleted += shard.header.num_deleted.load(Ordering::Relaxed) as usize;
            stats.wasted_bytes += shard.header.wasted_bytes.load(Ordering::Relaxed) as usize;
            stats.total_bytes +=
                shard.header.write_offset.load(Ordering::Relaxed) as usize + HEADER_SIZE as usize;
        }
        stats
    }

    /// Returns an iterator over the whole store
    pub fn iter(&self) -> VickyStoreIterator {
        VickyStoreIterator::new(self)
    }

    /// Returns an iterator starting from the specified cookie (obtained from `get_cookie` of a
    /// previously created `VickyStoreIterator`
    pub fn iter_from_cookie(&self, cookie: u64) -> VickyStoreIterator {
        VickyStoreIterator::from_cookie(self, cookie)
    }
}
