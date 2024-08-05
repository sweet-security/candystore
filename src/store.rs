use std::{
    collections::BTreeMap,
    ops::Bound,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use crate::{hashing::PartedHash, shard::InsertStatus, VickyError};
use crate::{
    hashing::USER_NAMESPACE,
    shard::{Shard, NUM_ROWS, ROW_WIDTH},
};
use crate::{Config, Result};

/// Stats from VickyStore, mainly useful for debugging
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Stats {
    pub num_entries: usize,
    pub num_splits: usize,
    pub num_compactions: usize,
}

/// The VickyStore object. Note that it's fully sync'ed, so can be shared between threads using `Arc`
pub struct VickyStore {
    shards: RwLock<BTreeMap<u32, Shard>>,
    pub(crate) config: Arc<Config>,
    dir_path: PathBuf,
    // stats
    num_entries: AtomicUsize,
    num_compactions: AtomicUsize,
    num_splits: AtomicUsize,
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
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = self.db.shards.read().unwrap();
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

                if kvres.is_some() {
                    return kvres;
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

        Ok(Self {
            config,
            dir_path,
            shards: RwLock::new(shards),
            num_entries: 0.into(),
            num_compactions: 0.into(),
            num_splits: 0.into(),
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
            assert!(start <= end && end <= Self::END_OF_SHARDS, "{start}..{end}");

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
                    assert!(next_end > *end);
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
        let shards_needed = (config.expected_number_of_keys / Shard::EXPECTED_CAPACITY).max(1);
        let step = Self::END_OF_SHARDS / 2u32.pow(shards_needed.ilog2());

        let mut start = 0;
        while start < Self::END_OF_SHARDS {
            let end = start + step;
            shards.insert(
                Self::END_OF_SHARDS,
                Shard::open(
                    dir_path.join(format!("shard_{:04x}-{:04x}", start, end)),
                    0..Self::END_OF_SHARDS,
                    false,
                    config.clone(),
                )?,
            );
            start = end;
        }

        Ok(())
    }

    pub(crate) fn get_internal<B: AsRef<[u8]> + ?Sized>(
        &self,
        ph: PartedHash,
        key: &B,
    ) -> Result<Option<Vec<u8>>> {
        self.shards
            .read()
            .unwrap()
            .lower_bound(Bound::Excluded(&(ph.shard_selector as u32)))
            .peek_next()
            .unwrap()
            .1
            .get(ph, key.as_ref())
    }

    /// Gets the value of a key from the store. If the key does not exist, `None` will be returned.
    /// The data is fully-owned, no references are returned.
    pub fn get<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key.as_ref());
        self.get_internal(ph, key.as_ref())
    }

    /// Checks whether the given key exists in the store
    pub fn contains<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<bool> {
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key.as_ref());
        Ok(self.get_internal(ph, key.as_ref())?.is_some())
    }

    fn compact(&self, shard_end: u32, write_offset: u32) -> Result<bool> {
        let mut guard = self.shards.write().unwrap();
        // it's possible that another thread already compacted this shard
        if guard
            .get(&shard_end)
            .unwrap()
            .header
            .write_offset
            .load(Ordering::Relaxed)
            < write_offset
        {
            return Ok(false);
        }

        let removed_shard = guard.remove(&shard_end).unwrap();
        let orig_filename = self.dir_path.join(format!(
            "shard_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let tmpfile = self.dir_path.join(format!(
            "compact_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let compacted_shard = Shard::open(
            tmpfile.clone(),
            removed_shard.span.clone(),
            true,
            self.config.clone(),
        )?;

        self.num_compactions.fetch_add(1, Ordering::SeqCst);

        for res in removed_shard.unlocked_iter() {
            let (k, v) = res?;
            // XXX: this will not work with namespaces
            let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, &k);

            let status = compacted_shard.insert(ph, &k, &v)?;
            assert!(matches!(status, InsertStatus::Added), "{status:?}");
        }

        std::fs::rename(tmpfile, &orig_filename)?;
        guard.insert(shard_end, compacted_shard);
        Ok(true)
    }

    fn split(&self, shard_start: u32, shard_end: u32) -> Result<bool> {
        let mut guard = self.shards.write().unwrap();
        // it's possible that another thread already split this range - check if midpoint exists, and if so, bail out
        let midpoint = shard_start / 2 + shard_end / 2;
        if guard.contains_key(&midpoint) {
            return Ok(false);
        }

        let removed_shard = guard.remove(&shard_end).unwrap();

        let bottomfile = self
            .dir_path
            .join(format!("bottom_{:04x}-{:04x}", shard_start, midpoint));
        let topfile = self
            .dir_path
            .join(format!("top_{:04x}-{:04x}", midpoint, shard_end));

        let bottom_shard = Shard::open(
            bottomfile.clone(),
            shard_start..midpoint,
            true,
            self.config.clone(),
        )?;
        let top_shard = Shard::open(
            topfile.clone(),
            midpoint..shard_end,
            true,
            self.config.clone(),
        )?;

        for res in removed_shard.unlocked_iter() {
            let (k, v) = res?;

            let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, &k);
            let status = if (ph.shard_selector as u32) < midpoint {
                bottom_shard.insert(ph, &k, &v)?
            } else {
                top_shard.insert(ph, &k, &v)?
            };
            assert!(matches!(status, InsertStatus::Added), "{status:?}");
        }

        self.num_splits.fetch_add(1, Ordering::SeqCst);

        // this is not atomic, so when loading, we need to take the larger span if it exists and
        // delete the partial ones
        std::fs::rename(
            bottomfile,
            self.dir_path
                .join(format!("shard_{:04x}-{:04x}", shard_start, midpoint)),
        )?;
        std::fs::rename(
            topfile,
            self.dir_path
                .join(format!("shard_{:04x}-{:04x}", midpoint, shard_end)),
        )?;
        std::fs::remove_file(
            self.dir_path
                .join(format!("shard_{:04x}-{:04x}", shard_start, shard_end)),
        )
        .unwrap();

        guard.insert(midpoint, bottom_shard);
        guard.insert(shard_end, top_shard);

        Ok(true)
    }

    fn try_insert(
        &self,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
    ) -> Result<(InsertStatus, u32, u32)> {
        let guard = self.shards.read().unwrap();
        let cursor = guard.lower_bound(Bound::Excluded(&(ph.shard_selector as u32)));
        let shard_start = cursor
            .peek_prev()
            .map(|(&shard_start, _)| shard_start)
            .unwrap_or(0);
        let (shard_end, shard) = cursor.peek_next().unwrap();
        let status = shard.insert(ph, key, val)?;

        Ok((status, shard_start, *shard_end))
    }

    pub(crate) fn insert_internal(&self, ph: PartedHash, key: &[u8], val: &[u8]) -> Result<()> {
        if key.len() > u16::MAX as usize {
            return Err(Box::new(VickyError::KeyTooLong));
        }
        if val.len() > u16::MAX as usize {
            return Err(Box::new(VickyError::ValueTooLong));
        }

        loop {
            let (status, shard_start, shard_end) = self.try_insert(ph, key, val)?;

            match status {
                InsertStatus::Added => {
                    self.num_entries.fetch_add(1, Ordering::SeqCst);
                    return Ok(());
                }
                InsertStatus::Replaced => {
                    return Ok(());
                }
                InsertStatus::CompactionNeeded(write_offset) => {
                    self.compact(shard_end, write_offset)?;
                    // retry
                }
                InsertStatus::SplitNeeded => {
                    self.split(shard_start, shard_end)?;
                    // retry
                }
            }
        }
    }

    /// Attempts for sync all in-memory changes of all shards to disk. Concurrent changes are allowed while
    /// flushing, and may result in partially-sync'ed store. Use sparingly, as this is a costly operaton.
    pub fn flush(&self) -> Result<()> {
        let guard = self.shards.read().unwrap();
        for (_, shard) in guard.iter() {
            shard.flush()?;
        }
        Ok(())
    }

    /// Inserts a key-value pair, creating it or replacing an existing pair. Note that if the program crashed
    /// while or "right after" this operation, or if the operating system is unable to flush the page cache,
    /// you may lose some data. However, you will still be in a consistent state, where you will get a previous
    /// version of the state.
    ///
    /// While this method is O(1) amortized, every so often it will trigger either a shard compaction or a
    /// shard split, which requires rewriting the whole shard.
    pub fn insert<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<()> {
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key.as_ref());
        self.insert_internal(ph, key.as_ref(), val.as_ref())
    }

    /// Modifies an existing entry in-place, instead of creating a version. Note that the key must exist
    /// and `patch.len() + patch_offset` must be less than or equal to the current value's length.
    ///
    /// This is operation is NOT crash-safe as it overwrites existing data, and thus may produce inconsistent
    /// results on crashes (reading part old data, part new data).
    ///
    /// This method will never trigger a shard split or a shard compaction.
    pub fn modify_inplace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        patch: &B2,
        patch_offset: usize,
    ) -> Result<()> {
        let key = key.as_ref();
        let patch = patch.as_ref();
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key);
        self.shards
            .read()
            .unwrap()
            .lower_bound(Bound::Excluded(&(ph.shard_selector as u32)))
            .peek_next()
            .unwrap()
            .1
            .modify_inplace(ph, key, patch, patch_offset)
    }

    pub(crate) fn remove_internal(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let val = self
            .shards
            .read()
            .unwrap()
            .lower_bound(Bound::Excluded(&(ph.shard_selector as u32)))
            .peek_next()
            .unwrap()
            .1
            .remove(ph, key)?;
        if val.is_some() {
            self.num_entries.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(val)
    }

    /// Removes a key-value pair from the store, returning `None` if the key did not exist,
    /// or `Some(old_value)` if it did
    pub fn remove<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key);
        self.remove_internal(ph, key)
    }

    /// Returns some stats, useful for debugging. Note that stats are local to the VickyStore instance and
    /// are not persisted, so closing and opening the store will reset the stats.
    pub fn stats(&self) -> Stats {
        Stats {
            num_entries: self.num_entries.load(Ordering::Acquire),
            num_compactions: self.num_compactions.load(Ordering::Relaxed),
            num_splits: self.num_splits.load(Ordering::Relaxed),
        }
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

    /// Clears the store (erasing all keys)
    pub fn clear(&self) -> Result<()> {
        let mut guard = self.shards.write().unwrap();

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
}
