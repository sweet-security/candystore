use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use crate::{hashing::PartedHash, shard::InsertStatus};
use crate::{
    hashing::USER_NAMESPACE,
    shard::{Config, Shard, NUM_ROWS, ROW_WIDTH},
    Error,
};
use crate::{shard::EntryRef, Result};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Stats {
    pub num_entries: usize,
    pub num_splits: usize,
    pub num_compactions: usize,
}

pub struct VickyStore {
    shards: RwLock<BTreeMap<u32, Shard>>,
    config: Arc<Config>,
    // stats
    num_entries: AtomicUsize,
    num_compactions: AtomicUsize,
    num_splits: AtomicUsize,
}

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

    pub fn cookie(&self) -> u64 {
        ((self.shard_idx as u64 & 0xffff) << 32)
            | ((self.row_idx as u64 & 0xffff) << 16)
            | (self.entry_idx as u64 & 0xffff)
    }

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

    pub fn open(config: Config) -> Result<Self> {
        let mut shards: BTreeMap<u32, Shard> = BTreeMap::new();
        let config = Arc::new(config);

        std::fs::create_dir_all(&config.dir_path)?;
        Self::load_existing_dir(config.clone(), &mut shards)?;

        if shards.is_empty() {
            Self::create_first_shard(&config, &mut shards)?
        }

        Ok(Self {
            config,
            shards: RwLock::new(shards),
            num_entries: 0.into(),
            num_compactions: 0.into(),
            num_splits: 0.into(),
        })
    }

    fn load_existing_dir(config: Arc<Config>, shards: &mut BTreeMap<u32, Shard>) -> Result<()> {
        for res in std::fs::read_dir(&config.dir_path)? {
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
                    std::fs::remove_file(config.dir_path.join(format!(
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
            let bottomfile = config.dir_path.join(format!("shard_{start:04x}-{end:04x}"));
            std::fs::remove_file(bottomfile)?;
            shards.remove(&end);
        }

        Ok(())
    }

    fn create_first_shard(config: &Arc<Config>, shards: &mut BTreeMap<u32, Shard>) -> Result<()> {
        shards.insert(
            Self::END_OF_SHARDS,
            Shard::open(
                config
                    .dir_path
                    .join(format!("shard_{:04x}-{:04x}", 0, Self::END_OF_SHARDS)),
                0..Self::END_OF_SHARDS,
                false,
                config.clone(),
            )?,
        );

        Ok(())
    }

    pub fn get<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key);
        self.shards
            .read()
            .unwrap()
            .lower_bound(Bound::Excluded(&(ph.shard_selector as u32)))
            .peek_next()
            .unwrap()
            .1
            .get(ph, key)
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
        let orig_filename = self.config.dir_path.join(format!(
            "shard_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let tmpfile = self.config.dir_path.join(format!(
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
            let entry = res?;
            let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, &entry.key);

            let status = compacted_shard.insert(ph, &EntryRef::from_entry(&entry))?;
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
            .config
            .dir_path
            .join(format!("bottom_{:04x}-{:04x}", shard_start, midpoint));
        let topfile = self
            .config
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
            let entry = res?;

            let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, &entry.key);
            let status = if (ph.shard_selector as u32) < midpoint {
                bottom_shard.insert(ph, &EntryRef::from_entry(&entry))?
            } else {
                top_shard.insert(ph, &EntryRef::from_entry(&entry))?
            };
            assert!(matches!(status, InsertStatus::Added), "{status:?}");
        }

        self.num_splits.fetch_add(1, Ordering::SeqCst);

        // this is not atomic, so when loading, we need to take the larger span if it exists and
        // delete the partial ones
        std::fs::rename(
            bottomfile,
            self.config
                .dir_path
                .join(format!("shard_{:04x}-{:04x}", shard_start, midpoint)),
        )?;
        std::fs::rename(
            topfile,
            self.config
                .dir_path
                .join(format!("shard_{:04x}-{:04x}", midpoint, shard_end)),
        )?;
        std::fs::remove_file(
            self.config
                .dir_path
                .join(format!("shard_{:04x}-{:04x}", shard_start, shard_end)),
        )
        .unwrap();

        guard.insert(midpoint, bottom_shard);
        guard.insert(shard_end, top_shard);

        Ok(true)
    }

    fn try_insert(&self, ph: PartedHash, entry: &EntryRef) -> Result<(InsertStatus, u32, u32)> {
        let guard = self.shards.read().unwrap();
        let cursor = guard.lower_bound(Bound::Excluded(&(ph.shard_selector as u32)));
        let shard_start = cursor
            .peek_prev()
            .map(|(&shard_start, _)| shard_start)
            .unwrap_or(0);
        let (shard_end, shard) = cursor.peek_next().unwrap();
        let status = shard.insert(ph, entry)?;

        Ok((status, shard_start, *shard_end))
    }

    fn _insert(&self, ph: PartedHash, entry: EntryRef) -> Result<()> {
        if entry.key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLong);
        }
        if entry.val.len() > u16::MAX as usize {
            return Err(Error::ValueTooLong);
        }

        loop {
            let (status, shard_start, shard_end) = self.try_insert(ph, &entry)?;

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

    pub fn insert<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<()> {
        let entry = EntryRef {
            key: key.as_ref(),
            val: val.as_ref(),
        };
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, entry.key);
        self._insert(ph, entry)
    }

    // Modify an existing entry in-place, instead of creating a version. Note that the key must exist
    // and `patch.len() + patch_offset` must be less than or equal to the current value's length.
    // This method is guaranteed to never trigger a split or a compaction
    //
    // This is not crash-safe as it overwrite existing data, and thus may produce inconsistent results
    // on crashes (part old data, part new data)
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

    pub fn remove<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key);
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

    pub fn stats(&self) -> Stats {
        Stats {
            num_entries: self.num_entries.load(Ordering::Acquire),
            num_compactions: self.num_compactions.load(Ordering::Relaxed),
            num_splits: self.num_splits.load(Ordering::Relaxed),
        }
    }

    pub fn iter(&self) -> VickyStoreIterator {
        VickyStoreIterator::new(self)
    }

    pub fn iter_from_cookie(&self, cookie: u64) -> VickyStoreIterator {
        VickyStoreIterator::from_cookie(self, cookie)
    }

    pub fn clear(&self) -> Result<()> {
        let mut guard = self.shards.write().unwrap();

        for res in std::fs::read_dir(&self.config.dir_path)? {
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
        Self::create_first_shard(&self.config, &mut guard)?;

        Ok(())
    }
}
