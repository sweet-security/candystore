use anyhow::{anyhow, Context};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::BTreeMap,
    ops::{Bound, Range},
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
    shard::{Shard, NUM_ROWS, ROW_WIDTH},
    CandyError,
};
use crate::{Config, Result};

pub(crate) const USER_NAMESPACE: &[u8] = &[1];
pub(crate) const TYPED_NAMESPACE: &[u8] = &[2];
pub(crate) const LIST_NAMESPACE: &[u8] = &[3];
pub(crate) const ITEM_NAMESPACE: &[u8] = &[4];

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
        self.data_bytes() / self.len()
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

/// The CandyStore object. Note that it's fully sync'ed, so can be shared between threads using `Arc`
pub struct CandyStore {
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

/// An iterator over a CandyStore. Note that it's safe to modify (insert/delete) keys while iterating,
/// but the results of the iteration may or may not include these changes. This is considered a
/// well-defined behavior of the store.
pub struct CandyStoreIterator<'a> {
    db: &'a CandyStore,
    shard_idx: u32,
    row_idx: usize,
    entry_idx: usize,
    raw: bool,
}

impl<'a> CandyStoreIterator<'a> {
    fn new(db: &'a CandyStore, raw: bool) -> Self {
        Self {
            db,
            shard_idx: 0,
            row_idx: 0,
            entry_idx: 0,
            raw,
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
    pub fn from_cookie(db: &'a CandyStore, cookie: u64, raw: bool) -> Self {
        Self {
            db,
            shard_idx: ((cookie >> 32) & 0xffff) as u32,
            row_idx: ((cookie >> 16) & 0xffff) as usize,
            entry_idx: (cookie & 0xffff) as usize,
            raw,
        }
    }
}

impl<'a> Iterator for CandyStoreIterator<'a> {
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
                            if self.raw {
                                return Some(Ok((k, v)));
                            }
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

impl CandyStore {
    const END_OF_SHARDS: u32 = 1u32 << 16;

    /// Opens or creates a new CandyStore.
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
            num_entries: Default::default(),
            num_compactions: Default::default(),
            num_splits: Default::default(),
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
                return Err(anyhow!(CandyError::LoadingFailed(format!(
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
                        return Err(anyhow!(CandyError::LoadingFailed(format!(
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

    /// Syncs all in-memory changes of all shards to disk. Concurrent changes are allowed while
    /// flushing, and may result in partially-sync'ed store. Use sparingly, as this is a costly operaton.
    pub fn flush(&self) -> Result<()> {
        let guard = self.shards.read();
        for (_, shard) in guard.iter() {
            shard.flush()?;
        }
        Ok(())
    }

    /// Clears the store (erasing all keys), and removing all shard files
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

    pub(crate) fn make_user_key(&self, mut key: Vec<u8>) -> Vec<u8> {
        key.extend_from_slice(USER_NAMESPACE);
        key
    }

    pub(crate) fn get_by_hash(&self, ph: PartedHash) -> Result<Vec<Result<KVPair>>> {
        debug_assert!(ph.is_valid());
        Ok(self
            .shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for 0x{:04x}", ph.shard_selector()))?
            .1
            .iter_by_hash(ph)
            .collect::<Vec<_>>())
    }

    pub(crate) fn get_raw(&self, full_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);
        self.shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for 0x{:04x}", ph.shard_selector()))?
            .1
            .get(ph, &full_key)
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
            .shards
            .read()
            .lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)))
            .peek_next()
            .with_context(|| format!("missing shard for 0x{:04x}", ph.shard_selector()))?
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
        self.owned_remove(key.as_ref().to_owned())
    }

    /// Same as [Self::remove] but takes an owned key
    pub fn owned_remove(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.remove_raw(&self.make_user_key(key))
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

    /// Returns useful stats about the store
    pub fn stats(&self) -> Stats {
        let guard = self.shards.read();
        let mut stats = Stats {
            num_shards: guard.len(),
            ..Default::default()
        };
        for (_, shard) in guard.iter() {
            stats.num_inserted += shard.header.num_inserted.load(Ordering::Relaxed) as usize;
            stats.num_removed += shard.header.num_removed.load(Ordering::Relaxed) as usize;
            stats.used_bytes = shard.header.write_offset.load(Ordering::Relaxed) as usize;
            stats.wasted_bytes += shard.header.wasted_bytes.load(Ordering::Relaxed) as usize;
        }
        stats
    }

    pub fn size_histogram(&self) -> SizeHistogram {
        let guard = self.shards.read();
        let mut hist = SizeHistogram::default();
        for (_, shard) in guard.iter() {
            for (i, h) in shard.header.size_histogram.counts_64b.iter().enumerate() {
                hist.counts_64b[i] += h.load(Ordering::Relaxed) as usize;
            }
            for (i, h) in shard.header.size_histogram.counts_1kb.iter().enumerate() {
                hist.counts_1kb[i] += h.load(Ordering::Relaxed) as usize;
            }
            for (i, h) in shard.header.size_histogram.counts_16kb.iter().enumerate() {
                hist.counts_16kb[i] += h.load(Ordering::Relaxed) as usize;
            }
        }
        hist
    }

    /// Returns an iterator over the whole store (skipping linked lists or typed items)
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
