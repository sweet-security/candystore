mod compaction;
mod list;
mod open;
mod queue;
mod recovery;
mod typed;

use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use siphasher::sip::SipHasher13;

use std::{
    collections::HashMap,
    hash::Hasher,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    data_file::DataFile,
    index_file::{EntryPointer, IndexFile, RowLayout, RowReadGuard, RowWriteGuard},
    internal::{
        HashCoord, KeyNamespace, MAX_DATA_FILE_IDX, MAX_DATA_FILES, MIN_SPLIT_LEVEL, ROW_WIDTH,
        aligned_data_entry_size, aligned_data_entry_waste, aligned_tombstone_entry_waste,
        index_file_path, index_rows_file_path, sync_dir,
    },
    types::{
        Config, Error, GetOrCreateStatus, INITIAL_DATA_FILE_ORDINAL, ReplaceStatus, Result, Stats,
    },
};

#[derive(Default)]
struct CompactionState {
    wake_requested: bool,
}

#[derive(Default)]
struct InnerStats {
    num_compactions: AtomicU64,
    compaction_time_ms: AtomicU64,
    compaction_errors: AtomicU64,
    num_positive_lookups: AtomicU64,
    num_negative_lookups: AtomicU64,
    num_collisions: AtomicU64,
    last_remap_dur_ms: AtomicU64,
    last_compaction_dur_ms: AtomicU64,
    last_compaction_reclaimed_bytes: AtomicU32,
    last_compaction_moved_bytes: AtomicU32,
    num_read_ops: AtomicU64,
    num_read_bytes: AtomicU64,
    num_write_ops: AtomicU64,
    num_write_bytes: AtomicU64,
}

impl InnerStats {
    fn reset(&self) {
        self.num_compactions.store(0, Ordering::Relaxed);
        self.compaction_time_ms.store(0, Ordering::Relaxed);
        self.compaction_errors.store(0, Ordering::Relaxed);
        self.num_positive_lookups.store(0, Ordering::Relaxed);
        self.num_negative_lookups.store(0, Ordering::Relaxed);
        self.num_collisions.store(0, Ordering::Relaxed);
        self.last_remap_dur_ms.store(0, Ordering::Relaxed);
        self.last_compaction_dur_ms.store(0, Ordering::Relaxed);
        self.last_compaction_reclaimed_bytes
            .store(0, Ordering::Relaxed);
        self.last_compaction_moved_bytes.store(0, Ordering::Relaxed);
        self.num_read_ops.store(0, Ordering::Relaxed);
        self.num_read_bytes.store(0, Ordering::Relaxed);
        self.num_write_ops.store(0, Ordering::Relaxed);
        self.num_write_bytes.store(0, Ordering::Relaxed);
    }
}

struct StoreInner {
    base_path: PathBuf,
    config: Arc<Config>,
    index_file: IndexFile,
    logical_locks: Vec<RwLock<()>>,
    logical_locks_mask: usize,
    data_files: RwLock<HashMap<u16, Arc<DataFile>>>,
    active_file_idx: AtomicU16,
    active_file_ordinal: AtomicU64,
    rotation_lock: Mutex<()>,
    compaction_state: Mutex<CompactionState>,
    compaction_condvar: Condvar,
    shutting_down: AtomicBool,
    stats: InnerStats,
}

/// A persistent key-value store backed by append-only data files and a mutable index.
pub struct CandyStore {
    inner: Arc<StoreInner>,
    _lockfile: fslock::LockFile,
    compaction_thd: Mutex<Option<std::thread::JoinHandle<()>>>,
    allow_clean_shutdown: AtomicBool,
    was_clean_shutdown: AtomicBool,
}

pub use list::{KVPair, ListIterator};
pub use typed::{CandyTypedDeque, CandyTypedKey, CandyTypedList, CandyTypedStore};

pub(super) struct OpenState {
    index_file: IndexFile,
    data_files: HashMap<u16, Arc<DataFile>>,
    active_file_idx: u16,
    active_file_ordinal: u64,
    was_clean_shutdown: bool,
}

pub(super) enum DirtyOpenAction {
    None,
    RebuildIndex,
    TrustIndex,
    ResetDb,
}

impl StoreInner {
    fn new(
        base_path: PathBuf,
        config: Arc<Config>,
        state: OpenState,
        num_logical_locks: usize,
    ) -> Self {
        Self {
            base_path,
            config,
            index_file: state.index_file,
            logical_locks: (0..num_logical_locks).map(|_| RwLock::new(())).collect(),
            logical_locks_mask: num_logical_locks - 1,
            data_files: RwLock::new(state.data_files),
            active_file_idx: AtomicU16::new(state.active_file_idx),
            active_file_ordinal: AtomicU64::new(state.active_file_ordinal),
            rotation_lock: Mutex::new(()),
            compaction_state: Mutex::new(CompactionState::default()),
            compaction_condvar: Condvar::new(),
            shutting_down: AtomicBool::new(false),
            stats: InnerStats::default(),
        }
    }

    fn reset(&self) -> Result<()> {
        let _rotation_lock = self.rotation_lock.lock();
        let _logical_guards = self
            .logical_locks
            .iter()
            .map(|lock| lock.write())
            .collect::<Vec<_>>();
        let row_table = self.index_file.rows_table_mut();
        let mut data_files = self.data_files.write();

        data_files.clear();
        self.index_file.reset(row_table)?;

        let index_path = index_file_path(self.base_path.as_path());
        let rows_path = index_rows_file_path(self.base_path.as_path());
        let mut removed_any = false;
        for entry in std::fs::read_dir(&self.base_path).map_err(Error::IOError)? {
            let entry = entry.map_err(Error::IOError)?;
            let path = entry.path();
            if path.file_name().and_then(|name| name.to_str()) == Some(".lockfile")
                || path == index_path
                || path == rows_path
            {
                continue;
            }

            let file_type = entry.file_type().map_err(Error::IOError)?;
            if file_type.is_dir() {
                std::fs::remove_dir_all(&path).map_err(Error::IOError)?;
                removed_any = true;
            } else if file_type.is_file() || file_type.is_symlink() {
                std::fs::remove_file(&path).map_err(Error::IOError)?;
                removed_any = true;
            }
        }
        if removed_any {
            sync_dir(self.base_path.as_path())?;
        }

        let active_file_idx = 0;
        let active_file_ordinal = INITIAL_DATA_FILE_ORDINAL;
        let data_file = Arc::new(DataFile::create(
            self.base_path.as_path(),
            self.config.clone(),
            active_file_idx,
            active_file_ordinal,
        )?);
        data_files.insert(active_file_idx, data_file);
        self.active_file_idx
            .store(active_file_idx, Ordering::Release);
        self.active_file_ordinal
            .store(active_file_ordinal, Ordering::Release);
        self.stats.reset();

        Ok(())
    }

    fn record_lookup(&self, found: bool) {
        if found {
            self.stats
                .num_positive_lookups
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .num_negative_lookups
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_read(&self, bytes: u64) {
        self.stats.num_read_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .num_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_write(&self, bytes: u64) {
        self.stats.num_write_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .num_write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn signal_compaction_scan(&self) {
        let mut state = self.compaction_state.lock();
        if state.wake_requested {
            return;
        }
        state.wake_requested = true;
        self.compaction_condvar.notify_one();
    }

    fn maybe_signal_compaction_threshold_crossing(
        &self,
        file_idx: u16,
        previous_waste: u32,
        new_waste: u32,
    ) {
        if file_idx == self.active_file_idx.load(Ordering::Acquire) {
            return;
        }

        let threshold = self.config.compaction_min_threshold;
        if previous_waste <= threshold && new_waste > threshold {
            self.signal_compaction_scan();
        }
    }

    fn next_compaction_candidate(&self) -> Option<(u16, u64)> {
        let active_file_idx = self.active_file_idx.load(Ordering::Acquire);
        let files = self.data_files.read();
        files
            .iter()
            .filter_map(|(&file_idx, data_file)| {
                if file_idx == active_file_idx
                    || self.index_file.file_waste(file_idx) <= self.config.compaction_min_threshold
                {
                    return None;
                }
                Some((file_idx, data_file.file_ordinal))
            })
            .min_by_key(|(_, file_ordinal)| *file_ordinal)
    }

    fn logical_lock_index(&self, ns: KeyNamespace, key: &[u8]) -> usize {
        let mut hasher = SipHasher13::new_with_keys(0x1701_0a66_2024_6b90, 0x284f_fa2e_3e02_3e2a);
        hasher.write_u8(ns as u8);
        hasher.write(key);
        (hasher.finish() as usize) & self.logical_locks_mask
    }

    fn data_file(&self, file_idx: u16) -> Result<Arc<DataFile>> {
        self.data_files
            .read()
            .get(&file_idx)
            .cloned()
            .ok_or(Error::MissingDataFile(file_idx))
    }

    fn bump_histogram(&self, entry_size: u64) {
        // Buckets: [<64, <256, <1K, <4K, <16K, >=16K]
        // Boundaries at ilog2 = 6, 8, 10, 12, 14 → bucket = ((ilog2 - 4) / 2).clamp(0, 5)
        let bucket = ((entry_size.max(1).ilog2() as usize).saturating_sub(4) / 2).min(5);
        self.index_file.header_ref().size_histogram[bucket].fetch_add(1, Ordering::Relaxed);
    }

    fn _split_row(&self, hc: HashCoord, sl: u64, gsl: u64) -> Result<()> {
        let nsl = sl + 1;
        let low_row_idx = hc.row_index(sl);
        let high_row_idx = low_row_idx | (1 << sl);

        if nsl > gsl
            && let Some(remap_dur) = self.index_file.grow(nsl)?
        {
            self.stats.last_remap_dur_ms.store(
                u64::try_from(remap_dur.as_millis()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }

        let rows_table = self.index_file.rows_table();

        let low_shard = rows_table.shard_id(low_row_idx);
        let high_shard = rows_table.shard_id(high_row_idx);

        let _high_guard = if low_shard < high_shard {
            None // low_row will automatically lock low_shard
        } else if low_shard > high_shard {
            Some(rows_table.lock_shard(high_shard))
        } else {
            None
        };

        let mut low_row = rows_table.row_mut(low_row_idx);

        let _high_guard_post = if low_shard < high_shard {
            Some(rows_table.lock_shard(high_shard))
        } else {
            None
        };

        if low_row.split_level.load(Ordering::Acquire) != sl {
            return Ok(());
        }
        // SAFETY: the high row (being created) has a split_level of 0, making it unusable by anyone.
        // We properly hold the high_row shard lock if it differs from the low_row shard.
        let high_row = unsafe { &mut *rows_table.unlocked_row_ptr(high_row_idx) };
        debug_assert_eq!(high_row.split_level.load(Ordering::Acquire), 0);
        let split_bit = 1 << (sl - MIN_SPLIT_LEVEL as u64);
        for col in 0..ROW_WIDTH {
            let entry = low_row.pointers[col];
            if low_row.signatures[col] != HashCoord::INVALID_SIG
                && entry.is_valid()
                && (entry.masked_row_selector() as u64) & split_bit != 0
            {
                high_row.insert(col, low_row.signatures[col], entry);
                low_row.remove(col);
            }
        }

        low_row.set_split_level(nsl);
        high_row.set_split_level(nsl);

        Ok(())
    }

    /// Rotate to a new data file when the active one is full.
    ///
    /// The `rotation_lock` serializes concurrent rotations, so the read-then-write
    /// on `data_files` (find a free index, then insert) is not a TOCTOU race.
    /// `compact_file` also writes to `data_files` (removing files) but only
    /// touches non-active indices, so there is no conflict.
    fn _rotate_data_file(&self, active_idx: u16) -> Result<()> {
        let _rot_lock = self.rotation_lock.lock();

        if self.active_file_idx.load(Ordering::Acquire) != active_idx {
            return Ok(());
        }

        let active_ordinal = if let Ok(active_file) = self.data_file(active_idx) {
            let _ = active_file.file.sync_all();
            active_file.file_ordinal
        } else {
            0
        };

        let mut next_idx = (self.active_file_idx.load(Ordering::Relaxed) + 1) & MAX_DATA_FILE_IDX;
        let mut attempts = 0;
        {
            let files = self.data_files.read();
            while files.contains_key(&next_idx) {
                next_idx = (next_idx + 1) & MAX_DATA_FILE_IDX;
                attempts += 1;
                if attempts > MAX_DATA_FILES {
                    return Err(Error::TooManyDataFiles);
                }
            }
        }

        let ordinal = self.active_file_ordinal.fetch_add(1, Ordering::Relaxed) + 1;
        let data_file = Arc::new(DataFile::create(
            self.base_path.as_path(),
            self.config.clone(),
            next_idx,
            ordinal,
        )?);

        self.data_files.write().insert(next_idx, data_file);
        self.active_file_idx.store(next_idx, Ordering::Release);

        if active_ordinal != 0
            && self.index_file.file_waste(active_idx) > self.config.compaction_min_threshold
        {
            self.signal_compaction_scan();
        }

        Ok(())
    }

    fn _mut_op<T>(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        val: &[u8],
        mut op: impl FnMut(HashCoord, RowWriteGuard, &[u8], &[u8]) -> Result<T>,
    ) -> Result<T> {
        let entry_size = aligned_data_entry_size(key.len(), val.len()) as usize;
        if key.len() > crate::types::MAX_USER_KEY_SIZE
            || val.len() > crate::types::MAX_USER_VALUE_SIZE
            || entry_size > self.config.max_data_file_size as usize
        {
            return Err(Error::PayloadTooLarge(entry_size));
        }

        let hc = HashCoord::new(ns, key, self.config.hash_key);

        loop {
            let res = {
                let row_table = self.index_file.rows_table();
                let gsl = self
                    .index_file
                    .header_ref()
                    .global_split_level
                    .load(Ordering::Acquire);
                let mut sl = gsl;
                let mut res = None;

                loop {
                    debug_assert!(sl >= MIN_SPLIT_LEVEL as u64, "sl={sl}");
                    let row = row_table.row_mut(hc.row_index(sl));
                    let row_sl = row.split_level.load(Ordering::Acquire);
                    if row_sl == 0 {
                        sl -= 1;
                        continue;
                    }
                    if row_sl > sl {
                        break;
                    }

                    res = Some(op(hc, row, key, val));
                    break;
                }

                res
            };

            let Some(res) = res else {
                continue;
            };

            match res {
                Ok(res) => return Ok(res),
                Err(Error::SplitRow(sl)) => {
                    let gsl = self
                        .index_file
                        .header_ref()
                        .global_split_level
                        .load(Ordering::Acquire);
                    self._split_row(hc, sl, gsl)?;
                }
                Err(Error::RotateDataFile(active_idx)) => {
                    self._rotate_data_file(active_idx)?;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl CandyStore {
    pub fn get_db_path(&self) -> &Path {
        &self.inner.base_path
    }

    fn logical_read_guard(&self, ns: KeyNamespace, key: &[u8]) -> RwLockReadGuard<'_, ()> {
        self.inner.logical_locks[self.inner.logical_lock_index(ns, key)].read()
    }

    fn logical_write_guard(&self, ns: KeyNamespace, key: &[u8]) -> RwLockWriteGuard<'_, ()> {
        self.inner.logical_locks[self.inner.logical_lock_index(ns, key)].write()
    }

    fn _immut_op<T>(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        mut op: impl FnMut(HashCoord, RowReadGuard, &[u8]) -> Result<T>,
    ) -> Result<T> {
        let hc = HashCoord::new(ns, key, self.inner.config.hash_key);
        loop {
            let row_table = self.inner.index_file.rows_table();
            let gsl = self
                .inner
                .index_file
                .header_ref()
                .global_split_level
                .load(Ordering::Acquire);
            let mut sl = gsl;
            loop {
                debug_assert!(sl >= MIN_SPLIT_LEVEL as u64, "sl={sl}");
                let row = row_table.row(hc.row_index(sl));
                let row_sl = row.split_level.load(Ordering::Acquire);
                if row_sl == 0 {
                    sl -= 1;
                    continue;
                }
                if row_sl > sl {
                    break;
                }
                return op(hc, row, key);
            }
        }
    }

    fn get_ns(&self, ns: KeyNamespace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self._immut_op(ns, key, |hc, row, key| {
            let files = self.inner.data_files.read();
            for (_, entry) in row.iter_matches(hc) {
                let Some(file) = files.get(&entry.file_idx()) else {
                    continue;
                };
                self.inner.record_read(entry.size_hint() as u64);
                let kv = match file.read_kv(entry.file_offset(), entry.size_hint()) {
                    Ok(kv) => kv,
                    Err(Error::IOError(e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof
                            || e.kind() == std::io::ErrorKind::InvalidData =>
                    {
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                if kv.key() == key {
                    return Ok(Some(kv.value().to_vec()));
                } else {
                    self.inner
                        .stats
                        .num_collisions
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(None)
        })
    }

    /// Returns the current value for `key`, if it exists.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let value = self.get_ns(KeyNamespace::User, key.as_ref())?;
        self.inner.record_lookup(value.is_some());
        Ok(value)
    }

    /// Returns `true` if `key` currently exists.
    pub fn contains(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.get(key).map(|value| value.is_some())
    }

    fn get_or_create_ns(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        default_val: &[u8],
    ) -> Result<GetOrCreateStatus> {
        self.inner
            ._mut_op(ns, key, default_val, |hc, mut row, key, val| {
                let files = self.inner.data_files.read();
                for (_, entry) in row.iter_matches(hc) {
                    let Some(file) = files.get(&entry.file_idx()) else {
                        continue;
                    };
                    self.inner.record_read(entry.size_hint() as u64);
                    let kv = file.read_kv(entry.file_offset(), entry.size_hint())?;
                    if kv.key() == key {
                        return Ok(GetOrCreateStatus::ExistingValue(kv.into_value()));
                    }
                }

                if let Some(col) = row.find_free_slot() {
                    let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
                    let active_file = files
                        .get(&active_idx)
                        .ok_or(Error::MissingDataFile(active_idx))?;
                    let (file_off, size) = active_file.append_kv(ns, key, val)?;
                    self.inner.record_write(size as u64);
                    row.insert(
                        col,
                        hc.sig,
                        EntryPointer::new(active_idx, file_off, size, hc.masked_row_selector()),
                    );
                    self.record_write_stats(key.len(), val.len());
                    Ok(GetOrCreateStatus::CreatedNew(val.to_vec()))
                } else {
                    Err(Error::SplitRow(row.split_level.load(Ordering::Relaxed)))
                }
            })
    }

    /// Returns the existing value for `key`, or inserts `default_val` and returns it.
    pub fn get_or_create<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        default_val: &B2,
    ) -> Result<GetOrCreateStatus> {
        self.get_or_create_ns(KeyNamespace::User, key.as_ref(), default_val.as_ref())
    }

    fn track_update_waste(&self, file_idx: u16, _file_ordinal: u64, klen: usize, vlen: usize) {
        let added_waste = aligned_data_entry_waste(klen, vlen);
        let new_waste = self.inner.index_file.add_file_waste(file_idx, added_waste);
        self.inner.maybe_signal_compaction_threshold_crossing(
            file_idx,
            new_waste.saturating_sub(added_waste),
            new_waste,
        );
    }

    fn record_write_stats(&self, klen: usize, vlen: usize) {
        let entry_size = aligned_data_entry_size(klen, vlen);
        let h = self.inner.index_file.header_ref();
        h.written_bytes.fetch_add(entry_size, Ordering::Relaxed);
        h.num_created.fetch_add(1, Ordering::Relaxed);
        self.inner.bump_histogram(entry_size);
    }

    fn record_replace_stats(
        &self,
        old_klen: usize,
        old_vlen: usize,
        new_klen: usize,
        new_vlen: usize,
    ) {
        let old_entry_size = aligned_data_entry_size(old_klen, old_vlen);
        let new_entry_size = aligned_data_entry_size(new_klen, new_vlen);
        let h = self.inner.index_file.header_ref();
        h.written_bytes.fetch_add(new_entry_size, Ordering::Relaxed);
        h.waste_bytes.fetch_add(old_entry_size, Ordering::Relaxed);
        h.num_replaced.fetch_add(1, Ordering::Relaxed);
    }

    fn record_remove_stats(&self, klen: usize, vlen: usize) {
        let entry_size = aligned_data_entry_size(klen, vlen);
        let h = self.inner.index_file.header_ref();
        h.waste_bytes.fetch_add(entry_size, Ordering::Relaxed);
        h.num_removed.fetch_add(1, Ordering::Relaxed);
    }

    fn set_ns(&self, ns: KeyNamespace, key: &[u8], val: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner._mut_op(ns, key, val, |hc, mut row, key, val| {
            let files = self.inner.data_files.read();
            for (col, entry) in row.iter_matches(hc) {
                let Some(file) = files.get(&entry.file_idx()) else {
                    continue;
                };
                self.inner.record_read(entry.size_hint() as u64);
                let kv = file.read_kv(entry.file_offset(), entry.size_hint())?;
                if kv.key() == key {
                    let klen = kv.key().len();
                    let vlen = kv.value().len();
                    let old_val = kv.into_value();
                    let src_file_idx = file.file_idx;
                    let src_file_ordinal = file.file_ordinal;

                    let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
                    let active_file = files
                        .get(&active_idx)
                        .ok_or(Error::MissingDataFile(active_idx))?;
                    let (file_off, size) = active_file.append_kv(ns, key, val)?;
                    self.inner.record_write(size as u64);

                    row.replace_pointer(
                        col,
                        EntryPointer::new(active_idx, file_off, size, hc.masked_row_selector()),
                    );
                    self.track_update_waste(src_file_idx, src_file_ordinal, klen, vlen);
                    self.record_replace_stats(klen, vlen, key.len(), val.len());
                    return Ok(Some(old_val));
                } else {
                    self.inner
                        .stats
                        .num_collisions
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            if let Some(col) = row.find_free_slot() {
                let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
                let active_file = files
                    .get(&active_idx)
                    .ok_or(Error::MissingDataFile(active_idx))?;
                let (file_off, size) = active_file.append_kv(ns, key, val)?;
                self.inner.record_write(size as u64);
                row.insert(
                    col,
                    hc.sig,
                    EntryPointer::new(active_idx, file_off, size, hc.masked_row_selector()),
                );
                self.record_write_stats(key.len(), val.len());
                Ok(None)
            } else {
                Err(Error::SplitRow(row.split_level.load(Ordering::Relaxed)))
            }
        })
    }

    /// Inserts or replaces `key` with `val`.
    pub fn set(&self, key: impl AsRef<[u8]>, val: impl AsRef<[u8]>) -> Result<crate::SetStatus> {
        Ok(
            match self.set_ns(KeyNamespace::User, key.as_ref(), val.as_ref())? {
                Some(previous) => crate::SetStatus::PrevValue(previous),
                None => crate::SetStatus::CreatedNew,
            },
        )
    }

    fn replace_ns(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        val: &[u8],
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        self.inner._mut_op(ns, key, val, |hc, mut row, key, val| {
            let files = self.inner.data_files.read();
            for (col, entry) in row.iter_matches(hc) {
                let Some(file) = files.get(&entry.file_idx()) else {
                    continue;
                };
                self.inner.record_read(entry.size_hint() as u64);
                let kv = file.read_kv(entry.file_offset(), entry.size_hint())?;
                if kv.key() == key {
                    if let Some(expected) = expected_val
                        && kv.value() != expected
                    {
                        return Ok(ReplaceStatus::WrongValue(kv.into_value()));
                    }

                    let klen = kv.key().len();
                    let vlen = kv.value().len();
                    let old_val = kv.into_value();
                    let src_file_idx = file.file_idx;
                    let src_file_ordinal = file.file_ordinal;

                    let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
                    let active_file = files
                        .get(&active_idx)
                        .ok_or(Error::MissingDataFile(active_idx))?;
                    let (file_off, size) = active_file.append_kv(ns, key, val)?;
                    self.inner.record_write(size as u64);
                    row.replace_pointer(
                        col,
                        EntryPointer::new(active_idx, file_off, size, hc.masked_row_selector()),
                    );
                    self.track_update_waste(src_file_idx, src_file_ordinal, klen, vlen);
                    self.record_replace_stats(klen, vlen, key.len(), val.len());
                    return Ok(ReplaceStatus::PrevValue(old_val));
                }
            }
            Ok(ReplaceStatus::DoesNotExist)
        })
    }

    /// Replaces `key` with `val` only if the current value matches `expected_val` when provided.
    pub fn replace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized, B3: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
        expected_val: Option<&B3>,
    ) -> Result<ReplaceStatus> {
        self.replace_ns(
            KeyNamespace::User,
            key.as_ref(),
            val.as_ref(),
            expected_val.map(|expected| expected.as_ref()),
        )
    }

    fn track_tombstone_waste(&self, file_idx: u16, _file_ordinal: u64, klen: usize, vlen: usize) {
        let active_idx = self.inner.active_file_idx.load(Ordering::Relaxed);
        if file_idx == active_idx {
            self.inner.index_file.add_file_waste(
                file_idx,
                aligned_data_entry_waste(klen, vlen) + aligned_tombstone_entry_waste(klen),
            );
        } else {
            let old_entry_waste = aligned_data_entry_waste(klen, vlen);
            let new_waste = self
                .inner
                .index_file
                .add_file_waste(file_idx, old_entry_waste);
            self.inner.maybe_signal_compaction_threshold_crossing(
                file_idx,
                new_waste.saturating_sub(old_entry_waste),
                new_waste,
            );
            self.inner
                .index_file
                .add_file_waste(active_idx, aligned_tombstone_entry_waste(klen));
        }
    }

    fn remove_ns(&self, ns: KeyNamespace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner._mut_op(ns, key, &[], |hc, mut row, key, _| {
            let files = self.inner.data_files.read();
            for (col, entry) in row.iter_matches(hc) {
                let Some(file) = files.get(&entry.file_idx()) else {
                    continue;
                };
                self.inner.record_read(entry.size_hint() as u64);
                let kv = file.read_kv(entry.file_offset(), entry.size_hint())?;

                if kv.key() == key {
                    let klen = kv.key().len();
                    let vlen = kv.value().len();
                    let old_val = kv.into_value();
                    let src_file_idx = file.file_idx;
                    let src_file_ordinal = file.file_ordinal;

                    let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
                    let active_file = files
                        .get(&active_idx)
                        .ok_or(Error::MissingDataFile(active_idx))?;
                    let tombstone_size = active_file.append_tombstone(ns, key)?;
                    self.inner.record_write(tombstone_size as u64);

                    row.remove(col);
                    self.track_tombstone_waste(src_file_idx, src_file_ordinal, klen, vlen);
                    self.record_remove_stats(klen, vlen);
                    return Ok(Some(old_val));
                }
            }

            Ok(None)
        })
    }

    /// Removes `key` and returns its previous value if it existed.
    pub fn remove(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.remove_ns(KeyNamespace::User, key.as_ref())
    }

    /// Iterates over all currently live user key/value pairs.
    pub fn iter_items(&self) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_ {
        let mut row_idx = 0usize;
        let mut row_entries: Vec<EntryPointer> = Vec::with_capacity(ROW_WIDTH);
        let mut batch_files = None::<RwLockReadGuard<'_, HashMap<u16, Arc<DataFile>>>>;
        let mut scratch_buf = Vec::new();
        let mut ptr_idx = 0usize;

        std::iter::from_fn(move || {
            loop {
                if ptr_idx < row_entries.len() {
                    let ptr = row_entries[ptr_idx];
                    ptr_idx += 1;
                    let files = batch_files
                        .as_ref()
                        .expect("row entries should only be drained with a file map guard");
                    let Some(file) = files.get(&ptr.file_idx()) else {
                        continue;
                    };
                    self.inner.record_read(ptr.size_hint() as u64);
                    let kv = match file.read_kv_into(
                        ptr.file_offset(),
                        ptr.size_hint(),
                        &mut scratch_buf,
                    ) {
                        Ok(kv) => kv,
                        Err(Error::IOError(e))
                            if e.kind() == std::io::ErrorKind::UnexpectedEof
                                || e.kind() == std::io::ErrorKind::InvalidData =>
                        {
                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    };
                    if kv.ns != KeyNamespace::User as u8 {
                        continue;
                    }
                    let key = kv.key().to_vec();
                    let value = kv.value().to_vec();
                    return Some(Ok((key, value)));
                }

                row_entries.clear();
                batch_files = None;
                ptr_idx = 0;

                loop {
                    let row_table = self.inner.index_file.rows_table();
                    let gsl = self
                        .inner
                        .index_file
                        .header_ref()
                        .global_split_level
                        .load(Ordering::Acquire);
                    let active_rows = 1usize << gsl;

                    if row_idx >= active_rows {
                        break;
                    }

                    let idx = row_idx;
                    row_idx += 1;

                    let row = row_table.row(idx);
                    if row.split_level.load(Ordering::Acquire) == 0 {
                        continue;
                    }
                    for col in 0..ROW_WIDTH {
                        if row.signatures[col] != HashCoord::INVALID_SIG
                            && row.pointers[col].is_valid()
                        {
                            row_entries.push(row.pointers[col]);
                        }
                    }
                    batch_files = Some(self.inner.data_files.read());
                    break;
                }

                if row_entries.is_empty() {
                    return None;
                }
            }
        })
    }

    /// Flushes index and data files to stable storage.
    pub fn flush(&self) -> Result<()> {
        self.inner.index_file.sync_all()?;
        let files = self.inner.data_files.read();
        for data_file in files.values() {
            data_file.file.sync_all().map_err(Error::IOError)?;
        }
        sync_dir(&self.inner.base_path)
    }

    /// Returns whether the store was opened from a clean shutdown state.
    pub fn was_clean_shutdown(&self) -> bool {
        self.was_clean_shutdown.load(Ordering::Relaxed)
    }

    /// Returns the number of background compaction errors observed since open.
    pub fn compaction_errors(&self) -> u64 {
        self.inner.stats.compaction_errors.load(Ordering::Relaxed)
    }

    /// Returns the number of currently live entries.
    pub fn num_items(&self) -> usize {
        self.stats().num_entries() as usize
    }

    /// Returns the current index capacity in entries.
    pub fn capacity(&self) -> usize {
        let row_table = self.inner.index_file.rows_table();
        let row_count = row_table.row_guard.len() / std::mem::size_of::<RowLayout>();
        row_count * ROW_WIDTH
    }

    /// Shrinks the index when the reclaimable row ratio is at least `min_wasted_ratio`.
    pub fn shrink_to_fit_blocking(&self, min_wasted_ratio: f64) -> Result<usize> {
        let _key_guards = self
            .inner
            .logical_locks
            .iter()
            .map(|lock| lock.write())
            .collect::<Vec<_>>();

        let min_wasted_ratio = min_wasted_ratio.clamp(0.0, 1.0);
        let current_rows = self.inner.index_file.num_rows();
        if current_rows == 0 {
            return Ok(0);
        }

        let required_rows = self.num_items().div_ceil(ROW_WIDTH * 8 / 10).max(1);
        let min_rows_cfg = (self.inner.config.initial_capacity / ROW_WIDTH)
            .max(1usize << MIN_SPLIT_LEVEL)
            .max(1);
        let min_rows = required_rows.max(min_rows_cfg);

        let reclaimable_rows = current_rows.saturating_sub(min_rows);
        let reclaimable_ratio = reclaimable_rows as f64 / current_rows as f64;
        if reclaimable_ratio < min_wasted_ratio {
            return Ok(current_rows);
        }

        self.inner.index_file.shrink(min_rows_cfg)
    }

    /// Returns a snapshot of store statistics and accounting counters.
    pub fn stats(&self) -> Stats {
        let h = self.inner.index_file.header_ref();
        let num_rows = self.inner.index_file.num_rows() as u64;
        let capacity = num_rows.saturating_mul(ROW_WIDTH as u64);
        let num_items = h
            .num_created
            .load(Ordering::Relaxed)
            .saturating_sub(h.num_removed.load(Ordering::Relaxed));
        Stats {
            num_rows,
            capacity,
            num_items,
            index_size_bytes: self.inner.index_file.file_size_bytes(),
            num_compactions: self.inner.stats.num_compactions.load(Ordering::Relaxed),
            compaction_time_ms: self.inner.stats.compaction_time_ms.load(Ordering::Relaxed),
            num_data_files: self.inner.data_files.read().len() as u64,
            num_positive_lookups: self
                .inner
                .stats
                .num_positive_lookups
                .load(Ordering::Relaxed),
            num_negative_lookups: self
                .inner
                .stats
                .num_negative_lookups
                .load(Ordering::Relaxed),
            num_collisions: self.inner.stats.num_collisions.load(Ordering::Relaxed),
            last_remap_dur: Duration::from_millis(
                self.inner.stats.last_remap_dur_ms.load(Ordering::Relaxed),
            ),
            last_compaction_dur: Duration::from_millis(
                self.inner
                    .stats
                    .last_compaction_dur_ms
                    .load(Ordering::Relaxed),
            ),
            last_compaction_reclaimed_bytes: self
                .inner
                .stats
                .last_compaction_reclaimed_bytes
                .load(Ordering::Relaxed),
            last_compaction_moved_bytes: self
                .inner
                .stats
                .last_compaction_moved_bytes
                .load(Ordering::Relaxed),
            num_read_ops: self.inner.stats.num_read_ops.load(Ordering::Relaxed),
            num_read_bytes: self.inner.stats.num_read_bytes.load(Ordering::Relaxed),
            num_write_ops: self.inner.stats.num_write_ops.load(Ordering::Relaxed),
            num_write_bytes: self.inner.stats.num_write_bytes.load(Ordering::Relaxed),
            num_created: h.num_created.load(Ordering::Relaxed),
            num_removed: h.num_removed.load(Ordering::Relaxed),
            num_replaced: h.num_replaced.load(Ordering::Relaxed),
            written_bytes: h.written_bytes.load(Ordering::Relaxed),
            waste_bytes: h.waste_bytes.load(Ordering::Relaxed),
            reclaimed_bytes: h.reclaimed_bytes.load(Ordering::Relaxed),
            entries_under_64: h.size_histogram[0].load(Ordering::Relaxed),
            entries_under_256: h.size_histogram[1].load(Ordering::Relaxed),
            entries_under_1024: h.size_histogram[2].load(Ordering::Relaxed),
            entries_under_4096: h.size_histogram[3].load(Ordering::Relaxed),
            entries_under_16384: h.size_histogram[4].load(Ordering::Relaxed),
            entries_over_16384: h.size_histogram[5].load(Ordering::Relaxed),
        }
    }

    /// Simulates a crash by dropping the instance without performing clean shutdown operations (e.g. marking the index as clean).
    pub fn _abort_for_testing(self) {
        self.allow_clean_shutdown.store(false, Ordering::Relaxed);
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_compaction_errors_reports_counter() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        assert_eq!(db.compaction_errors(), 0);

        db.inner.stats.compaction_errors.store(7, Ordering::Relaxed);

        assert_eq!(db.compaction_errors(), 7);

        Ok(())
    }

    #[test]
    fn test_stats_reports_transient_collision_counter() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        db.inner.stats.num_collisions.store(11, Ordering::Relaxed);

        assert_eq!(db.stats().num_collisions, 11);

        Ok(())
    }

    #[test]
    fn test_stats_reports_last_remap_duration() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        db.inner
            .stats
            .last_remap_dur_ms
            .store(17, Ordering::Relaxed);

        assert_eq!(db.stats().last_remap_dur, Duration::from_millis(17));

        Ok(())
    }

    #[test]
    fn test_stats_reports_last_compaction_stats() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        db.inner
            .stats
            .last_compaction_dur_ms
            .store(23, Ordering::Relaxed);
        db.inner
            .stats
            .last_compaction_reclaimed_bytes
            .store(1234, Ordering::Relaxed);
        db.inner
            .stats
            .last_compaction_moved_bytes
            .store(5678, Ordering::Relaxed);

        let stats = db.stats();
        assert_eq!(stats.last_compaction_dur, Duration::from_millis(23));
        assert_eq!(stats.last_compaction_reclaimed_bytes, 1234);
        assert_eq!(stats.last_compaction_moved_bytes, 5678);

        Ok(())
    }
}
