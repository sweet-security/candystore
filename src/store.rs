mod checkpoint;
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
        atomic::{AtomicBool, AtomicI64, AtomicU16, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    data_file::{DataFile, InflightTracker},
    index_file::{EntryPointer, IndexFile, RowLayout, RowReadGuard, RowWriteGuard},
    internal::{
        EntryType, HashCoord, KeyNamespace, MAX_DATA_FILE_IDX, MAX_DATA_FILES, MIN_SPLIT_LEVEL,
        ROW_WIDTH, RangeMetadata, aligned_data_entry_size, aligned_data_entry_waste,
        aligned_tombstone_entry_waste, index_file_path, index_rows_file_path, sync_dir,
    },
    types::{
        Config, Error, GetOrCreateStatus, INITIAL_DATA_FILE_ORDINAL, ReplaceStatus, Result, Stats,
    },
};

#[derive(Default)]
struct CompactionState {
    wake_requested: bool,
}

// this is needed because std::io::Error is not clone()
#[derive(Debug, Clone)]
enum CheckpointFailure {
    IO(std::io::ErrorKind, String),
    MissingDataFile(u16),
    Other(String),
}

impl CheckpointFailure {
    fn from_error(err: Error) -> Self {
        match err {
            Error::IOError(io_err) => Self::IO(io_err.kind(), io_err.to_string()),
            Error::MissingDataFile(file_idx) => Self::MissingDataFile(file_idx),
            other => Self::Other(other.to_string()),
        }
    }

    fn to_error(&self) -> Error {
        match self {
            Self::IO(kind, message) => Error::IOError(std::io::Error::new(*kind, message.clone())),
            Self::MissingDataFile(file_idx) => Error::MissingDataFile(*file_idx),
            Self::Other(message) => Error::IOError(std::io::Error::other(message.clone())),
        }
    }
}

#[derive(Default)]
struct CheckpointState {
    requested_epoch: u64,
    handled_epoch: u64,
    completed_epoch: u64,
    last_failure_epoch: u64,
    last_failure: Option<CheckpointFailure>,
    last_checkpoint_dur_ms: u64,
}

#[derive(Clone, Copy)]
struct CheckpointSnapshot {
    checkpoint_ordinal: u64,
    checkpoint_offset: u64,
    checkpointed_delta: i64,
    last_commit_ordinal: u64,
}

#[derive(Default)]
struct InnerStats {
    num_compactions: AtomicU64,
    compaction_errors: AtomicU64,
    checkpoint_errors: AtomicU64,
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
    num_inserted: AtomicU64,
    num_updated: AtomicU64,
    num_removed: AtomicU64,
    num_rebuilt_entries: AtomicU64,
    num_rebuild_purged_bytes: AtomicU64,
    size_histogram: [AtomicU64; 6],
}

impl InnerStats {
    fn reset(&self) {
        self.num_compactions.store(0, Ordering::Relaxed);
        self.compaction_errors.store(0, Ordering::Relaxed);
        self.checkpoint_errors.store(0, Ordering::Relaxed);
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
        self.num_inserted.store(0, Ordering::Relaxed);
        self.num_removed.store(0, Ordering::Relaxed);
        self.num_updated.store(0, Ordering::Relaxed);
        self.num_rebuilt_entries.store(0, Ordering::Relaxed);
        self.num_rebuild_purged_bytes.store(0, Ordering::Relaxed);
        for bucket in &self.size_histogram {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

struct StoreInner {
    base_path: PathBuf,
    config: Arc<Config>,
    index_file: IndexFile,
    list_meta_locks: Vec<RwLock<()>>,
    list_meta_locks_mask: usize,
    data_files: RwLock<HashMap<u16, Arc<DataFile>>>,
    inflight_tracker: InflightTracker,
    active_file_idx: AtomicU16,
    active_file_ordinal: AtomicU64,
    uncommitted_entries_delta: AtomicI64,
    checkpoint_state: Mutex<CheckpointState>,
    checkpoint_condvar: Condvar,
    checkpoint_shutting_down: AtomicBool,
    rotation_lock: Mutex<()>,
    compaction_state: Mutex<CompactionState>,
    compaction_condvar: Condvar,
    compaction_shutting_down: AtomicBool,
    stats: InnerStats,
}

struct ExistingEntryUpdate<'a> {
    files: &'a HashMap<u16, Arc<DataFile>>,
    ns: KeyNamespace,
    key: &'a [u8],
    val: &'a [u8],
    hc: HashCoord,
    col: usize,
    shard_idx: usize,
    src_file_idx: u16,
    old_klen: usize,
    old_vlen: usize,
    crash_point_name: Option<&'a str>,
}

/// A persistent key-value store backed by append-only data files and a mutable index.
pub struct CandyStore {
    inner: Arc<StoreInner>,
    _lockfile: fslock::LockFile,
    compaction_thd: Mutex<Option<std::thread::JoinHandle<()>>>,
    checkpoint_thd: Mutex<Option<std::thread::JoinHandle<()>>>,
    allow_clean_shutdown: AtomicBool,
}

pub use list::{KVPair, ListIterator};
pub use typed::{CandyTypedDeque, CandyTypedKey, CandyTypedList, CandyTypedStore};

pub(super) struct OpenState {
    index_file: IndexFile,
    data_files: HashMap<u16, Arc<DataFile>>,
    active_file_idx: u16,
    active_file_ordinal: u64,
}

impl StoreInner {
    fn new(
        base_path: PathBuf,
        config: Arc<Config>,
        state: OpenState,
        num_logical_locks: usize,
    ) -> Self {
        let num_shards = state.index_file.num_shards();
        Self {
            base_path,
            config,
            index_file: state.index_file,
            list_meta_locks: (0..num_logical_locks).map(|_| RwLock::new(())).collect(),
            list_meta_locks_mask: num_logical_locks - 1,
            data_files: RwLock::new(state.data_files),
            inflight_tracker: InflightTracker::new(num_shards),
            active_file_idx: AtomicU16::new(state.active_file_idx),
            active_file_ordinal: AtomicU64::new(state.active_file_ordinal),
            uncommitted_entries_delta: AtomicI64::new(0),
            checkpoint_state: Mutex::new(CheckpointState::default()),
            checkpoint_condvar: Condvar::new(),
            checkpoint_shutting_down: AtomicBool::new(false),
            rotation_lock: Mutex::new(()),
            compaction_state: Mutex::new(CompactionState::default()),
            compaction_condvar: Condvar::new(),
            compaction_shutting_down: AtomicBool::new(false),
            stats: InnerStats::default(),
        }
    }

    fn reset(&self) -> Result<()> {
        let _logical_guards = self
            .list_meta_locks
            .iter()
            .map(|lock| lock.write())
            .collect::<Vec<_>>();
        let row_table = self.index_file.rows_table_mut();
        let _rotation_lock = self.rotation_lock.lock();
        let mut data_files = self.data_files.write();

        data_files.clear();
        self.inflight_tracker.clear_all();
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
        self.uncommitted_entries_delta.store(0, Ordering::Relaxed);
        *self.checkpoint_state.lock() = CheckpointState::default();
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

    fn record_write(&self, offset: u64, bytes: u64) {
        self.stats.num_write_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .num_write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.note_checkpoint_write(offset + bytes);
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

    fn next_compaction_candidates(&self, max_candidates: usize) -> Vec<(u16, u64)> {
        let active_file_idx = self.active_file_idx.load(Ordering::Acquire);
        let commit_file_ordinal = self.index_file.checkpoint_cursor().0;
        let files = self.data_files.read();
        let mut candidates = files
            .iter()
            .filter_map(|(&file_idx, data_file)| {
                if file_idx == active_file_idx
                    || data_file.file_ordinal >= commit_file_ordinal
                    || self.index_file.file_waste(file_idx) <= self.config.compaction_min_threshold
                {
                    return None;
                }
                Some((
                    file_idx,
                    data_file.file_ordinal,
                    self.index_file.file_waste(file_idx),
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .2
                .cmp(&left.2)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.0.cmp(&right.0))
        });
        candidates
            .into_iter()
            .take(max_candidates)
            .map(|(file_idx, file_ordinal, _)| (file_idx, file_ordinal))
            .collect()
    }

    fn logical_lock_index(&self, ns: KeyNamespace, key: &[u8]) -> usize {
        let mut hasher = SipHasher13::new_with_keys(0x1701_0a66_2024_6b90, 0x284f_fa2e_3e02_3e2a);
        hasher.write_u8(ns as u8);
        hasher.write(key);
        (hasher.finish() as usize) & self.list_meta_locks_mask
    }

    fn data_file(&self, file_idx: u16) -> Result<Arc<DataFile>> {
        self.data_files
            .read()
            .get(&file_idx)
            .cloned()
            .ok_or(Error::MissingDataFile(file_idx))
    }

    fn ordered_data_files(&self) -> Vec<Arc<DataFile>> {
        let mut files = self.data_files.read().values().cloned().collect::<Vec<_>>();
        files.sort_by_key(|data_file| data_file.file_ordinal);
        files
    }

    fn bump_histogram(&self, entry_size: u64) {
        // Buckets: [<64, <256, <1K, <4K, <16K, >=16K]
        // Boundaries at ilog2 = 6, 8, 10, 12, 14 → bucket = ((ilog2 - 4) / 2).clamp(0, 5)
        let bucket = ((entry_size.max(1).ilog2() as usize).saturating_sub(4) / 2).min(5);
        self.stats.size_histogram[bucket].fetch_add(1, Ordering::Relaxed);
    }

    fn add_uncommitted_num_entries(&self, delta: i64) {
        self.uncommitted_entries_delta
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Applies `delta` to the persisted committed entry count, clamping at
    /// zero.  Returns the actual change applied (which may differ from `delta`
    /// when the count would underflow).
    fn advance_committed_num_entries(&self, delta: i64) -> i64 {
        if delta == 0 {
            return 0;
        }

        let committed = &self.index_file.header_ref().committed_num_entries;
        let mut current = committed.load(Ordering::Relaxed);
        loop {
            let updated = current.saturating_add_signed(delta);
            match committed.compare_exchange_weak(
                current,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return updated as i64 - current as i64,
                Err(observed) => current = observed,
            }
        }
    }

    /// Folds a checkpointed delta into the persisted committed count and
    /// adjusts the runtime uncommitted delta so that
    /// `committed + uncommitted == live_count` is preserved.
    ///
    /// When many inserts and removes of the same keys happen within one
    /// checkpoint window, the drained delta can be more negative than
    /// `committed` can absorb (since it is unsigned).  In that case only
    /// the clamped portion is applied and the remainder stays in
    /// `uncommitted_entries_delta`.
    fn fold_checkpointed_num_entries(&self, delta: i64) {
        if delta == 0 {
            return;
        }

        let actual = self.advance_committed_num_entries(delta);
        self.uncommitted_entries_delta
            .fetch_add(-actual, Ordering::Relaxed);
    }

    fn persist_checkpoint_cursor(&self, ordinal: u64, offset: u64) {
        self.index_file.persist_checkpoint_cursor(ordinal, offset);
    }

    fn perform_checkpoint(&self) -> Result<()> {
        let snapshot = self.snapshot_checkpoint_progress()?;
        let current_cursor = self.index_file.checkpoint_cursor();
        if snapshot.checkpoint_ordinal == current_cursor.0
            && snapshot.checkpoint_offset == current_cursor.1
            && snapshot.checkpointed_delta == 0
        {
            return Ok(());
        }
        self.sync_checkpoint(snapshot)
    }

    fn snapshot_checkpoint_progress(&self) -> Result<CheckpointSnapshot> {
        let files = self.data_files.read();
        let active_idx = self.active_file_idx.load(Ordering::Acquire);
        let active_file = files
            .get(&active_idx)
            .cloned()
            .ok_or(Error::MissingDataFile(active_idx))?;
        let (checkpoint_ordinal, checkpoint_offset, checkpointed_delta) =
            self.inflight_tracker.checkpoint_progress(&active_file);
        let last_commit_ordinal = self.index_file.checkpoint_cursor().0;
        Ok(CheckpointSnapshot {
            checkpoint_ordinal,
            checkpoint_offset,
            checkpointed_delta,
            last_commit_ordinal,
        })
    }

    fn sync_checkpoint(&self, snap: CheckpointSnapshot) -> Result<()> {
        let files = self.data_files.read();
        for data_file in files.values() {
            if data_file.file_ordinal >= snap.last_commit_ordinal {
                data_file.file.sync_all().map_err(Error::IOError)?;
            }
        }
        drop(files);

        self.fold_checkpointed_num_entries(snap.checkpointed_delta);
        self.persist_checkpoint_cursor(snap.checkpoint_ordinal, snap.checkpoint_offset);
        self.index_file.sync_all()?;
        sync_dir(&self.base_path)
    }

    fn perform_checkpoint_with_logical_locks(&self) -> Result<()> {
        let _logical_guards = self
            .list_meta_locks
            .iter()
            .map(|lock| lock.write())
            .collect::<Vec<_>>();
        self.perform_checkpoint()
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
        debug_assert!(
            low_shard <= high_shard,
            "high_row_idx sets a higher bit, so high_shard >= low_shard"
        );

        let mut low_row = rows_table.row_mut(low_row_idx);

        let _high_guard = if low_shard < high_shard {
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
        {
            let _rot_lock = self.rotation_lock.lock();

            if self.active_file_idx.load(Ordering::Acquire) != active_idx {
                return Ok(());
            }

            let active_file = self.data_file(active_idx)?;
            let active_ordinal = active_file.file_ordinal;

            let mut next_idx =
                (self.active_file_idx.load(Ordering::Relaxed) + 1) & MAX_DATA_FILE_IDX;
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

            active_file.seal_for_rotation();

            self.data_files.write().insert(next_idx, data_file);
            self.active_file_idx.store(next_idx, Ordering::Release);

            if active_ordinal != 0
                && self.index_file.file_waste(active_idx) > self.config.compaction_min_threshold
            {
                self.signal_compaction_scan();
            }
        }

        _ = self.request_checkpoint_epoch();
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
                    let row = row_table.row_mut(hc.row_index(sl));
                    let row_sl = row.split_level.load(Ordering::Acquire);
                    if row_sl == 0 {
                        // nonexistent row
                        sl -= 1;
                        continue;
                    }
                    if row_sl > sl {
                        // split happened, retry
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
                    // note: it is critical we do not hold the row's lock here
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

    fn list_read_guard(&self, ns: KeyNamespace, key: &[u8]) -> RwLockReadGuard<'_, ()> {
        self.inner.list_meta_locks[self.inner.logical_lock_index(ns, key)].read()
    }

    fn list_write_guard(&self, ns: KeyNamespace, key: &[u8]) -> RwLockWriteGuard<'_, ()> {
        self.inner.list_meta_locks[self.inner.logical_lock_index(ns, key)].write()
    }

    fn try_heal_range_head<GetMeta, SetMeta>(
        &self,
        meta_ns: KeyNamespace,
        range_key: &[u8],
        initial_next_idx: u64,
        new_head: u64,
        mut get_meta: GetMeta,
        mut set_meta: SetMeta,
    ) -> Result<()>
    where
        GetMeta: FnMut(&CandyStore, &[u8]) -> Result<RangeMetadata>,
        SetMeta: FnMut(&CandyStore, &[u8], RangeMetadata) -> Result<()>,
    {
        let _lock = self.list_write_guard(meta_ns, range_key);
        let mut meta = get_meta(self, range_key)?;
        if meta.head >= initial_next_idx && meta.head < new_head {
            meta.head = new_head;
            set_meta(self, range_key, meta)?;
        }
        Ok(())
    }

    fn try_heal_range_tail<GetMeta, SetMeta>(
        &self,
        meta_ns: KeyNamespace,
        range_key: &[u8],
        initial_end_idx: u64,
        new_tail: u64,
        mut get_meta: GetMeta,
        mut set_meta: SetMeta,
    ) -> Result<()>
    where
        GetMeta: FnMut(&CandyStore, &[u8]) -> Result<RangeMetadata>,
        SetMeta: FnMut(&CandyStore, &[u8], RangeMetadata) -> Result<()>,
    {
        let _lock = self.list_write_guard(meta_ns, range_key);
        let mut meta = get_meta(self, range_key)?;
        if meta.tail <= initial_end_idx && meta.tail > new_tail {
            meta.tail = new_tail;
            set_meta(self, range_key, meta)?;
        }
        Ok(())
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
                let row = row_table.row(hc.row_index(sl));
                let row_sl = row.split_level.load(Ordering::Acquire);
                if row_sl == 0 {
                    // nonexistent row
                    sl -= 1;
                    continue;
                }
                if row_sl > sl {
                    // split happened, retry
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
                    let (file_off, size, inflight_guard) = active_file.append_kv(
                        EntryType::Insert,
                        ns,
                        key,
                        val,
                        row.shard_idx,
                        &self.inner.inflight_tracker,
                    )?;
                    self.inner.record_write(file_off, size as u64);
                    row.insert(
                        col,
                        hc.sig,
                        EntryPointer::new(active_idx, file_off, size, hc.masked_row_selector()),
                    );
                    self.record_write_stats(key.len(), val.len());
                    inflight_guard.complete();
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

    fn track_update_waste(&self, file_idx: u16, klen: usize, vlen: usize) {
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
        self.inner.add_uncommitted_num_entries(1);
        self.inner
            .stats
            .num_inserted
            .fetch_add(1, Ordering::Relaxed);
        self.inner.bump_histogram(entry_size);
    }

    fn record_replace_stats(&self, new_klen: usize, new_vlen: usize) {
        let new_entry_size = aligned_data_entry_size(new_klen, new_vlen);
        self.inner.stats.num_updated.fetch_add(1, Ordering::Relaxed);
        self.inner.bump_histogram(new_entry_size);
    }

    fn record_remove_stats(&self) {
        self.inner.add_uncommitted_num_entries(-1);
        self.inner.stats.num_removed.fetch_add(1, Ordering::Relaxed);
    }

    fn apply_update_to_existing_entry(
        &self,
        row: &mut RowWriteGuard<'_>,
        update: ExistingEntryUpdate<'_>,
    ) -> Result<()> {
        let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
        let active_file = update
            .files
            .get(&active_idx)
            .ok_or(Error::MissingDataFile(active_idx))?;
        let (file_off, size, inflight_guard) = active_file.append_kv(
            EntryType::Update,
            update.ns,
            update.key,
            update.val,
            update.shard_idx,
            &self.inner.inflight_tracker,
        )?;
        self.inner.record_write(file_off, size as u64);
        if let Some(name) = update.crash_point_name {
            crate::crash_point(name);
        }

        row.replace_pointer(
            update.col,
            EntryPointer::new(active_idx, file_off, size, update.hc.masked_row_selector()),
        );
        self.track_update_waste(update.src_file_idx, update.old_klen, update.old_vlen);
        self.record_replace_stats(update.key.len(), update.val.len());
        inflight_guard.complete();
        Ok(())
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
                    // optimization
                    if kv.value() == val {
                        return Ok(Some(kv.into_value()));
                    }
                    let klen = kv.key().len();
                    let vlen = kv.value().len();
                    let old_val = kv.into_value();
                    let src_file_idx = file.file_idx;

                    let shard_idx = row.shard_idx;
                    self.apply_update_to_existing_entry(
                        &mut row,
                        ExistingEntryUpdate {
                            files: &files,
                            ns,
                            key,
                            val,
                            hc,
                            col,
                            shard_idx,
                            src_file_idx,
                            old_klen: klen,
                            old_vlen: vlen,
                            crash_point_name: Some("set_after_write_before_update"),
                        },
                    )?;
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
                let (file_off, size, inflight_guard) = active_file.append_kv(
                    EntryType::Insert,
                    ns,
                    key,
                    val,
                    row.shard_idx,
                    &self.inner.inflight_tracker,
                )?;
                self.inner.record_write(file_off, size as u64);
                crate::crash_point("set_after_write_before_insert");
                row.insert(
                    col,
                    hc.sig,
                    EntryPointer::new(active_idx, file_off, size, hc.masked_row_selector()),
                );
                self.record_write_stats(key.len(), val.len());
                inflight_guard.complete();
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
                    // optimization
                    if kv.value() == val {
                        return Ok(ReplaceStatus::PrevValue(kv.into_value()));
                    }

                    let klen = kv.key().len();
                    let vlen = kv.value().len();
                    let old_val = kv.into_value();
                    let src_file_idx = file.file_idx;

                    let shard_idx = row.shard_idx;
                    self.apply_update_to_existing_entry(
                        &mut row,
                        ExistingEntryUpdate {
                            files: &files,
                            ns,
                            key,
                            val,
                            hc,
                            col,
                            shard_idx,
                            src_file_idx,
                            old_klen: klen,
                            old_vlen: vlen,
                            crash_point_name: None,
                        },
                    )?;
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

    fn track_tombstone_waste(&self, file_idx: u16, klen: usize, vlen: usize) {
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

                    let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
                    let active_file = files
                        .get(&active_idx)
                        .ok_or(Error::MissingDataFile(active_idx))?;
                    let (file_off, tombstone_size, inflight_guard) = active_file.append_tombstone(
                        ns,
                        key,
                        row.shard_idx,
                        &self.inner.inflight_tracker,
                    )?;
                    self.inner.record_write(file_off, tombstone_size as u64);

                    row.remove(col);
                    self.track_tombstone_waste(src_file_idx, klen, vlen);
                    self.record_remove_stats();
                    inflight_guard.complete();
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

    /// Establishes a durable recovery checkpoint.
    ///
    /// Reads the earliest in-flight `(file_ordinal, offset)` tuple across all
    /// shards to determine the first position that may still require replay.
    /// If no writes are in flight, the checkpoint targets the active file tail.
    /// Syncs the data and index files and advances the persisted replay cursor
    /// so the next open can resume from this point without replaying earlier
    /// writes.
    ///
    /// This waits for the background checkpoint worker to establish a checkpoint
    /// after taking all logical list/queue locks, so compound operations are
    /// checkpointed only at well-defined boundaries.
    pub fn checkpoint(&self) -> Result<()> {
        let target_epoch = self.inner.request_checkpoint_epoch();
        self.inner.wait_for_checkpoint_epoch(target_epoch)
    }

    /// Returns the number of background compaction errors observed since open.
    pub fn compaction_errors(&self) -> u64 {
        self.inner.stats.compaction_errors.load(Ordering::Relaxed)
    }

    /// Returns the number of currently live entries.
    pub fn num_items(&self) -> usize {
        let committed = self
            .inner
            .index_file
            .header_ref()
            .committed_num_entries
            .load(Ordering::Relaxed);
        let uncommitted = self.inner.uncommitted_entries_delta.load(Ordering::Relaxed);
        let count = committed.saturating_add_signed(uncommitted);
        debug_assert!(
            (committed as i128 + uncommitted as i128) >= 0,
            "live entry count underflow: committed={committed}, uncommitted={uncommitted}"
        );
        count as usize
    }

    /// Returns the current index capacity in entries.
    pub fn capacity(&self) -> usize {
        let row_table = self.inner.index_file.rows_table();
        let row_count = row_table.row_guard.len() / std::mem::size_of::<RowLayout>();
        row_count * ROW_WIDTH
    }

    /// Shrinks the index when the reclaimable row ratio is at least `min_wasted_ratio`.
    pub fn shrink_to_fit_blocking(&self, min_wasted_ratio: f64) -> Result<usize> {
        let _logical_guards = self
            .inner
            .list_meta_locks
            .iter()
            .map(|lock| lock.write())
            .collect::<Vec<_>>();
        let row_table = self.inner.index_file.rows_table_mut();

        let min_wasted_ratio = min_wasted_ratio.clamp(0.0, 1.0);
        let current_rows = row_table.row_guard.len() / std::mem::size_of::<RowLayout>();
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

        self.inner
            .index_file
            .shrink_with_rows_guard(min_rows, row_table)
    }

    /// Returns a snapshot of store statistics and accounting counters.
    pub fn stats(&self) -> Stats {
        let num_rows = self.inner.index_file.num_rows() as u64;

        // Derive data_bytes and waste_bytes from file sizes and per-file
        // waste levels rather than maintaining them as persistent counters.
        let (total_bytes, num_data_files) = {
            let data_files = self.inner.data_files.read();

            (
                data_files.values().map(|df| df.used_bytes()).sum(),
                data_files.len() as u64,
            )
        };
        let waste_bytes = self.inner.index_file.total_waste();
        let s = &self.inner.stats;
        let checkpoint_state = self.inner.checkpoint_state.lock();
        let checkpoint_generation = self.inner.index_file.checkpoint_generation();
        let checkpoint_epoch = checkpoint_state.completed_epoch;
        let uncheckpointed_bytes = self.inner.approx_uncheckpointed_bytes();
        let last_checkpoint_dur = Duration::from_millis(checkpoint_state.last_checkpoint_dur_ms);

        Stats {
            num_rows,
            num_items: self.num_items() as u64,
            index_size_bytes: self.inner.index_file.file_size_bytes(),
            num_data_files,

            total_bytes,
            waste_bytes,

            num_compactions: s.num_compactions.load(Ordering::Relaxed),
            checkpoint_errors: s.checkpoint_errors.load(Ordering::Relaxed),

            last_remap_dur: Duration::from_millis(s.last_remap_dur_ms.load(Ordering::Relaxed)),
            checkpoint_generation,
            checkpoint_epoch,
            uncheckpointed_bytes,
            last_checkpoint_dur,
            last_compaction_dur: Duration::from_millis(
                s.last_compaction_dur_ms.load(Ordering::Relaxed),
            ),
            last_compaction_reclaimed_bytes: s
                .last_compaction_reclaimed_bytes
                .load(Ordering::Relaxed),
            last_compaction_moved_bytes: s.last_compaction_moved_bytes.load(Ordering::Relaxed),

            num_read_ops: s.num_read_ops.load(Ordering::Relaxed),
            num_read_bytes: s.num_read_bytes.load(Ordering::Relaxed),
            num_write_ops: s.num_write_ops.load(Ordering::Relaxed),
            num_write_bytes: s.num_write_bytes.load(Ordering::Relaxed),

            num_inserted: s.num_inserted.load(Ordering::Relaxed),
            num_updated: s.num_updated.load(Ordering::Relaxed),
            num_removed: s.num_removed.load(Ordering::Relaxed),
            num_positive_lookups: s.num_positive_lookups.load(Ordering::Relaxed),
            num_negative_lookups: s.num_negative_lookups.load(Ordering::Relaxed),
            num_collisions: s.num_collisions.load(Ordering::Relaxed),

            num_rebuilt_entries: s.num_rebuilt_entries.load(Ordering::Relaxed),
            num_rebuild_purged_bytes: s.num_rebuild_purged_bytes.load(Ordering::Relaxed),

            entries_under_64: s.size_histogram[0].load(Ordering::Relaxed),
            entries_under_256: s.size_histogram[1].load(Ordering::Relaxed),
            entries_under_1024: s.size_histogram[2].load(Ordering::Relaxed),
            entries_under_4096: s.size_histogram[3].load(Ordering::Relaxed),
            entries_under_16384: s.size_histogram[4].load(Ordering::Relaxed),
            entries_over_16384: s.size_histogram[5].load(Ordering::Relaxed),
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

    use std::{thread, time::Instant};

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

    #[test]
    fn test_stats_reports_rebuild_counters() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        db.inner
            .stats
            .num_rebuilt_entries
            .store(11, Ordering::Relaxed);
        db.inner
            .stats
            .num_rebuild_purged_bytes
            .store(96, Ordering::Relaxed);

        let stats = db.stats();
        assert_eq!(stats.num_rebuilt_entries, 11);
        assert_eq!(stats.num_rebuild_purged_bytes, 96);

        Ok(())
    }

    #[test]
    fn test_stats_reports_checkpoint_state() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        db.stop_compaction();
        db.set("checkpoint-stats", vec![b'z'; 512])?;

        let active_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        let active_ordinal = db
            .inner
            .data_files
            .read()
            .get(&active_idx)
            .expect("active data file should exist")
            .file_ordinal;
        db.inner.persist_checkpoint_cursor(active_ordinal, 0);

        {
            let mut checkpoint_state = db.inner.checkpoint_state.lock();
            checkpoint_state.completed_epoch = 13;
            checkpoint_state.last_checkpoint_dur_ms = 29;
        }

        let expected_dirty = db
            .inner
            .data_files
            .read()
            .get(&active_idx)
            .expect("active data file should exist")
            .used_bytes();

        let stats = db.stats();
        assert!(stats.checkpoint_generation > 0);
        assert_eq!(stats.checkpoint_epoch, 13);
        assert_eq!(stats.uncheckpointed_bytes, expected_dirty);
        assert_eq!(stats.last_checkpoint_dur, Duration::from_millis(29));

        Ok(())
    }

    #[test]
    fn test_checkpoint_does_not_join_compaction_thread() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(dir.path(), Config::default())?;

        db.stop_compaction();
        *db.compaction_thd.lock() = Some(thread::spawn(|| {
            thread::sleep(Duration::from_millis(400));
        }));

        let t0 = Instant::now();
        db.checkpoint()?;
        assert!(
            t0.elapsed() < Duration::from_millis(200),
            "checkpoint should not wait for the compaction thread handle"
        );

        db.compaction_thd
            .lock()
            .take()
            .expect("test compaction thread should still be present")
            .join()
            .expect("test compaction thread panicked");

        Ok(())
    }

    #[test]
    fn test_rotation_schedules_background_checkpoint() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 2048,
                compaction_min_threshold: u32::MAX,
                compaction_throughput_bytes_per_sec: 0,
                ..Config::default()
            },
        )?;

        db.stop_compaction();
        while db.stats().num_data_files < 2 {
            let idx = db.stats().num_write_ops;
            db.set(
                format!("rotate-{idx}"),
                format!("payload-{}", "x".repeat(768)),
            )?;
        }

        let t0 = Instant::now();
        while db.inner.index_file.checkpoint_cursor() == (0, 0) {
            assert!(
                t0.elapsed() < Duration::from_secs(2),
                "rotation should enqueue a checkpoint that advances the replay cursor"
            );
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }

    #[test]
    fn test_checkpoint_without_new_bytes_skips_io_and_advances_epoch() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(
            dir.path(),
            Config {
                checkpoint_interval: None,
                checkpoint_delta_bytes: None,
                compaction_throughput_bytes_per_sec: 0,
                ..Config::default()
            },
        )?;

        db.stop_compaction();
        let cursor_before = db.inner.index_file.checkpoint_cursor();
        let requested_before = db.inner.checkpoint_state.lock().requested_epoch;
        db.checkpoint()?;
        let state = db.inner.checkpoint_state.lock();
        assert_eq!(state.requested_epoch, requested_before + 1);
        assert_eq!(state.completed_epoch, requested_before + 1);
        assert_eq!(db.inner.index_file.checkpoint_cursor(), cursor_before);

        Ok(())
    }

    #[test]
    fn test_checkpoint_delta_bytes_schedules_background_checkpoint() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(
            dir.path(),
            Config {
                checkpoint_interval: None,
                checkpoint_delta_bytes: Some(512),
                compaction_min_threshold: u32::MAX,
                compaction_throughput_bytes_per_sec: 0,
                ..Config::default()
            },
        )?;

        db.stop_compaction();
        db.set("delta-threshold", vec![b'x'; 1024])?;

        let t0 = Instant::now();
        while db.inner.index_file.checkpoint_cursor() == (0, 0) {
            assert!(
                t0.elapsed() < Duration::from_secs(2),
                "checkpoint_delta_bytes should schedule a background checkpoint"
            );
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }

    #[test]
    fn test_checkpoint_interval_schedules_background_checkpoint() -> Result<()> {
        let dir = tempdir().unwrap();
        let db = CandyStore::open(
            dir.path(),
            Config {
                checkpoint_interval: Some(Duration::from_millis(50)),
                checkpoint_delta_bytes: None,
                compaction_min_threshold: u32::MAX,
                compaction_throughput_bytes_per_sec: 0,
                ..Config::default()
            },
        )?;

        db.stop_compaction();
        db.set("interval-threshold", vec![b'y'; 256])?;

        let t0 = Instant::now();
        while db.inner.index_file.checkpoint_cursor() == (0, 0) {
            assert!(
                t0.elapsed() < Duration::from_secs(2),
                "checkpoint_interval should checkpoint dirty bytes even without explicit requests"
            );
            thread::sleep(Duration::from_millis(10));
        }

        Ok(())
    }
}
