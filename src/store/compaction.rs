use std::sync::{Arc, atomic::Ordering};

use crate::{
    data_file::DataFile,
    index_file::EntryPointer,
    internal::{KeyNamespace, data_file_path, is_corrupt_entry_error, sync_dir},
    pacer::Pacer,
    types::{Error, Result},
};

use super::{CandyStore, StoreInner};

pub(super) struct CompactionOutcome {
    pub(super) compacted_files: u64,
    pub(super) reclaimed_bytes: u32,
    pub(super) moved_bytes: u32,
}

type CompactionSources = Vec<(u16, Arc<DataFile>)>;

struct CompactionEntry {
    row_idx: usize,
    col: usize,
    entry: EntryPointer,
    source_file: Arc<DataFile>,
}

type CompactionRowSnapshot = Vec<CompactionEntry>;

impl StoreInner {
    fn empty_compaction_outcome() -> CompactionOutcome {
        CompactionOutcome {
            compacted_files: 0,
            reclaimed_bytes: 0,
            moved_bytes: 0,
        }
    }

    fn collect_compaction_sources(
        &self,
        candidates: &[(u16, u64)],
        active_file_idx: u16,
    ) -> CompactionSources {
        let files = self.data_files.read();
        candidates
            .iter()
            .filter_map(|&(file_idx, expected_ordinal)| {
                if file_idx == active_file_idx {
                    return None;
                }

                let data_file = files.get(&file_idx)?.clone();
                if data_file.file_ordinal != expected_ordinal {
                    return None;
                }

                Some((file_idx, data_file))
            })
            .collect()
    }

    fn snapshot_compaction_row(
        &self,
        row_idx: usize,
        sources: &CompactionSources,
    ) -> Option<CompactionRowSnapshot> {
        let rows = self.index_file.rows_table();
        let active_rows = self.index_file.num_rows();
        if row_idx >= active_rows {
            return None;
        }

        let row = rows.row(row_idx);
        let sl = row.split_level.load(std::sync::atomic::Ordering::Acquire);
        if sl == 0 {
            return Some(Vec::new());
        }
        Some(
            row.pointers
                .iter()
                .enumerate()
                .filter_map(|(col, &entry)| {
                    if !entry.is_valid() {
                        return None;
                    }
                    if !row.entry_belongs_to_row(col, row_idx, sl) {
                        return None;
                    }
                    let (_, source_file) =
                        sources.iter().find(|(idx, _)| *idx == entry.file_idx())?;
                    Some(CompactionEntry {
                        row_idx,
                        col,
                        entry,
                        source_file: source_file.clone(),
                    })
                })
                .collect(),
        )
    }

    fn rewrite_compacted_entry(
        &self,
        task: &CompactionEntry,
        ns: KeyNamespace,
        key: &[u8],
        value: &[u8],
        moved_bytes: &mut u64,
    ) -> Result<()> {
        let mut rotate_idx_req = None;
        loop {
            if let Some(rotate_idx) = rotate_idx_req.take() {
                self._rotate_data_file(rotate_idx)?;
            }

            let rows = self.index_file.rows_table();
            let active_rows = self.index_file.num_rows();
            if task.row_idx >= active_rows {
                return Ok(());
            }

            let mut row = rows.row_mut(task.row_idx);
            if row.pointers[task.col] != task.entry {
                return Ok(());
            }

            let active_idx = self.active_file_idx.load(Ordering::Acquire);
            let active_file = self
                .data_files
                .read()
                .get(&active_idx)
                .cloned()
                .ok_or(Error::MissingDataFile(active_idx))?;

            match active_file.append_kv(
                crate::internal::EntryType::Update,
                ns,
                key,
                value,
                row.shard_idx,
                &self.inflight_tracker,
            ) {
                Ok((file_off, size, inflight_guard)) => {
                    self.record_write(file_off, size as u64);
                    *moved_bytes = moved_bytes.saturating_add(size as u64);
                    row.replace_pointer(
                        task.col,
                        EntryPointer::new(
                            active_idx,
                            file_off,
                            size,
                            task.entry.masked_row_selector(),
                        ),
                    );
                    inflight_guard.complete();
                    return Ok(());
                }
                Err(Error::RotateDataFile(rotate_idx)) => {
                    drop(row);
                    rotate_idx_req = Some(rotate_idx);
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn compact_snapshot_entry(
        &self,
        task: CompactionEntry,
        pacer: &mut Pacer,
        read_buf: &mut Vec<u8>,
        moved_bytes: &mut u64,
    ) -> Result<()> {
        self.record_read(task.entry.size_hint() as u64);
        pacer.consume(task.entry.size_hint() as u64);

        let kv = match task.source_file.read_kv_into(
            task.entry.file_offset(),
            task.entry.size_hint(),
            read_buf,
        ) {
            Ok(kv) => kv,
            Err(err) if is_corrupt_entry_error(&err) => {
                self.drop_corrupt_compaction_entry(&task);
                return Ok(());
            }
            Err(err) => {
                if self.read_failure_limit_reached(&task) {
                    self.drop_corrupt_compaction_entry(&task);
                    return Ok(());
                }
                return Err(err);
            }
        };

        let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
            self.drop_corrupt_compaction_entry(&task);
            return Ok(());
        };

        self.rewrite_compacted_entry(&task, ns, kv.key(), kv.value(), moved_bytes)
    }

    /// Counts a failed read per entry (not per file), so a disk-wide outage loses at most one entry per limit.
    fn read_failure_limit_reached(&self, task: &CompactionEntry) -> bool {
        let mut failures = self.compaction_read_failures.lock();
        let count = failures
            .entry((
                task.source_file.file_idx,
                task.source_file.file_ordinal,
                task.entry.file_offset(),
            ))
            .or_insert(0);
        if *count >= self.config.compaction_max_read_failures {
            return true;
        }
        *count += 1;
        false
    }

    fn drop_corrupt_compaction_entry(&self, task: &CompactionEntry) {
        let rows = self.index_file.rows_table();
        if task.row_idx >= self.index_file.num_rows() {
            return;
        }
        let mut row = rows.row_mut(task.row_idx);
        if row.pointers[task.col] == task.entry {
            self.drop_corrupt_entry(&mut row, task.col);
        }
    }

    pub(super) fn compact_files(
        &self,
        candidates: &[(u16, u64)],
        pacer: &mut Pacer,
    ) -> Result<CompactionOutcome> {
        if candidates.is_empty() {
            return Ok(Self::empty_compaction_outcome());
        }

        let _compaction_guard = self.compaction_lock.lock();
        let active_file_idx = self.active_file_idx.load(Ordering::Acquire);
        let sources = self.collect_compaction_sources(candidates, active_file_idx);

        if sources.is_empty() {
            return Ok(Self::empty_compaction_outcome());
        }

        let mut moved_bytes = 0u64;
        let mut read_buf = Vec::new();

        let mut row_idx = 0;
        loop {
            if self.compaction_shutting_down.load(Ordering::Acquire) {
                return Ok(Self::empty_compaction_outcome());
            }

            let Some(snapshot) = self.snapshot_compaction_row(row_idx, &sources) else {
                break;
            };

            for task in snapshot {
                self.compact_snapshot_entry(task, pacer, &mut read_buf, &mut moved_bytes)?;
            }

            row_idx += 1;
        }

        // Block pointer publication and rotation while making every possible
        // compaction destination durable. Rewrites may span multiple active
        // files, so syncing only the final active file is insufficient.
        let rows_table = self.index_file.rows_table_mut();
        let _rotation_lock = self.rotation_lock.lock();
        {
            let files = self.data_files.read();
            for data_file in files.values() {
                data_file.sync_to_current()?;
            }
        }
        self.index_file.sync_rows(rows_table)?;

        let mut compacted_files = 0u64;
        let mut reclaimed_bytes = 0u64;
        for (file_idx, data_file) in sources {
            let file_path = data_file_path(self.base_path.as_path(), file_idx);
            match std::fs::remove_file(&file_path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(Error::IOError(err)),
            }
            sync_dir(self.base_path.as_path())?;

            self.data_files.write().remove(&file_idx);
            self.compaction_read_failures
                .lock()
                .retain(|&(idx, ordinal, _), _| {
                    (idx, ordinal) != (file_idx, data_file.file_ordinal)
                });
            reclaimed_bytes =
                reclaimed_bytes.saturating_add(self.index_file.take_file_waste(file_idx) as u64);
            compacted_files += 1;
        }

        self.index_file.flush_header()?;

        Ok(CompactionOutcome {
            compacted_files,
            reclaimed_bytes: reclaimed_bytes.min(u64::from(u32::MAX)) as u32,
            moved_bytes: moved_bytes.min(u64::from(u32::MAX)) as u32,
        })
    }
}

impl CandyStore {
    pub(super) fn stop_compaction(&self) {
        self.inner
            .compaction_shutting_down
            .store(true, Ordering::Release);
        {
            let mut state = self.inner.compaction_state.lock();
            state.wake_requested = true;
            self.inner.compaction_condvar.notify_all();
        }
        if let Some(thd) = self.compaction_thd.lock().take() {
            let _ = thd.join();
        }
    }

    pub(super) fn start_compaction(&self) {
        let mut compaction_thd = self.compaction_thd.lock();
        if compaction_thd.is_some() {
            return;
        }

        self.inner
            .compaction_shutting_down
            .store(false, Ordering::Release);
        let ctx = Arc::clone(&self.inner);
        let thd = std::thread::Builder::new()
            .name("candy_compact".into())
            .spawn(move || {
                if ctx.config.compaction_throughput_bytes_per_sec == 0 {
                    // Compaction disabled — park until shutdown.
                    let mut state = ctx.compaction_state.lock();
                    while !ctx.compaction_shutting_down.load(Ordering::Acquire) {
                        ctx.compaction_condvar.wait(&mut state);
                    }
                    return;
                }

                let throughput_bytes_per_sec =
                    ctx.config.compaction_throughput_bytes_per_sec as u64;
                let tokens_per_unit = (throughput_bytes_per_sec / 10).max(1);
                let burst_size = tokens_per_unit.saturating_mul(2);
                let mut pacer = Pacer::new(
                    tokens_per_unit,
                    std::time::Duration::from_millis(100),
                    burst_size,
                );

                loop {
                    {
                        let mut state = ctx.compaction_state.lock();
                        while !state.wake_requested
                            && !ctx.compaction_shutting_down.load(Ordering::Acquire)
                        {
                            ctx.compaction_condvar.wait(&mut state);
                        }

                        if ctx.compaction_shutting_down.load(Ordering::Acquire) {
                            break;
                        }

                        state.wake_requested = false;
                    }
                    loop {
                        let candidates = ctx.next_compaction_candidates(4);
                        if candidates.is_empty() {
                            break;
                        }
                        if ctx.compaction_shutting_down.load(Ordering::Acquire) {
                            return;
                        }
                        let t0 = std::time::Instant::now();
                        let res = ctx.compact_files(&candidates, &mut pacer);
                        let compaction_millis =
                            u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
                        match res {
                            Ok(outcome) => {
                                ctx.stats
                                    .num_compactions
                                    .fetch_add(outcome.compacted_files, Ordering::Relaxed);
                                ctx.stats
                                    .last_compaction_dur_ms
                                    .store(compaction_millis, Ordering::Relaxed);
                                ctx.stats
                                    .last_compaction_reclaimed_bytes
                                    .store(outcome.reclaimed_bytes, Ordering::Relaxed);
                                ctx.stats
                                    .last_compaction_moved_bytes
                                    .store(outcome.moved_bytes, Ordering::Relaxed);
                            }
                            Err(_e) => {
                                ctx.stats.compaction_errors.fetch_add(1, Ordering::Relaxed);
                                // retrying immediately would just spin; wait for the next wake-up
                                break;
                            }
                        }
                    }
                }
            })
            .unwrap();

        *compaction_thd = Some(thd);
        self.inner.signal_compaction_scan();
    }
}

impl Drop for CandyStore {
    fn drop(&mut self) {
        self.stop_compaction();

        let should_checkpoint = self.allow_clean_shutdown.load(Ordering::Relaxed);
        self.stop_checkpoint_worker();

        if !should_checkpoint {
            return;
        }

        let _ = self.inner.perform_checkpoint_with_logical_locks();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{mem::size_of, sync::Arc, thread, time::Duration};

    use crate::index_file::RowLayout;
    use crate::internal::{HashCoord, KeyNamespace, MIN_SPLIT_LEVEL, ROW_WIDTH};
    use crate::{CandyStore, Config};

    fn colliding_user_keys(hash_key: (u64, u64), row_idx: usize, count: usize) -> Vec<String> {
        let mut keys = Vec::with_capacity(count);
        let mut candidate = 0u64;
        while keys.len() < count {
            let key = format!("compaction-split-{candidate:06}");
            let hc = HashCoord::new(KeyNamespace::User, key.as_bytes(), hash_key);
            if hc.row_index(MIN_SPLIT_LEVEL as u64) == row_idx {
                keys.push(key);
            }
            candidate += 1;
        }
        keys
    }

    fn count_live_entries_in_file(store: &CandyStore, file_idx: u16) -> u64 {
        let rows = store.inner.index_file.rows_table();
        let num_rows = store.inner.index_file.num_rows();
        let mut count = 0u64;

        for row_idx in 0..num_rows {
            let row = rows.row(row_idx);
            for entry in row.pointers.iter() {
                if entry.is_valid() && entry.file_idx() == file_idx {
                    count += 1;
                }
            }
        }

        count
    }

    #[test]
    fn test_snapshot_compaction_row_skips_unpublished_rows() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(dir.path(), Config::default())?;
        db.stop_compaction();

        let keys = colliding_user_keys(db.inner.config.hash_key, 0, ROW_WIDTH + 1);
        for (idx, key) in keys.iter().enumerate() {
            db.set(key, format!("value{idx:04}"))?;
        }

        assert_eq!(
            db.inner.index_file.num_rows(),
            1usize << (MIN_SPLIT_LEVEL + 1)
        );

        let (sig, ptr) = {
            let rows = db.inner.index_file.rows_table();
            let row = rows.row(0);
            row.signatures
                .iter()
                .enumerate()
                .find_map(|(col, &sig)| {
                    let ptr = row.pointers[col];
                    (sig != HashCoord::INVALID_SIG && ptr.is_valid()).then_some((sig, ptr))
                })
                .expect("split row should retain at least one live entry")
        };

        let unpublished_row_idx = (1usize << MIN_SPLIT_LEVEL) + 1;
        {
            let mut rows = db.inner.index_file.rows_table_mut();
            let row = unsafe {
                &mut *(rows
                    .row_guard
                    .as_mut_ptr()
                    .add(unpublished_row_idx * size_of::<RowLayout>())
                    as *mut RowLayout)
            };
            assert_eq!(row.split_level.load(Ordering::Acquire), 0);
            row.signatures[0] = sig;
            row.pointers[0] = ptr;
        }

        let source_file = db
            .inner
            .data_files
            .read()
            .get(&ptr.file_idx())
            .cloned()
            .ok_or(Error::MissingDataFile(ptr.file_idx()))?;
        let sources = vec![(ptr.file_idx(), source_file)];
        let snapshot = db
            .inner
            .snapshot_compaction_row(unpublished_row_idx, &sources)
            .expect("row is inside active rows");
        assert!(
            snapshot.is_empty(),
            "compaction must ignore split_level=0 rows even if they contain stale pointers"
        );

        Ok(())
    }

    #[test]
    fn test_compaction_drops_corrupt_entries() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 8192,
                compaction_min_threshold: u32::MAX,
                ..Default::default()
            },
        )?;
        db.stop_compaction();

        let mut num_keys = 0u32;
        while db.inner.data_files.read().len() == 1 {
            db.set(format!("key-{num_keys}"), format!("value-{num_keys}"))?;
            num_keys += 1;
        }

        let victim = crate::store::tests::corrupt_entry(&db, b"key-0");
        let target_idx = victim.file_idx();
        assert_ne!(target_idx, db.inner.active_file_idx.load(Ordering::Acquire));
        let target_ordinal = db.inner.data_files.read()[&target_idx].file_ordinal;
        let items_before = db.num_items();

        let mut pacer = Pacer::new(u64::MAX / 4, Duration::from_secs(1), u64::MAX / 4);
        db.inner
            .compaction_shutting_down
            .store(false, Ordering::Release);
        let outcome = db
            .inner
            .compact_files(&[(target_idx, target_ordinal)], &mut pacer)?;

        assert_eq!(outcome.compacted_files, 1);
        assert!(!db.inner.data_files.read().contains_key(&target_idx));
        assert_eq!(db.stats().num_corrupt_entries_dropped, 1);
        assert_eq!(db.num_items(), items_before - 1);
        assert_eq!(db.get("key-0")?, None);
        for idx in 1..num_keys {
            assert_eq!(
                db.get(format!("key-{idx}"))?,
                Some(format!("value-{idx}").into_bytes())
            );
        }

        Ok(())
    }

    #[test]
    fn test_background_compaction_reclaims_file_with_corrupt_entry() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 8192,
                compaction_min_threshold: 0,
                checkpoint_interval: None,
                checkpoint_delta_bytes: None,
                ..Default::default()
            },
        )?;

        // no overwrites yet, so no file has waste and compaction has nothing to pick
        let mut num_keys = 0u32;
        while db.inner.data_files.read().len() == 1 {
            db.set(format!("key-{num_keys}"), format!("value-{num_keys}"))?;
            num_keys += 1;
        }

        let victim = crate::store::tests::corrupt_entry(&db, b"key-0");
        let target_idx = victim.file_idx();
        let target_ordinal = db.inner.data_files.read()[&target_idx].file_ordinal;

        db.set("key-1", "value-1-updated")?;
        db.checkpoint()?;

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while db
            .inner
            .data_files
            .read()
            .get(&target_idx)
            .is_some_and(|file| file.file_ordinal == target_ordinal)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "compaction never reclaimed the file with the corrupt entry"
            );
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(db.compaction_errors(), 0);
        assert_eq!(db.stats().num_corrupt_entries_dropped, 1);
        assert_eq!(db.get("key-0")?, None);
        assert_eq!(db.get("key-1")?, Some(b"value-1-updated".to_vec()));
        for idx in 2..num_keys {
            assert_eq!(
                db.get(format!("key-{idx}"))?,
                Some(format!("value-{idx}").into_bytes())
            );
        }
        assert_eq!(db.num_items(), num_keys as usize - 1);
        assert_eq!(db.iter_items().count(), db.num_items());

        Ok(())
    }

    #[test]
    fn test_compaction_survives_concurrent_shrink() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let config = Config {
            initial_capacity: 16,
            max_data_file_size: 8192,
            checkpoint_interval: None,
            checkpoint_delta_bytes: None,
            ..Default::default()
        };
        let db = Arc::new(CandyStore::open(dir.path(), config)?);
        db.stop_compaction();
        let key = (0u64..)
            .map(|number| format!("shrink-compaction-{number}"))
            .find(|key| {
                HashCoord::new(KeyNamespace::User, key.as_bytes(), db.inner.config.hash_key)
                    .row_index(MIN_SPLIT_LEVEL as u64 + 1)
                    == 1usize << MIN_SPLIT_LEVEL
            })
            .unwrap();
        let coord = HashCoord::new(KeyNamespace::User, key.as_bytes(), db.inner.config.hash_key);
        db.set(&key, "healthy")?;
        db.inner
            ._split_row(coord, MIN_SPLIT_LEVEL as u64, MIN_SPLIT_LEVEL as u64)?;
        let target_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        let target_ordinal = db.inner.data_files.read()[&target_idx].file_ordinal;
        db.inner._rotate_data_file(target_idx)?;
        db.checkpoint()?;

        let before_reads = db.stats().num_read_ops;
        let worker_db = Arc::clone(&db);
        db.inner
            .compaction_shutting_down
            .store(false, Ordering::Release);
        let worker = thread::spawn(move || {
            let mut pacer = Pacer::new(256, Duration::from_secs(1), 256);
            pacer.consume(256);
            worker_db
                .inner
                .compact_files(&[(target_idx, target_ordinal)], &mut pacer)
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while db.stats().num_read_ops == before_reads {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        assert_eq!(db.shrink_to_fit_blocking(0.0)?, 1usize << MIN_SPLIT_LEVEL);
        assert_eq!(worker.join().unwrap()?.compacted_files, 1);
        assert_eq!(db.get(&key)?, Some(b"healthy".to_vec()));
        drop(db);
        let db = CandyStore::open(dir.path(), config)?;
        assert_eq!(db.get(&key)?, Some(b"healthy".to_vec()));
        Ok(())
    }

    #[test]
    fn test_compaction_reads_only_live_entries_for_target_file() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 8192,
                compaction_min_threshold: u32::MAX,
                ..Default::default()
            },
        )?;
        db.stop_compaction();

        for idx in 0..24 {
            db.set("hot", format!("hot-value-{idx:02}-{}", "x".repeat(48)))?;
        }

        let mut filler_idx = 0u64;
        while db.inner.data_files.read().len() == 1 {
            db.set(
                format!("filler-{filler_idx}"),
                format!("filler-value-{}", "y".repeat(48)),
            )?;
            filler_idx += 1;
        }

        let active_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        let (target_idx, target_ordinal) = {
            let files = db.inner.data_files.read();
            let (&target_idx, target_file) = files
                .iter()
                .find(|(idx, _)| **idx != active_idx)
                .expect("expected a non-active file to compact");
            (target_idx, target_file.file_ordinal)
        };

        let live_entries = count_live_entries_in_file(&db, target_idx);
        assert!(
            live_entries > 0,
            "expected live entries in the compacted file"
        );

        let before_read_ops = db.stats().num_read_ops;
        let mut pacer = Pacer::new(u64::MAX / 4, Duration::from_secs(1), u64::MAX / 4);
        db.inner
            .compaction_shutting_down
            .store(false, Ordering::Release);
        let outcome = db
            .inner
            .compact_files(&[(target_idx, target_ordinal)], &mut pacer)?;

        assert_eq!(outcome.compacted_files, 1);
        assert_eq!(db.stats().num_read_ops - before_read_ops, live_entries);
        assert_eq!(count_live_entries_in_file(&db, target_idx), 0);

        Ok(())
    }

    #[test]
    fn test_compaction_batch_reads_live_entries_for_all_target_files() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 2048,
                compaction_min_threshold: u32::MAX,
                ..Default::default()
            },
        )?;
        db.stop_compaction();

        for idx in 0..32 {
            db.set(format!("hot-{idx}"), format!("seed-{}", "x".repeat(48)))?;
        }

        while db.inner.data_files.read().len() < 5 {
            let idx = db.stats().num_write_ops;
            db.set(
                format!("roll-{idx}"),
                format!("roll-value-{}", "y".repeat(48)),
            )?;
        }

        let pre_compaction_active_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        db.inner._rotate_data_file(pre_compaction_active_idx)?;

        let active_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        let targets = {
            let files = db.inner.data_files.read();
            let mut target_files = files
                .iter()
                .filter(|(file_idx, _)| **file_idx != active_idx)
                .map(|(&file_idx, data_file)| (file_idx, data_file.file_ordinal))
                .collect::<Vec<_>>();
            target_files.sort_by_key(|(file_idx, _)| *file_idx);
            target_files.truncate(4);
            target_files
        };
        assert_eq!(targets.len(), 4);

        let live_entries = targets
            .iter()
            .map(|(file_idx, _)| count_live_entries_in_file(&db, *file_idx))
            .sum::<u64>();
        assert!(live_entries > 0);

        let before_read_ops = db.stats().num_read_ops;
        let mut pacer = Pacer::new(u64::MAX / 4, Duration::from_secs(1), u64::MAX / 4);
        db.inner
            .compaction_shutting_down
            .store(false, Ordering::Release);
        let outcome = db.inner.compact_files(&targets, &mut pacer)?;

        assert_eq!(outcome.compacted_files, 4);
        assert_eq!(db.stats().num_read_ops - before_read_ops, live_entries);
        for (file_idx, _) in targets {
            assert_eq!(count_live_entries_in_file(&db, file_idx), 0);
        }
        for data_file in db.inner.data_files.read().values() {
            assert!(!data_file.is_dirty());
        }

        Ok(())
    }

    #[test]
    fn test_compaction_retries_after_transient_read_error() -> Result<()> {
        for periodic in [false, true] {
            let dir = tempfile::tempdir().map_err(Error::IOError)?;
            let db = CandyStore::open(
                dir.path(),
                Config {
                    max_data_file_size: 8192,
                    compaction_min_threshold: 0,
                    checkpoint_interval: periodic.then_some(Duration::from_millis(20)),
                    checkpoint_delta_bytes: None,
                    compaction_max_read_failures: u32::MAX,
                    ..Default::default()
                },
            )?;
            db.stop_compaction();
            db.set("key", "old")?;
            db.set("key", "new")?;
            let target_idx = db.inner.active_file_idx.load(Ordering::Acquire);
            db.inner._rotate_data_file(target_idx)?;
            db.checkpoint()?;
            let mut unreadable =
                DataFile::open(dir.path(), db.inner.config.clone(), target_idx, false)?;
            unreadable.file = std::fs::File::options()
                .write(true)
                .open(data_file_path(dir.path(), target_idx))
                .map_err(Error::IOError)?;
            let original = db
                .inner
                .data_files
                .write()
                .insert(target_idx, Arc::new(unreadable))
                .unwrap();
            db.start_compaction();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while db.compaction_errors() == 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "expected an injected read error"
                );
                thread::sleep(Duration::from_millis(1));
            }
            db.inner.data_files.write().insert(target_idx, original);
            if !periodic {
                db.checkpoint()?;
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while db.inner.data_files.read().contains_key(&target_idx) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "idle checkpoint did not retry compaction; periodic={periodic}"
                );
                thread::sleep(Duration::from_millis(1));
            }
            db.stop_compaction();
            assert_eq!(db.stats().num_compactions, 1);
            assert_eq!(db.stats().num_corrupt_entries_dropped, 0);
            assert_eq!(db.get("key")?, Some(b"new".to_vec()));
            assert_eq!(db.num_items(), 1);
        }
        Ok(())
    }

    #[test]
    fn test_compaction_drops_permanently_unreadable_entries() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 8192,
                compaction_min_threshold: 0,
                compaction_max_read_failures: 2,
                checkpoint_interval: Some(Duration::from_millis(20)),
                checkpoint_delta_bytes: None,
                ..Default::default()
            },
        )?;
        db.stop_compaction();

        let mut sources = Vec::new();
        for key in ["broken", "healthy"] {
            db.set(key, "old")?;
            db.set(key, "new")?;
            let file_idx = db.inner.active_file_idx.load(Ordering::Acquire);
            db.inner._rotate_data_file(file_idx)?;
            sources.push(file_idx);
        }
        db.checkpoint()?;

        let broken_idx = sources[0];
        let mut unreadable =
            DataFile::open(dir.path(), db.inner.config.clone(), broken_idx, false)?;
        unreadable.file = std::fs::File::options()
            .write(true)
            .open(data_file_path(dir.path(), broken_idx))
            .map_err(Error::IOError)?;
        db.inner
            .data_files
            .write()
            .insert(broken_idx, Arc::new(unreadable));
        db.start_compaction();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sources
            .iter()
            .any(|idx| db.inner.data_files.read().contains_key(idx))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "compaction stayed stuck on a permanently unreadable file"
            );
            thread::sleep(Duration::from_millis(1));
        }
        db.stop_compaction();

        assert_eq!(db.compaction_errors(), 2);
        assert_eq!(db.stats().num_corrupt_entries_dropped, 1);
        assert!(db.inner.compaction_read_failures.lock().is_empty());
        assert_eq!(db.get("broken")?, None);
        assert_eq!(db.get("healthy")?, Some(b"new".to_vec()));
        assert_eq!(db.num_items(), 1);
        Ok(())
    }

    #[test]
    fn test_read_outage_drops_at_most_one_entry_per_limit() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = CandyStore::open(
            dir.path(),
            Config {
                max_data_file_size: 8192,
                compaction_max_read_failures: 2,
                checkpoint_interval: None,
                checkpoint_delta_bytes: None,
                ..Default::default()
            },
        )?;
        db.stop_compaction();

        let keys = ["a", "b", "c"];
        for key in keys {
            db.set(key, "old")?;
            db.set(key, "new")?;
        }
        let source_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        let source_ordinal = db.inner.data_files.read()[&source_idx].file_ordinal;
        db.inner._rotate_data_file(source_idx)?;
        db.checkpoint()?;

        let mut unreadable =
            DataFile::open(dir.path(), db.inner.config.clone(), source_idx, false)?;
        unreadable.file = std::fs::File::options()
            .write(true)
            .open(data_file_path(dir.path(), source_idx))
            .map_err(Error::IOError)?;
        let original = db
            .inner
            .data_files
            .write()
            .insert(source_idx, Arc::new(unreadable))
            .unwrap();

        db.inner
            .compaction_shutting_down
            .store(false, Ordering::Release);
        let mut pacer = Pacer::new(u64::MAX / 4, Duration::from_secs(1), u64::MAX / 4);
        // the limit is spent on the first entry, then the next entry starts its own count
        for _ in 0..3 {
            assert!(
                db.inner
                    .compact_files(&[(source_idx, source_ordinal)], &mut pacer)
                    .is_err()
            );
        }
        assert_eq!(db.stats().num_corrupt_entries_dropped, 1);

        db.inner.data_files.write().insert(source_idx, original);
        let outcome = db
            .inner
            .compact_files(&[(source_idx, source_ordinal)], &mut pacer)?;
        assert_eq!(outcome.compacted_files, 1);
        assert_eq!(db.stats().num_corrupt_entries_dropped, 1);
        assert!(db.inner.compaction_read_failures.lock().is_empty());
        let survivors = keys
            .iter()
            .filter(|key| db.get(key).unwrap() == Some(b"new".to_vec()))
            .count();
        assert_eq!(survivors, 2);
        assert_eq!(db.num_items(), 2);
        Ok(())
    }

    #[test]
    fn test_compaction_retries_failed_file_deletions() -> Result<()> {
        for failed_position in [0, 1] {
            for already_deleted in [false, true] {
                let dir = tempfile::tempdir().map_err(Error::IOError)?;
                let db = CandyStore::open(
                    dir.path(),
                    Config {
                        max_data_file_size: 8192,
                        checkpoint_interval: None,
                        checkpoint_delta_bytes: None,
                        compaction_min_threshold: 0,
                        ..Default::default()
                    },
                )?;
                db.stop_compaction();
                let mut sources = Vec::new();
                for key in ["first", "second"] {
                    db.set(key, "old")?;
                    db.set(key, "new")?;
                    let file_idx = db.inner.active_file_idx.load(Ordering::Acquire);
                    let ordinal = db.inner.data_files.read()[&file_idx].file_ordinal;
                    sources.push((file_idx, ordinal));
                    db.inner._rotate_data_file(file_idx)?;
                }
                db.checkpoint()?;
                let path = data_file_path(dir.path(), sources[failed_position].0);
                let backup = dir.path().join("source-backup");
                std::fs::rename(&path, &backup).map_err(Error::IOError)?;
                std::fs::create_dir(&path).map_err(Error::IOError)?;
                db.inner
                    .compaction_shutting_down
                    .store(false, Ordering::Release);
                let mut pacer = Pacer::new(u64::MAX / 4, Duration::from_secs(1), u64::MAX / 4);
                let result = db.inner.compact_files(&sources, &mut pacer);
                std::fs::remove_dir(&path).map_err(Error::IOError)?;
                if already_deleted {
                    std::fs::remove_file(&backup).map_err(Error::IOError)?;
                } else {
                    std::fs::rename(&backup, &path).map_err(Error::IOError)?;
                }
                assert!(matches!(result, Err(Error::IOError(_))));
                let candidates = db.inner.next_compaction_candidates(4);
                for (position, &(file_idx, ordinal)) in sources.iter().enumerate() {
                    if position < failed_position {
                        assert!(!db.inner.data_files.read().contains_key(&file_idx));
                    } else {
                        assert!(db.inner.data_files.read().contains_key(&file_idx));
                        assert!(db.inner.index_file.file_waste(file_idx) > 0);
                        assert!(candidates.contains(&(file_idx, ordinal)));
                    }
                }
                db.start_compaction();
                db.checkpoint()?;
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while sources
                    .iter()
                    .any(|(file_idx, _)| db.inner.data_files.read().contains_key(file_idx))
                {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "failed file deletion was not retried"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                db.stop_compaction();
                for (file_idx, _) in sources {
                    assert!(!data_file_path(dir.path(), file_idx).exists());
                    assert_eq!(db.inner.index_file.file_waste(file_idx), 0);
                }
                assert_eq!(db.get("first")?, Some(b"new".to_vec()));
                assert_eq!(db.get("second")?, Some(b"new".to_vec()));
                assert_eq!(db.num_items(), 2);
            }
        }
        Ok(())
    }

    #[test]
    fn test_compaction_allows_concurrent_index_growth() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let db = Arc::new(CandyStore::open(
            dir.path(),
            Config {
                initial_capacity: 16,
                remap_scaler: 1,
                max_data_file_size: 64 * 1024,
                compaction_min_threshold: u32::MAX,
                ..Default::default()
            },
        )?);
        db.stop_compaction();

        let mut expected = Vec::new();
        for idx in 0..64u32 {
            let key = format!("seed-{idx:04}");
            let value = format!("seed-value-{idx:04}-{}", "x".repeat(768));
            db.set(&key, &value)?;
            expected.push((key, value.into_bytes()));
        }

        let target_idx = db.inner.active_file_idx.load(Ordering::Acquire);
        let target_ordinal = {
            let files = db.inner.data_files.read();
            files
                .get(&target_idx)
                .expect("target file should exist before rotation")
                .file_ordinal
        };
        db.inner._rotate_data_file(target_idx)?;

        let live_entries = count_live_entries_in_file(&db, target_idx);
        assert!(
            live_entries >= 8,
            "expected a file with enough live entries to slow compaction"
        );

        let rows_before = db.inner.index_file.num_rows();
        let db_for_compaction = Arc::clone(&db);
        let compaction_handle = thread::spawn(move || {
            let mut pacer = Pacer::new(256, Duration::from_millis(10), 256);
            db_for_compaction
                .inner
                .compaction_shutting_down
                .store(false, Ordering::Release);
            db_for_compaction
                .inner
                .compact_files(&[(target_idx, target_ordinal)], &mut pacer)
        });

        let mut grew = false;
        for idx in 0..20_000u32 {
            let key = format!("grow-{idx:04}");
            let value = format!("grow-value-{idx:04}-{}", "y".repeat(96));
            db.set(&key, &value)?;
            expected.push((key, value.into_bytes()));

            if db.inner.index_file.num_rows() > rows_before {
                grew = true;
                break;
            }
        }

        let outcome = compaction_handle
            .join()
            .expect("compaction thread panicked")?;
        assert_eq!(outcome.compacted_files, 1);
        assert!(
            grew,
            "expected concurrent writes to force index growth during compaction"
        );
        assert!(db.inner.index_file.num_rows() > rows_before);
        assert_eq!(count_live_entries_in_file(&db, target_idx), 0);

        for (key, value) in expected {
            assert_eq!(
                db.get(&key)?,
                Some(value),
                "key {key} should remain readable"
            );
        }

        Ok(())
    }
}
