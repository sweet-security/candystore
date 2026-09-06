use std::sync::{Arc, atomic::Ordering};

use crate::{
    data_file::DataFile,
    index_file::EntryPointer,
    internal::{KeyNamespace, data_file_path, invalid_data_error, sync_dir},
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

        let kv = task.source_file.read_kv_into(
            task.entry.file_offset(),
            task.entry.size_hint(),
            read_buf,
        )?;

        let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
            return Err(invalid_data_error("unknown key namespace in data file"));
        };

        self.rewrite_compacted_entry(&task, ns, kv.key(), kv.value(), moved_bytes)
    }

    pub(super) fn compact_files(
        &self,
        candidates: &[(u16, u64)],
        pacer: &mut Pacer,
        #[cfg(windows)] pending_deletions: &mut Vec<std::path::PathBuf>,
    ) -> Result<CompactionOutcome> {
        if candidates.is_empty() {
            return Ok(Self::empty_compaction_outcome());
        }

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

        let removed = {
            let mut files = self.data_files.write();
            sources
                .iter()
                .filter_map(|(file_idx, _)| {
                    files
                        .remove(file_idx)
                        .map(|data_file| (*file_idx, data_file))
                })
                .collect::<Vec<_>>()
        };
        let compacted_files = removed.len() as u64;
        drop(sources);

        let mut reclaimed_bytes = 0u64;
        for (file_idx, data_file) in removed {
            drop(data_file);

            reclaimed_bytes =
                reclaimed_bytes.saturating_add(self.index_file.take_file_waste(file_idx) as u64);

            let file_path = data_file_path(self.base_path.as_path(), file_idx);
            match std::fs::remove_file(&file_path) {
                Ok(()) => sync_dir(self.base_path.as_path())?,
                #[cfg(windows)]
                Err(_) => pending_deletions.push(file_path),
                #[cfg(not(windows))]
                Err(err) => return Err(Error::IOError(err)),
            }
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

    #[cfg(windows)]
    fn retry_pending_deletions(ctx: &StoreInner, pending: &mut Vec<std::path::PathBuf>) {
        let before = pending.len();
        pending.retain(|path| std::fs::remove_file(path).is_err());
        if pending.len() < before {
            let _ = sync_dir(ctx.base_path.as_path());
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

                #[cfg(windows)]
                let mut pending_deletions = Vec::<std::path::PathBuf>::new();
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
                        #[cfg(windows)]
                        Self::retry_pending_deletions(&ctx, &mut pending_deletions);
                        let t0 = std::time::Instant::now();
                        let res = ctx.compact_files(
                            &candidates,
                            &mut pacer,
                            #[cfg(windows)]
                            &mut pending_deletions,
                        );
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
                            }
                        }
                    }
                    #[cfg(windows)]
                    Self::retry_pending_deletions(&ctx, &mut pending_deletions);
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
        let outcome = db.inner.compact_files(
            &[(target_idx, target_ordinal)],
            &mut pacer,
            #[cfg(windows)]
            &mut Vec::new(),
        )?;

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
        let outcome = db.inner.compact_files(
            &targets,
            &mut pacer,
            #[cfg(windows)]
            &mut Vec::new(),
        )?;

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
            db_for_compaction.inner.compact_files(
                &[(target_idx, target_ordinal)],
                &mut pacer,
                #[cfg(windows)]
                &mut Vec::new(),
            )
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
