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

impl StoreInner {
    pub(super) fn compact_files(
        &self,
        candidates: &[(u16, u64)],
        pacer: &mut Pacer,
        #[cfg(windows)] pending_deletions: &mut Vec<std::path::PathBuf>,
    ) -> Result<CompactionOutcome> {
        if candidates.is_empty() {
            return Ok(CompactionOutcome {
                compacted_files: 0,
                reclaimed_bytes: 0,
                moved_bytes: 0,
            });
        }

        let active_file_idx = self.active_file_idx.load(Ordering::Acquire);
        let files = self.data_files.read();
        let sources = candidates
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
            .collect::<Vec<_>>();
        drop(files);

        if sources.is_empty() {
            return Ok(CompactionOutcome {
                compacted_files: 0,
                reclaimed_bytes: 0,
                moved_bytes: 0,
            });
        }

        let mut moved_bytes = 0u64;
        let mut read_buf = Vec::new();

        let mut row_idx = 0;
        loop {
            if self.shutting_down.load(Ordering::Acquire) {
                return Ok(CompactionOutcome {
                    compacted_files: 0,
                    reclaimed_bytes: 0,
                    moved_bytes: 0,
                });
            }

            // Snapshot one row at a time, then drop the rows table read lock before any I/O.
            let snapshot: Vec<(usize, EntryPointer, Arc<DataFile>)> = {
                let rows = self.index_file.rows_table();
                let active_rows = self.index_file.num_rows();
                if row_idx >= active_rows {
                    break;
                }

                let row = rows.row(row_idx);
                row.pointers
                    .iter()
                    .enumerate()
                    .filter_map(|(col, &entry)| {
                        if !entry.is_valid() {
                            return None;
                        }
                        let (_, source_file) =
                            sources.iter().find(|(idx, _)| *idx == entry.file_idx())?;
                        Some((col, entry, source_file.clone()))
                    })
                    .collect()
            };

            for (col, entry, source_file) in &snapshot {
                self.record_read(entry.size_hint() as u64);
                pacer.consume(entry.size_hint() as u64);

                let kv = source_file.read_kv_into(
                    entry.file_offset(),
                    entry.size_hint(),
                    &mut read_buf,
                )?;

                let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
                    return Err(invalid_data_error("unknown key namespace in data file"));
                };

                // re-acquire the write lock and verify the pointer hasn't been moved
                // by a concurrent set/remove before appending + replacing
                let mut rotate_idx_req = None;
                loop {
                    if let Some(rotate_idx) = rotate_idx_req.take() {
                        self._rotate_data_file(rotate_idx)?;
                    }

                    let rows = self.index_file.rows_table();
                    let active_rows = self.index_file.num_rows();
                    if row_idx >= active_rows {
                        break;
                    }

                    let mut row = rows.row_mut(row_idx);
                    if row.pointers[*col] != *entry {
                        // a concurrent op already moved/removed this entry -- skip it
                        break;
                    }

                    let active_idx = self.active_file_idx.load(Ordering::Acquire);
                    let files = self.data_files.read();
                    let active_file = files
                        .get(&active_idx)
                        .ok_or(Error::MissingDataFile(active_idx))?;

                    match active_file.append_kv(ns, kv.key(), kv.value()) {
                        Ok((file_off, size)) => {
                            drop(files);
                            self.record_write(size as u64);
                            moved_bytes = moved_bytes.saturating_add(size as u64);
                            row.replace_pointer(
                                *col,
                                EntryPointer::new(
                                    active_idx,
                                    file_off,
                                    size,
                                    entry.masked_row_selector(),
                                ),
                            );
                            break;
                        }
                        Err(Error::RotateDataFile(rotate_idx)) => {
                            drop(files);
                            drop(row);
                            rotate_idx_req = Some(rotate_idx);
                        }
                        Err(err) => {
                            drop(files);
                            return Err(err);
                        }
                    }
                }
            }

            row_idx += 1;
        }

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

        self.index_file
            .header_ref()
            .reclaimed_bytes
            .fetch_add(reclaimed_bytes, Ordering::Relaxed);

        Ok(CompactionOutcome {
            compacted_files,
            reclaimed_bytes: reclaimed_bytes.min(u64::from(u32::MAX)) as u32,
            moved_bytes: moved_bytes.min(u64::from(u32::MAX)) as u32,
        })
    }
}

impl CandyStore {
    pub(super) fn stop_compaction(&self) {
        self.inner.shutting_down.store(true, Ordering::Release);
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

        self.inner.shutting_down.store(false, Ordering::Release);
        let ctx = Arc::clone(&self.inner);
        let thd = std::thread::spawn(move || {
            if ctx.config.compaction_throughput_bytes_per_sec == 0 {
                // Compaction disabled — park until shutdown.
                let mut state = ctx.compaction_state.lock();
                while !ctx.shutting_down.load(Ordering::Acquire) {
                    ctx.compaction_condvar.wait(&mut state);
                }
                return;
            }

            let throughput_bytes_per_sec = ctx.config.compaction_throughput_bytes_per_sec as u64;
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
                    while !state.wake_requested && !ctx.shutting_down.load(Ordering::Acquire) {
                        ctx.compaction_condvar.wait(&mut state);
                    }

                    if ctx.shutting_down.load(Ordering::Acquire) {
                        break;
                    }

                    state.wake_requested = false;
                }
                loop {
                    let candidates = ctx.next_compaction_candidates(4);
                    if candidates.is_empty() {
                        break;
                    }
                    if ctx.shutting_down.load(Ordering::Acquire) {
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
                    ctx.stats
                        .compaction_time_ms
                        .fetch_add(compaction_millis, Ordering::Relaxed);
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
        });

        *compaction_thd = Some(thd);
        self.inner.signal_compaction_scan();
    }
}

impl Drop for CandyStore {
    fn drop(&mut self) {
        self.stop_compaction();

        if !self.allow_clean_shutdown.load(Ordering::Relaxed) {
            return;
        }
        let data_files_synced = self
            .inner
            .data_files
            .read()
            .values()
            .all(|df| df.file.sync_all().is_ok());
        if !data_files_synced {
            return;
        }
        self.inner
            .index_file
            .header_ref()
            .dirty
            .store(0, Ordering::Release);
        if self.inner.index_file.flush_header().is_err() {
            self.inner
                .index_file
                .header_ref()
                .dirty
                .store(1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::Arc, thread, time::Duration};

    use crate::{CandyStore, Config};

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
        db.inner.shutting_down.store(false, Ordering::Release);
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
        db.inner.shutting_down.store(false, Ordering::Release);
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
                .shutting_down
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
