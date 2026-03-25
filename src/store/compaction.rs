use std::sync::{Arc, atomic::Ordering};

use crate::{
    index_file::EntryPointer,
    internal::{EntryType, KeyNamespace, data_file_path, invalid_data_error, sync_dir},
    pacer::Pacer,
    types::{Error, Result},
};

use super::{CandyStore, StoreInner};

pub(super) struct CompactionOutcome {
    pub(super) reclaimed_bytes: u32,
    pub(super) moved_bytes: u32,
}

impl StoreInner {
    pub(super) fn compact_file(
        &self,
        file_idx: u16,
        expected_ordinal: u64,
        pacer: &mut Pacer,
        #[cfg(windows)] pending_deletions: &mut Vec<std::path::PathBuf>,
    ) -> Result<CompactionOutcome> {
        if self.active_file_idx.load(Ordering::Acquire) == file_idx {
            return Ok(CompactionOutcome {
                reclaimed_bytes: 0,
                moved_bytes: 0,
            });
        }

        let source_file = match self.data_file(file_idx) {
            Ok(f) => f,
            Err(Error::MissingDataFile(_)) => {
                return Ok(CompactionOutcome {
                    reclaimed_bytes: 0,
                    moved_bytes: 0,
                });
            }
            Err(e) => return Err(e),
        };
        if source_file.file_ordinal != expected_ordinal {
            return Ok(CompactionOutcome {
                reclaimed_bytes: 0,
                moved_bytes: 0,
            });
        }

        let mut offset = 0u64;
        let mut moved_bytes = 0u64;
        let mut read_buf = Vec::new();
        let mut buf_file_offset = 0u64;
        let mut match_scratch = Vec::new();

        loop {
            if self.shutting_down.load(Ordering::Acquire) {
                return Ok(CompactionOutcome {
                    reclaimed_bytes: 0,
                    moved_bytes: 0,
                });
            }

            let Some((kv, entry_offset, next_offset)) =
                source_file.read_next_entry_ref(offset, &mut read_buf, &mut buf_file_offset)?
            else {
                break;
            };
            offset = next_offset;
            self.record_read(next_offset - entry_offset);

            pacer.consume(next_offset - entry_offset);

            let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
                return Err(invalid_data_error("unknown key namespace in data file"));
            };

            if let EntryType::Data = kv.entry_type {
                let key = kv.key();
                let val = kv.value();

                self._mut_op(ns, key, val, |hc, mut row, key, val| {
                    let files = self.data_files.read();
                    for (col, entry) in row.iter_matches(hc) {
                        let Some(file) = files.get(&entry.file_idx()) else {
                            continue;
                        };
                        self.record_read(entry.size_hint() as u64);
                        let existing_kv = file.read_kv_into(
                            entry.file_offset(),
                            entry.size_hint(),
                            &mut match_scratch,
                        )?;
                        if existing_kv.key() == key {
                            if entry.file_idx() != file_idx || entry.file_offset() != entry_offset {
                                return Ok(());
                            }

                            let active_idx = self.active_file_idx.load(Ordering::Acquire);
                            let active_file = files
                                .get(&active_idx)
                                .ok_or(Error::MissingDataFile(active_idx))?;
                            let (file_off, size) = active_file.append_kv(ns, key, val)?;
                            self.record_write(size as u64);
                            moved_bytes = moved_bytes.saturating_add(size as u64);
                            row.replace_pointer(
                                col,
                                EntryPointer::new(
                                    active_idx,
                                    file_off,
                                    size,
                                    hc.masked_row_selector(),
                                ),
                            );
                            return Ok(());
                        }
                    }
                    Ok(())
                })?;
            }
        }

        let removed = self.data_files.write().remove(&file_idx);
        drop(source_file); // MUST drop before removing file to release mmap and handle
        drop(removed); // Drop any open handles to the file

        // Take file waste regardless of whether remove succeeds, to avoid infinite 100% loop if remove fails
        let reclaimed = self.index_file.take_file_waste(file_idx);
        self.index_file
            .header_ref()
            .reclaimed_bytes
            .fetch_add(reclaimed as u64, Ordering::Relaxed);

        let file_path = data_file_path(self.base_path.as_path(), file_idx);
        match std::fs::remove_file(&file_path) {
            Ok(()) => sync_dir(self.base_path.as_path())?,
            #[cfg(windows)]
            Err(_) => pending_deletions.push(file_path),
            #[cfg(not(windows))]
            Err(err) => return Err(Error::IOError(err)),
        }
        Ok(CompactionOutcome {
            reclaimed_bytes: reclaimed,
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
            let throughput_bytes_per_sec =
                (ctx.config.compaction_throughput_bytes_per_sec as u64).max(1);
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
                while let Some((file_idx, file_ordinal)) = ctx.next_compaction_candidate() {
                    if ctx.shutting_down.load(Ordering::Acquire) {
                        return;
                    }
                    #[cfg(windows)]
                    Self::retry_pending_deletions(&ctx, &mut pending_deletions);
                    let t0 = std::time::Instant::now();
                    let res = ctx.compact_file(
                        file_idx,
                        file_ordinal,
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
                            ctx.stats.num_compactions.fetch_add(1, Ordering::Relaxed);
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
