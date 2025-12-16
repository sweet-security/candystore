use crate::files::data_file::DataFile;
use crate::types::{
    CandyError, EntryPointer, HashCoordinates, KeyNamespace, Result, SpecialEntryType,
};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::error;

use super::inner::CandyStoreInner;

pub(super) fn flush_loop(inner: Arc<CandyStoreInner>, shutdown: Arc<(Mutex<bool>, Condvar)>) {
    let interval = inner.config.flush_interval.unwrap();
    let (lock, cvar) = &*shutdown;
    let mut shutdown_guard = lock.lock();

    while !*shutdown_guard {
        let result = cvar.wait_for(&mut shutdown_guard, interval);
        if *shutdown_guard {
            break;
        }
        if result.timed_out() {
            drop(shutdown_guard);

            let data_files_guard = inner.data_files.write();
            let active_id = inner.active_file_id.load(Ordering::Relaxed) as u16;
            let file = data_files_guard.get(&active_id).cloned();
            drop(data_files_guard);

            if let Some(file) = file
                && let Err(e) = file.flush_checkpoint(file.write_offset.load(Ordering::SeqCst), 0)
            {
                error!("Failed to flush file {}: {}", active_id, e);
            }

            shutdown_guard = lock.lock();
        }
    }
}

pub(super) fn compaction_loop(inner: Arc<CandyStoreInner>, shutdown: Arc<(Mutex<bool>, Condvar)>) {
    let (lock, cvar) = &*shutdown;
    let mut shutdown_guard = lock.lock();

    while !*shutdown_guard {
        let result = cvar.wait_for(&mut shutdown_guard, inner.config.compaction_interval);
        if *shutdown_guard {
            break;
        }
        if result.timed_out() {
            drop(shutdown_guard);

            let candidates: Vec<(u16, Arc<DataFile>)> = {
                let data_files = inner.data_files.read();
                let active_id = inner.active_file_id.load(Ordering::Relaxed) as u16;
                data_files
                    .iter()
                    .filter(|(id, _)| **id != active_id)
                    .map(|(id, file)| (*id, file.clone()))
                    .collect()
            };

            let mut eligible_candidates = Vec::new();

            for (id, file) in candidates {
                let wasted = if let Ok(Some(w)) = inner.get_special_key(
                    KeyNamespace::StatsWastedBytes,
                    SpecialEntryType::WastedBytes,
                    id as u64,
                ) {
                    w
                } else {
                    0
                };

                let size = file.write_offset.load(Ordering::Relaxed);
                if size < inner.config.compaction_min_file_size as u64 {
                    continue;
                }

                let ratio = wasted as f64 / size as f64;
                if ratio > inner.config.compaction_min_waste_threshold.clamp(0.0, 1.0) {
                    eligible_candidates.push((ratio, id, file));
                }
            }

            // Sort by waste ratio descending (highest waste first)
            eligible_candidates
                .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

            for (_, file_id, file) in eligible_candidates {
                // Check for shutdown and delay
                {
                    let mut guard = lock.lock();
                    if *guard {
                        break;
                    }
                    // Wait for 100ms or shutdown signal
                    let _ = cvar.wait_for(&mut guard, std::time::Duration::from_millis(100));
                    if *guard {
                        break;
                    }
                }

                if let Err(e) = compact_file(&inner, file_id, &file) {
                    error!("Compaction failed for file {}: {}", file_id, e);
                }
            }

            shutdown_guard = lock.lock();
        }
    }
}

pub(super) fn compact_file(
    inner: &Arc<CandyStoreInner>,
    file_id: u16,
    file: &DataFile,
) -> Result<()> {
    for entry_res in file.iter_entries() {
        let (offset, _size, kvbuf) = entry_res?;

        let ns = KeyNamespace::from_u8(kvbuf.ns());
        if !ns.is_data_entry() {
            continue;
        }

        let hc = HashCoordinates::from_key(
            inner.config.hash_key.0,
            inner.config.hash_key.1,
            ns,
            kvbuf.key(),
        );

        let is_valid = inner.index_file.operate_on_row(hc, |row| {
            for (_, ptr) in row.iter_matches(hc) {
                if ptr.file_id == file_id && ptr.file_offset == offset {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;

        if !is_valid {
            continue;
        }

        let (active_id, new_offset, size) =
            inner.append_kv_to_active_file(ns, kvbuf.key(), kvbuf.value())?;

        inner.index_file.operate_on_row_mut(hc, |row, header| {
            for (idx, ptr) in row.iter_matches(hc) {
                if ptr.file_id == file_id && ptr.file_offset == offset {
                    let old_checksum = ptr.calc_checksum(hc.signature);
                    row.pointers[idx] =
                        EntryPointer::new(active_id, new_offset, hc.row_selector, size);
                    let new_checksum = row.pointers[idx].calc_checksum(hc.signature);
                    header
                        .index_checksum
                        .fetch_xor(old_checksum ^ new_checksum, Ordering::Relaxed);
                    return Ok(());
                }
            }
            Ok(())
        })?;

        let _ = inner.maybe_rotate_data_file()?;
    }

    {
        inner.data_files.write().remove(&file_id);
    }

    let path = inner.dir_path.join(format!("data_{:05}.db", file_id));
    std::fs::remove_file(path).map_err(CandyError::IOError)?;

    inner.index_file.record_compacted_file();

    Ok(())
}
