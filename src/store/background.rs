use crate::files::data_file::{DataEntryType, DataFile};
use crate::files::index_file::RowLayout;
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

            let (candidates, min_serial) = {
                let data_files = inner.data_files.read();
                let active_id = inner.active_file_id.load(Ordering::Relaxed) as u16;
                let min_serial = data_files.values().map(|f| f.serial).min().unwrap_or(0);
                let candidates = data_files
                    .iter()
                    .filter(|(id, _)| **id != active_id)
                    .map(|(id, file)| (*id, file.clone()))
                    .collect::<Vec<_>>();
                (candidates, min_serial)
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

                file.is_under_compaction.store(true, Ordering::SeqCst);
                if let Err(e) = compact_file(&inner, file_id, &file, min_serial) {
                    file.is_under_compaction.store(false, Ordering::SeqCst);
                    error!("Compaction failed for file {}: {}", file_id, e);
                }
            }

            shutdown_guard = lock.lock();
        }
    }
}

fn key_exists_in_row(
    inner: &CandyStoreInner,
    row: &RowLayout,
    hc: HashCoordinates,
    ns: KeyNamespace,
    key: &[u8],
) -> bool {
    for (_idx, ptr) in row.iter_matches(hc) {
        if !ptr.is_data_pointer() {
            continue;
        }
        let data_files = inner.data_files.read();
        let file_id = ptr.file_id;
        if let Some(file) = data_files.get(&file_id)
            && let Ok(kv) = file.read_kv(ptr.file_offset, ptr.size_hint())
            && kv.ns() == ns as u8
            && kv.key() == key
        {
            return true;
        }
    }
    false
}

pub(super) fn compact_file(
    inner: &Arc<CandyStoreInner>,
    file_id: u16,
    file: &DataFile,
    min_serial: u64,
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

        let is_active = inner.index_file.operate_on_row(hc, |row| {
            for (_, ptr) in row.iter_matches(hc) {
                if ptr.file_id == file_id && ptr.file_offset == offset {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;

        if is_active {
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
        } else if kvbuf.entry_type == DataEntryType::Tombstone {
            // Check if the key exists in the index (shadowed by a newer value)
            let key_exists = inner.index_file.operate_on_row(hc, |row| {
                Ok(key_exists_in_row(inner, row, hc, ns, kvbuf.key()))
            })?;

            if key_exists {
                // Stale tombstone, shadowed by a newer value. Drop it.
                continue;
            }

            // If we are compacting the oldest file, we can drop the tombstone IF we are sure
            // no other file contains an older version of this key.
            // However, since we don't check all other files, we only drop if this is the ONLY file
            // (min_serial check is insufficient if there are multiple files and we are not checking them).
            // For safety, we only drop tombstones if we are sure no older data exists.
            // Since we don't have a global view here easily, we'll be conservative:
            // Only drop if this is the oldest file AND we are sure no other file has this key.
            // But checking all files is expensive.
            //
            // Correct approach for LSM: Tombstones can be dropped only during a "major compaction"
            // (merging all files) or if we know this is the bottom-most level.
            // Here, we have a flat list of files. "min_serial" implies this is the oldest file.
            // If this is the oldest file, no OLDER file exists. So any value for this key must be
            // in a NEWER file.
            // If a newer file has the key, `key_exists` (checked against index) would be true.
            // So if `key_exists` is false, it means the key is NOT in the index.
            // If the key is not in the index, it means either:
            // 1. It's deleted (current state).
            // 2. It's in a newer file but not indexed yet? (Impossible, we index on write).
            //
            // So if `key_exists` is false, the key is effectively deleted.
            // If we are the oldest file (`file.serial == min_serial`), then no older version exists.
            // So it is safe to drop the tombstone.

            if file.serial == min_serial {
                continue;
            }

            // Keep tombstone to shadow potential older values in other files.
            let _ = inner.append_tombstone_to_active_file(ns, kvbuf.key())?;
        }

        let _ = inner.maybe_rotate_data_file()?;
    }

    // Ensure the active file is synced before we delete the old file, to prevent data loss
    // if we crash after deletion but before the new data hits the disk.
    {
        let data_files = inner.data_files.read();
        let active_id = inner.active_file_id.load(Ordering::Relaxed) as u16;
        if let Some(active_file) = data_files.get(&active_id) {
            active_file.file.sync_all().map_err(CandyError::IOError)?;
        }
    }

    {
        inner.data_files.write().remove(&file_id);
    }

    let path = inner.dir_path.join(format!("data_{:05}.db", file_id));
    std::fs::remove_file(path).map_err(CandyError::IOError)?;

    inner.index_file.record_compacted_file();

    Ok(())
}
