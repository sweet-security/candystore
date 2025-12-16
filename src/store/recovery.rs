use crate::files::data_file::{DataEntryType, DataFile, KVBuf};
use crate::files::index_file::IndexFile;
use crate::types::{
    CandyError, Config, EntryPointer, HashCoordinates, KeyNamespace, RecoveryMode, Result,
    SpecialEntryType,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

struct RebuildEntry {
    file_id: u16,
    offset: u32,
    size: u32,
    kvbuf: KVBuf,
}

fn process_rebuild_entry(
    index_file: &IndexFile,
    data_files: &HashMap<u16, Arc<DataFile>>,
    config: &Config,
    file_stats: &mut HashMap<u16, u64>,
    entry: RebuildEntry,
) -> Result<()> {
    let RebuildEntry {
        file_id,
        offset,
        size,
        kvbuf,
    } = entry;
    debug_assert!(size > 0);
    debug_assert!(offset.checked_add(size).is_some());
    debug_assert_eq!(
        size as usize,
        kvbuf.buf.len() + DataFile::ENTRY_CHECKSUM_LEN,
        "rebuild size mismatch"
    );
    let ns = KeyNamespace::from_u8(kvbuf.ns());
    if ns == KeyNamespace::_Invalid {
        return Ok(());
    }
    let key = kvbuf.key();

    let hc = HashCoordinates::from_key(config.hash_key.0, config.hash_key.1, ns, key);

    index_file.operate_on_row_mut(hc, |row, header| {
        for (idx, ptr) in row.iter_matches(hc) {
            if !ptr.is_data_pointer() {
                continue;
            }

            let old_file_id = ptr.file_id;
            if let Some(old_file) = data_files.get(&old_file_id) {
                if old_file_id == file_id && ptr.file_offset == offset {
                    return Ok(());
                }

                if let Ok(old_kv) = old_file.read_entry(ptr.file_offset, Some(ptr.size_hint()))
                    && old_kv.ns() == kvbuf.ns()
                    && old_kv.key() == key
                {
                    let old_checksum = ptr.calc_checksum(hc.signature);
                    let old_size = (old_kv.buf.len() + DataFile::ENTRY_CHECKSUM_LEN) as u64;
                    let stats = file_stats.entry(old_file_id).or_default();

                    if kvbuf.entry_type == DataEntryType::Tombstone {
                        row.signatures[idx] = HashCoordinates::INVALID_SIG;
                        row.pointers[idx] = EntryPointer::INVALID_PTR;
                        header
                            .index_checksum
                            .fetch_xor(old_checksum, std::sync::atomic::Ordering::Relaxed);
                        header
                            .num_deletes
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        *stats += old_size;
                    } else {
                        row.pointers[idx] =
                            EntryPointer::new(file_id, offset, hc.row_selector, size);
                        let new_checksum = row.pointers[idx].calc_checksum(hc.signature);
                        header.index_checksum.fetch_xor(
                            old_checksum ^ new_checksum,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        header
                            .num_updates
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        *stats += old_size;
                    }
                    return Ok(());
                }
            }
        }

        if kvbuf.entry_type == DataEntryType::KV {
            if let Some(idx) = row
                .signatures
                .iter()
                .position(|&sig| sig == HashCoordinates::INVALID_SIG)
            {
                row.signatures[idx] = hc.signature;
                row.pointers[idx] = EntryPointer::new(file_id, offset, hc.row_selector, size);
                let new_checksum = row.pointers[idx].calc_checksum(hc.signature);
                header
                    .index_checksum
                    .fetch_xor(new_checksum, std::sync::atomic::Ordering::Relaxed);
                header
                    .num_inserts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            } else {
                Err(CandyError::SplitRow)
            }
        } else {
            Ok(())
        }
    })
}

fn record_special_entry(
    index_file: &IndexFile,
    config: &Config,
    file_id: u16,
    val: u64,
    special_type: SpecialEntryType,
    ns: KeyNamespace,
) -> Result<()> {
    let key = file_id as u64;
    let key_bytes = key.to_le_bytes();
    let hc = HashCoordinates::from_key(config.hash_key.0, config.hash_key.1, ns, &key_bytes);
    index_file.operate_on_row_mut(hc, |row, _header| {
        if let Some(idx) = row
            .signatures
            .iter()
            .position(|&sig| sig == HashCoordinates::INVALID_SIG)
        {
            row.signatures[idx] = hc.signature;
            row.pointers[idx] = EntryPointer::new_special(special_type, val, hc.row_selector);
            Ok(())
        } else {
            Err(CandyError::SplitRow)
        }
    })
}

fn rebuild_index(
    index_file: &IndexFile,
    data_files: &HashMap<u16, Arc<DataFile>>,
    config: &Config,
    start_pos: Option<(u16, u32)>,
) -> Result<()> {
    let mut sorted_files: Vec<_> = data_files.iter().collect();
    sorted_files.sort_by_key(|(_, f)| f.serial);

    let mut file_stats: HashMap<u16, u64> = HashMap::new();
    let (start_file_id, start_offset) = start_pos.unwrap_or((0, 0));

    let mut max_file_id = start_file_id;
    let mut max_offset = start_offset;

    for (&file_id, data_file) in sorted_files {
        if file_id < start_file_id {
            continue;
        }

        let iter = if file_id == start_file_id {
            data_file.iter_entries_from(start_offset)
        } else {
            data_file.iter_entries()
        };

        for entry in iter {
            let (offset, size, kvbuf) = match entry {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        "Error reading data file {} at entry iteration: {}. Stopping iteration for this file.",
                        file_id, e
                    );
                    break;
                }
            };

            process_rebuild_entry(
                index_file,
                data_files,
                config,
                &mut file_stats,
                RebuildEntry {
                    file_id,
                    offset,
                    size,
                    kvbuf,
                },
            )?;

            if file_id > max_file_id || (file_id == max_file_id && offset + size > max_offset) {
                max_file_id = file_id;
                max_offset = offset + size;
            }
        }
    }

    for (file_id, wasted) in file_stats {
        if wasted > 0 {
            record_special_entry(
                index_file,
                config,
                file_id,
                wasted,
                SpecialEntryType::WastedBytes,
                KeyNamespace::StatsWastedBytes,
            )?;
        }
    }

    Ok(())
}

fn reset_store(
    index_file: &IndexFile,
    data_files: &HashMap<u16, Arc<DataFile>>,
    config: &Config,
) -> Result<()> {
    warn!("Resetting store due to corruption (RecoveryMode::ClearAllIfCorrupted)");
    index_file.reset(config)?;
    for data_file in data_files.values() {
        data_file.reset()?;
    }
    Ok(())
}

pub(super) fn check_consistency_and_recover(
    index_file: &IndexFile,
    data_files: &HashMap<u16, Arc<DataFile>>,
    config: &Config,
    needs_rebuild: bool,
) -> Result<()> {
    if !needs_rebuild {
        return Ok(());
    }
    match config.recovery_mode {
        RecoveryMode::RebuildIndexIfCorrupted => {
            warn!("Index corrupted or missing. Performing full rebuild.");
            rebuild_index(index_file, data_files, config, None)?
        }
        RecoveryMode::ClearAllIfCorrupted => reset_store(index_file, data_files, config)?,
        RecoveryMode::FailIfCorrupted => {
            return Err(CandyError::DataCorruption(
                "Index checksum mismatch".to_string(),
            ));
        }
    }
    Ok(())
}
