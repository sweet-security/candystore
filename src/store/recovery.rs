use std::sync::{Arc, atomic::Ordering};

use crate::{
    crash_point,
    data_file::DataFile,
    index_file::EntryPointer,
    internal::{
        EntryType, FILE_OFFSET_ALIGNMENT, HashCoord, KVRef, KeyNamespace, ROW_WIDTH,
        aligned_data_entry_size, aligned_tombstone_entry_waste, invalid_data_error,
    },
    types::{Error, MAX_USER_KEY_SIZE, MAX_USER_VALUE_SIZE, Result},
};

use super::CandyStore;

#[derive(Clone, Copy)]
enum RebuildMode {
    TailFrom(u64),
    FullFile,
}

impl CandyStore {
    /// How many bytes of replayed data between progressive checkpoints.
    const REBUILD_CHECKPOINT_INTERVAL_BYTES: u64 = 256 * 1024;

    pub(super) fn recover_index(&self) -> Result<()> {
        let ordered_files = self.inner.ordered_data_files();
        let Some(last_file) = ordered_files.last().cloned() else {
            return Ok(());
        };

        let (commit_file_ordinal, commit_offset) = self.inner.index_file.checkpoint_cursor();

        let start_idx = ordered_files
            .iter()
            .position(|data_file| data_file.file_ordinal >= commit_file_ordinal)
            .unwrap_or(ordered_files.len() - 1);
        let start_file = &ordered_files[start_idx];
        let rebuild_mode = if start_file.file_ordinal == commit_file_ordinal {
            self.validated_commit_offset(start_file, commit_offset)?
        } else {
            RebuildMode::TailFrom(0)
        };

        // Recompute the runtime-only delta from the persisted replay cursor.
        self.inner
            .uncommitted_entries_delta
            .store(0, Ordering::Relaxed);
        let mut match_scratch = Vec::new();
        let mut bytes_since_checkpoint = 0u64;
        let mut pending_committed_delta = 0i64;

        let mut final_cursor = (last_file.file_ordinal, last_file.used_bytes());
        for (idx, data_file) in ordered_files.iter().enumerate().skip(start_idx) {
            let file_mode = if idx == start_idx {
                rebuild_mode
            } else {
                RebuildMode::TailFrom(0)
            };
            let durable_extent = self.rebuild_file_from(
                data_file,
                file_mode,
                &mut bytes_since_checkpoint,
                &mut pending_committed_delta,
                &mut match_scratch,
            )?;
            final_cursor = (data_file.file_ordinal, durable_extent);
        }

        self.persist_rebuild_checkpoint(final_cursor.0, final_cursor.1, pending_committed_delta)?;
        debug_assert_eq!(
            self.inner.uncommitted_entries_delta.load(Ordering::Relaxed),
            0
        );

        Ok(())
    }

    fn rebuild_file_from(
        &self,
        data_file: &Arc<DataFile>,
        rebuild_mode: RebuildMode,
        bytes_since_checkpoint: &mut u64,
        pending_committed_delta: &mut i64,
        match_scratch: &mut Vec<u8>,
    ) -> Result<u64> {
        let start_offset = match rebuild_mode {
            RebuildMode::TailFrom(offset) => offset,
            RebuildMode::FullFile => 0,
        };

        // Pre-purge any index entries that point past the file's durable
        // extent. This handles the case where the data file was truncated
        // (e.g. disk-full or corruption) and ensures the replay loop won't
        // encounter stale pointers when comparing existing entries.
        let pre_rebuild_used_bytes = data_file.used_bytes();
        let pre_purge_extent = pre_rebuild_used_bytes.next_multiple_of(FILE_OFFSET_ALIGNMENT);
        self.apply_recovery_delta(
            self.purge_uncommitted_file_entries(data_file.file_idx, pre_purge_extent)?,
            pending_committed_delta,
        );

        if matches!(rebuild_mode, RebuildMode::FullFile) {
            // The saved checkpoint within this file is no longer trustworthy.
            // Remove every pointer into it and rebuild its contribution from 0.
            self.apply_recovery_delta(
                self.purge_uncommitted_file_entries(data_file.file_idx, 0)?,
                pending_committed_delta,
            );
        }

        let mut offset = start_offset;
        let mut read_buf = Vec::new();
        let mut buf_file_offset = 0u64;
        let mut last_durable_offset = start_offset;
        loop {
            let Some((kv, entry_offset, next_offset)) =
                data_file.read_next_entry_ref(offset, &mut read_buf, &mut buf_file_offset)?
            else {
                break;
            };
            let entry_bytes = next_offset - offset;
            offset = next_offset;

            let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
                return Err(invalid_data_error("unknown key namespace in data file"));
            };

            self.apply_recovery_delta(
                self.recover_entry(data_file, ns, kv, entry_offset, match_scratch)?,
                pending_committed_delta,
            );
            self.inner
                .stats
                .num_rebuilt_entries
                .fetch_add(1, Ordering::Relaxed);
            last_durable_offset = next_offset;
            crash_point("rebuild_entry");

            *bytes_since_checkpoint += entry_bytes;
            if *bytes_since_checkpoint >= Self::REBUILD_CHECKPOINT_INTERVAL_BYTES {
                self.persist_rebuild_checkpoint(
                    data_file.file_ordinal,
                    offset,
                    *pending_committed_delta,
                )?;
                *pending_committed_delta = 0;
                *bytes_since_checkpoint = 0;
            }
        }

        let durable_extent = last_durable_offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);

        if durable_extent < pre_rebuild_used_bytes {
            self.inner
                .stats
                .num_rebuild_purged_bytes
                .fetch_add(pre_rebuild_used_bytes - durable_extent, Ordering::Relaxed);
            data_file.truncate_to_offset(durable_extent)?;
        }

        self.apply_recovery_delta(
            self.purge_uncommitted_file_entries(data_file.file_idx, durable_extent)?,
            pending_committed_delta,
        );
        Ok(durable_extent)
    }

    fn validated_commit_offset(
        &self,
        active_file: &Arc<DataFile>,
        checkpoint_offset: u64,
    ) -> Result<RebuildMode> {
        if checkpoint_offset == 0 {
            return Ok(RebuildMode::TailFrom(0));
        }

        let used_bytes = active_file.used_bytes();
        if checkpoint_offset > used_bytes {
            return Ok(RebuildMode::FullFile);
        }
        if checkpoint_offset == used_bytes {
            return Ok(RebuildMode::TailFrom(checkpoint_offset));
        }

        let mut probe_buf = Vec::new();
        let mut probe_file_offset = 0u64;
        match active_file.read_next_entry_ref(
            checkpoint_offset,
            &mut probe_buf,
            &mut probe_file_offset,
        )? {
            Some((_, entry_offset, _)) if entry_offset == checkpoint_offset => {
                Ok(RebuildMode::TailFrom(checkpoint_offset))
            }
            _ => Ok(RebuildMode::FullFile),
        }
    }

    fn persist_rebuild_checkpoint(&self, ordinal: u64, offset: u64, delta: i64) -> Result<()> {
        let resume_offset = offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);

        self.inner.fold_checkpointed_num_entries(delta);
        self.inner.persist_checkpoint_cursor(ordinal, resume_offset);

        self.inner.index_file.sync_all()
    }

    /// Remove index entries pointing to the active file at or beyond `durable_extent`.
    fn purge_uncommitted_file_entries(&self, file_idx: u16, min_offset: u64) -> Result<i64> {
        let row_table = self.inner.index_file.rows_table();
        let num_rows = self.inner.index_file.num_rows();
        let mut removed = 0i64;

        for row_idx in 0..num_rows {
            let mut row = row_table.row_mut(row_idx);
            if row.split_level.load(Ordering::Acquire) == 0 {
                continue;
            }
            for col in 0..ROW_WIDTH {
                if row.signatures[col] == HashCoord::INVALID_SIG {
                    continue;
                }
                let ptr = row.pointers[col];
                if !ptr.is_valid() {
                    continue;
                }
                if ptr.file_idx() == file_idx && ptr.file_offset() >= min_offset {
                    row.remove(col);
                    removed += 1;
                }
            }
        }
        Ok(-removed)
    }

    fn recover_entry(
        &self,
        data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        entry_offset: u64,
        match_scratch: &mut Vec<u8>,
    ) -> Result<i64> {
        match kv.entry_type {
            EntryType::Insert | EntryType::Update => {
                self.recover_data_entry(data_file, ns, kv, entry_offset, match_scratch)
            }
            EntryType::Tombstone => self.recover_tombstone_entry(data_file, ns, kv, match_scratch),
            _ => Ok(0),
        }
    }

    fn apply_recovery_delta(&self, delta: i64, pending_committed_delta: &mut i64) {
        if delta == 0 {
            return;
        }

        self.inner.add_uncommitted_num_entries(delta);
        *pending_committed_delta += delta;
    }

    /// Fix index pointers for a data/update entry and return its live-entry delta.
    fn recover_data_entry(
        &self,
        data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        entry_offset: u64,
        match_scratch: &mut Vec<u8>,
    ) -> Result<i64> {
        let key = kv.key();
        let val = kv.value();
        self.validate_recovered_data_entry(key, val)?;
        let entry_len = 4 + 4 + key.len() + val.len() + 2;
        let aligned_len = entry_len.next_multiple_of(FILE_OFFSET_ALIGNMENT as usize);
        let hc = HashCoord::new(ns, key, self.inner.config.hash_key);
        let ptr = EntryPointer::new(
            data_file.file_idx,
            entry_offset,
            aligned_len,
            hc.masked_row_selector(),
        );

        self.inner._mut_op(ns, key, &[], |hc, mut row, key, _| {
            let files = self.inner.data_files.read();
            for (col, entry) in row.iter_matches(hc) {
                let file = files
                    .get(&entry.file_idx())
                    .ok_or(Error::MissingDataFile(entry.file_idx()))?;
                let existing_kv =
                    file.read_kv_into(entry.file_offset(), entry.size_hint(), match_scratch)?;
                if existing_kv.key() == key {
                    if entry == ptr {
                        // Already points at this entry — nothing to fix.
                        return Ok(0);
                    }
                    if entry.file_idx() == data_file.file_idx
                        && entry.file_offset() > ptr.file_offset()
                    {
                        // A newer active-file entry already exists — skip.
                        return Ok(0);
                    }
                    // Older pointer — replace with this newer one.
                    row.replace_pointer(col, ptr);
                    return Ok(0);
                }
            }
            // Key not in index — insert it.
            if let Some(col) = row.find_free_slot() {
                row.insert(col, hc.sig, ptr);
                Ok(1)
            } else {
                Err(Error::SplitRow(row.split_level.load(Ordering::Relaxed)))
            }
        })
    }

    /// Fix index pointers for a tombstone entry and return its live-entry delta.
    fn recover_tombstone_entry(
        &self,
        _data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        match_scratch: &mut Vec<u8>,
    ) -> Result<i64> {
        let key = kv.key();
        self.validate_recovered_tombstone_entry(key)?;

        self.inner._mut_op(ns, key, &[], |hc, mut row, key, _| {
            let files = self.inner.data_files.read();
            for (col, entry) in row.iter_matches(hc) {
                let file = files
                    .get(&entry.file_idx())
                    .ok_or(Error::MissingDataFile(entry.file_idx()))?;
                let existing_kv =
                    file.read_kv_into(entry.file_offset(), entry.size_hint(), match_scratch)?;
                if existing_kv.key() == key {
                    row.remove(col);
                    return Ok(-1);
                }
            }
            Ok(0)
        })
    }

    fn validate_recovered_data_entry(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let entry_size = aligned_data_entry_size(key.len(), val.len()) as usize;
        if key.len() > MAX_USER_KEY_SIZE
            || val.len() > MAX_USER_VALUE_SIZE
            || entry_size > self.inner.config.max_data_file_size as usize
        {
            return Err(invalid_data_error(
                "recovered data entry exceeds configured limits",
            ));
        }
        Ok(())
    }

    fn validate_recovered_tombstone_entry(&self, key: &[u8]) -> Result<()> {
        let entry_size = aligned_tombstone_entry_waste(key.len()) as usize;
        if key.len() > MAX_USER_KEY_SIZE
            || entry_size > self.inner.config.max_data_file_size as usize
        {
            return Err(invalid_data_error(
                "recovered tombstone entry exceeds configured limits",
            ));
        }
        Ok(())
    }
}
