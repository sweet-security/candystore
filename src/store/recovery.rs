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

enum RebuildMode {
    TailFrom(u64),
    FullActiveFile,
}

impl CandyStore {
    /// How many bytes of replayed data between progressive checkpoints.
    const REBUILD_CHECKPOINT_INTERVAL_BYTES: u64 = 256 * 1024;

    pub(super) fn recover_index(&self) -> Result<()> {
        let active_idx = self.inner.active_file_idx.load(Ordering::Acquire);
        let active_file = self
            .inner
            .data_files
            .read()
            .get(&active_idx)
            .cloned()
            .ok_or(Error::MissingDataFile(active_idx))?;

        let commit_file_ordinal = self
            .inner
            .index_file
            .header_ref()
            .commit_file_ordinal
            .load(Ordering::Acquire);
        let commit_offset = self
            .inner
            .index_file
            .header_ref()
            .commit_offset
            .load(Ordering::Acquire);

        let rebuild_mode = if active_file.file_ordinal == commit_file_ordinal {
            self.validated_commit_offset(&active_file, commit_offset)?
        } else {
            RebuildMode::TailFrom(0)
        };

        let start_offset = match rebuild_mode {
            RebuildMode::TailFrom(offset) => offset,
            RebuildMode::FullActiveFile => 0,
        };

        // The committed cursor marks the active-file prefix already reflected
        // in the index. We only need to rebuild the uncommitted entries delta,
        // since data_bytes and waste_bytes are derived from file sizes and
        // per-file waste levels at query time.
        //
        // The entries delta can be recomputed exactly from the on-disk entry
        // types: InsertData → +1, UpdateData → 0, Tombstone → −1.
        self.inner
            .index_file
            .header_ref()
            .uncommitted_entries_delta
            .store(0, Ordering::Relaxed);

        // Pre-purge any index entries that point past the file's durable
        // extent. This handles the case where the data file was truncated
        // (e.g. disk-full or corruption) and ensures the replay loop won't
        // encounter stale pointers when comparing existing entries.
        let pre_rebuild_used_bytes = active_file.used_bytes();
        let pre_purge_extent = pre_rebuild_used_bytes.next_multiple_of(FILE_OFFSET_ALIGNMENT);
        self.purge_uncommitted_file_entries(active_idx, pre_purge_extent)?;

        if matches!(rebuild_mode, RebuildMode::FullActiveFile) {
            // The saved active-file cursor is no longer trustworthy. Remove
            // every active-file pointer from the index, then rebuild that
            // file's contribution from offset 0 on top of the older files.
            self.purge_uncommitted_file_entries(active_idx, 0)?;
            self.inner
                .index_file
                .header_ref()
                .committed_num_entries
                .store(self.count_live_index_entries(), Ordering::Relaxed);
        }

        let mut offset = start_offset;
        let mut read_buf = Vec::new();
        let mut buf_file_offset = 0u64;
        let mut match_scratch = Vec::new();
        let mut bytes_since_checkpoint = 0u64;
        let mut last_durable_offset = start_offset;
        loop {
            let Some((kv, entry_offset, next_offset)) =
                active_file.read_next_entry_ref(offset, &mut read_buf, &mut buf_file_offset)?
            else {
                break;
            };
            let entry_bytes = next_offset - offset;
            offset = next_offset;

            let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
                return Err(invalid_data_error("unknown key namespace in data file"));
            };

            // Count the entry's contribution to the entries delta based on its
            // on-disk type.  This is unconditional — it doesn't matter whether
            // the index pointer was already applied or not.
            match kv.entry_type {
                EntryType::Insert => self.inner.add_uncommitted_num_entries(1),
                EntryType::Tombstone => self.inner.add_uncommitted_num_entries(-1),
                _ => {} // UpdateData and any future types don't change num_items
            }

            // Fix up the index pointers (no stats accounting).
            self.recover_entry(&active_file, ns, kv, entry_offset, &mut match_scratch)?;
            self.inner
                .stats
                .num_rebuilt_entries
                .fetch_add(1, Ordering::Relaxed);
            last_durable_offset = next_offset;
            crash_point("rebuild_entry");

            bytes_since_checkpoint += entry_bytes;
            if bytes_since_checkpoint >= Self::REBUILD_CHECKPOINT_INTERVAL_BYTES {
                self.flush_rebuild_checkpoint(active_file.file_ordinal, offset)?;
                bytes_since_checkpoint = 0;
            }
        }

        let durable_extent = last_durable_offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);

        if durable_extent < pre_rebuild_used_bytes {
            self.inner
                .stats
                .num_rebuild_purged_bytes
                .fetch_add(pre_rebuild_used_bytes - durable_extent, Ordering::Relaxed);
            active_file.truncate_to_offset(durable_extent)?;
        }

        // Purge any phantom index entries that reference the active file
        // beyond this point (OS flushed the index page but not the data).
        self.purge_uncommitted_file_entries(active_idx, durable_extent)?;

        // Advance the persisted replay cursor to the end of what recovery
        // verified and applied to the index.
        self.flush_rebuild_checkpoint(active_file.file_ordinal, durable_extent)?;

        Ok(())
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
            return Ok(RebuildMode::FullActiveFile);
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
            _ => Ok(RebuildMode::FullActiveFile),
        }
    }

    fn flush_rebuild_checkpoint(&self, ordinal: u64, offset: u64) -> Result<()> {
        let resume_offset = offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);

        // Persist the same prefix in both index rows and committed counters
        // before advancing the replay cursor. The cursor itself must hit disk
        // after the rows it covers.
        self.inner.index_file.rollover_uncommitted_counters();

        self.inner
            .index_file
            .header_ref()
            .commit_file_ordinal
            .store(ordinal, Ordering::Release);
        self.inner
            .index_file
            .header_ref()
            .commit_offset
            .store(resume_offset, Ordering::Release);

        self.inner.index_file.sync_all()
    }

    /// Remove index entries pointing to the active file at or beyond `durable_extent`.
    fn purge_uncommitted_file_entries(&self, file_idx: u16, min_offset: u64) -> Result<()> {
        let row_table = self.inner.index_file.rows_table();
        let num_rows = self.inner.index_file.num_rows();

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
                }
            }
        }
        Ok(())
    }

    fn count_live_index_entries(&self) -> u64 {
        let row_table = self.inner.index_file.rows_table();
        let num_rows = self.inner.index_file.num_rows();
        let mut total = 0u64;

        for row_idx in 0..num_rows {
            let row = row_table.row(row_idx);
            if row.split_level.load(Ordering::Acquire) == 0 {
                continue;
            }
            for col in 0..ROW_WIDTH {
                if row.signatures[col] != HashCoord::INVALID_SIG && row.pointers[col].is_valid() {
                    total += 1;
                }
            }
        }

        total
    }

    fn recover_entry(
        &self,
        data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        entry_offset: u64,
        match_scratch: &mut Vec<u8>,
    ) -> Result<()> {
        match kv.entry_type {
            EntryType::Insert | EntryType::Update => {
                self.recover_data_entry(data_file, ns, kv, entry_offset, match_scratch)
            }
            EntryType::Tombstone => self.recover_tombstone_entry(data_file, ns, kv, match_scratch),
            _ => Ok(()),
        }
    }

    /// Fix index pointers for a data/update entry. No stats accounting —
    /// the entries delta is handled by the caller based on the on-disk entry type.
    fn recover_data_entry(
        &self,
        data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        entry_offset: u64,
        match_scratch: &mut Vec<u8>,
    ) -> Result<()> {
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
                        return Ok(());
                    }
                    if entry.file_idx() == data_file.file_idx
                        && entry.file_offset() > ptr.file_offset()
                    {
                        // A newer active-file entry already exists — skip.
                        return Ok(());
                    }
                    // Older pointer — replace with this newer one.
                    row.replace_pointer(col, ptr);
                    return Ok(());
                }
            }
            // Key not in index — insert it.
            if let Some(col) = row.find_free_slot() {
                row.insert(col, hc.sig, ptr);
                Ok(())
            } else {
                Err(Error::SplitRow(row.split_level.load(Ordering::Relaxed)))
            }
        })
    }

    /// Fix index pointers for a tombstone entry. No stats accounting.
    fn recover_tombstone_entry(
        &self,
        _data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        match_scratch: &mut Vec<u8>,
    ) -> Result<()> {
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
                    return Ok(());
                }
            }
            Ok(())
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
