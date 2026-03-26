use std::sync::{Arc, atomic::AtomicU64, atomic::Ordering};

use crate::{
    data_file::DataFile,
    index_file::EntryPointer,
    internal::{
        EntryType, FILE_OFFSET_ALIGNMENT, HashCoord, KVRef, KeyNamespace, aligned_data_entry_size,
        aligned_data_entry_waste, aligned_tombstone_entry_waste, invalid_data_error,
    },
    types::{Error, MAX_USER_KEY_SIZE, MAX_USER_VALUE_SIZE, Result},
};

use super::CandyStore;

static RECOVERED_ENTRIES_FOR_ABORT_TEST: AtomicU64 = AtomicU64::new(0);

impl CandyStore {
    /// How many entries to process before flushing a mid-file checkpoint.
    const REBUILD_CHECKPOINT_INTERVAL: u64 = 1000;

    fn rebuild_checkpoint_checksum(ordinal: u64, ptr: u64) -> u64 {
        ordinal.rotate_left(17) ^ ptr.rotate_right(11) ^ 0x5a17_b1d2_c3e4_f607
    }

    fn encode_rebuild_checkpoint_ptr(file_idx: u16, file_offset: u64) -> u64 {
        let fi = (file_idx as u64) & ((1 << 12) - 1);
        let fo = (file_offset / FILE_OFFSET_ALIGNMENT) << 12;
        fi | fo
    }

    fn decode_rebuild_checkpoint_ptr(ptr: u64) -> (u16, u64) {
        let file_idx = (ptr & ((1 << 12) - 1)) as u16;
        let file_offset = (ptr >> 12) * FILE_OFFSET_ALIGNMENT;
        (file_idx, file_offset)
    }

    fn read_rebuild_checkpoint(&self) -> Option<(u64, u16, u64)> {
        let header = self.inner.index_file.header_ref();
        let ordinal = header.rebuild_checkpoint_ordinal.load(Ordering::Acquire);
        let ptr = header.rebuild_checkpoint_ptr.load(Ordering::Acquire);
        let checksum = header.rebuild_checkpoint_checksum.load(Ordering::Acquire);

        if ordinal == 0 && ptr == 0 && checksum == 0 {
            return None;
        }

        if checksum != Self::rebuild_checkpoint_checksum(ordinal, ptr) {
            return None;
        }

        let (file_idx, file_offset) = Self::decode_rebuild_checkpoint_ptr(ptr);
        Some((ordinal, file_idx, file_offset))
    }

    fn maybe_abort_rebuild_for_testing(&self) {
        let Ok(after) = std::env::var("CANDYSTORE_ABORT_REBUILD_AFTER") else {
            return;
        };
        let Ok(after) = after.parse::<u64>() else {
            return;
        };
        if after == 0 {
            return;
        }

        let recovered = RECOVERED_ENTRIES_FOR_ABORT_TEST.fetch_add(1, Ordering::Relaxed) + 1;
        if recovered >= after {
            std::process::abort();
        }
    }

    pub(super) fn recover_index(&self) -> Result<()> {
        let mut sorted_files: Vec<Arc<DataFile>> =
            self.inner.data_files.read().values().cloned().collect();
        sorted_files.sort_by_key(|df| df.file_ordinal);

        let checkpoint =
            self.read_rebuild_checkpoint()
                .and_then(|(ordinal, file_idx, file_offset)| {
                    sorted_files
                        .iter()
                        .find(|df| df.file_idx == file_idx && df.file_ordinal == ordinal)
                        .map(|_| (ordinal, file_idx, file_offset))
                });

        if checkpoint.is_none() {
            // No previous progress, checkpoint corruption, or the checkpointed
            // file no longer matches the persisted stable identity — full rebuild.
            let row_table = self.inner.index_file.rows_table_mut();
            self.inner.index_file.reset(row_table)?;
        }

        for data_file in &sorted_files {
            let start_offset = if checkpoint.is_some_and(|(ordinal, file_idx, _)| {
                data_file.file_ordinal == ordinal && data_file.file_idx == file_idx
            }) {
                checkpoint.unwrap().2
            } else if checkpoint.is_some_and(|(ordinal, _, _)| data_file.file_ordinal < ordinal) {
                continue;
            } else {
                0
            };

            let mut offset = start_offset;
            let mut read_buf = Vec::new();
            let mut buf_file_offset = 0u64;
            let mut match_scratch = Vec::new();
            let mut entries_since_checkpoint = 0u64;
            loop {
                let Some((kv, entry_offset, next_offset)) =
                    data_file.read_next_entry_ref(offset, &mut read_buf, &mut buf_file_offset)?
                else {
                    break;
                };
                offset = next_offset;

                let Some(ns) = KeyNamespace::from_u8(kv.ns) else {
                    return Err(invalid_data_error("unknown key namespace in data file"));
                };

                self.recover_entry(data_file, ns, kv, entry_offset, &mut match_scratch)?;
                self.maybe_abort_rebuild_for_testing();

                entries_since_checkpoint += 1;
                if entries_since_checkpoint >= Self::REBUILD_CHECKPOINT_INTERVAL {
                    self.flush_rebuild_checkpoint(
                        data_file.file_ordinal,
                        data_file.file_idx,
                        offset,
                    )?;
                    entries_since_checkpoint = 0;
                }
            }

            // File fully processed — keep progress at this file's final offset.
            self.flush_rebuild_checkpoint(data_file.file_ordinal, data_file.file_idx, offset)?;
        }

        // Rebuild complete — clear checkpoint.
        self.clear_rebuild_checkpoint()?;

        Ok(())
    }

    fn flush_rebuild_checkpoint(&self, ordinal: u64, file_idx: u16, offset: u64) -> Result<()> {
        self.inner.index_file.flush_rows()?;

        let ptr = Self::encode_rebuild_checkpoint_ptr(file_idx, offset);
        let checksum = Self::rebuild_checkpoint_checksum(ordinal, ptr);
        let header = self.inner.index_file.header_ref();
        header
            .rebuild_checkpoint_ordinal
            .store(ordinal, Ordering::Release);
        header.rebuild_checkpoint_ptr.store(ptr, Ordering::Release);
        header
            .rebuild_checkpoint_checksum
            .store(checksum, Ordering::Release);
        self.inner.index_file.flush_header()
    }

    fn clear_rebuild_checkpoint(&self) -> Result<()> {
        self.inner.index_file.flush_rows()?;

        let header = self.inner.index_file.header_ref();
        header
            .rebuild_checkpoint_ordinal
            .store(0, Ordering::Release);
        header.rebuild_checkpoint_ptr.store(0, Ordering::Release);
        header
            .rebuild_checkpoint_checksum
            .store(0, Ordering::Release);
        self.inner.index_file.flush_header()
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
            EntryType::Data => {
                self.recover_data_entry(data_file, ns, kv, entry_offset, match_scratch)
            }
            EntryType::Tombstone => self.recover_tombstone_entry(data_file, ns, kv, match_scratch),
            _ => Ok(()),
        }
    }

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

        let entry_size = aligned_data_entry_size(key.len(), val.len());

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
                        // Resuming from a crash during rebuild may subject some already-processed
                        // entries to multiple iterations. If the row pointer is already identical,
                        // this entry was fully processed and we can skip it.
                        return Ok(());
                    }

                    let old_size =
                        aligned_data_entry_size(existing_kv.key().len(), existing_kv.value().len());
                    self.record_recovered_waste(
                        entry,
                        existing_kv.key().len(),
                        existing_kv.value().len(),
                    )?;
                    row.replace_pointer(col, ptr);
                    let h = self.inner.index_file.header_ref();
                    h.num_replaced.fetch_add(1, Ordering::Relaxed);
                    h.written_bytes.fetch_add(entry_size, Ordering::Relaxed);
                    h.waste_bytes.fetch_add(old_size, Ordering::Relaxed);
                    return Ok(());
                }
            }
            if let Some(col) = row.find_free_slot() {
                row.insert(col, hc.sig, ptr);
                let h = self.inner.index_file.header_ref();
                h.num_created.fetch_add(1, Ordering::Relaxed);
                h.written_bytes.fetch_add(entry_size, Ordering::Relaxed);
                self.inner.bump_histogram(entry_size);
                Ok(())
            } else {
                Err(Error::SplitRow(row.split_level.load(Ordering::Relaxed)))
            }
        })
    }

    fn recover_tombstone_entry(
        &self,
        data_file: &Arc<DataFile>,
        ns: KeyNamespace,
        kv: KVRef<'_>,
        match_scratch: &mut Vec<u8>,
    ) -> Result<()> {
        let key = kv.key();
        self.validate_recovered_tombstone_entry(key)?;
        self.inner
            .index_file
            .add_file_waste(data_file.file_idx, aligned_tombstone_entry_waste(key.len()));

        self.inner._mut_op(ns, key, &[], |hc, mut row, key, _| {
            let files = self.inner.data_files.read();
            for (col, entry) in row.iter_matches(hc) {
                let file = files
                    .get(&entry.file_idx())
                    .ok_or(Error::MissingDataFile(entry.file_idx()))?;
                let existing_kv =
                    file.read_kv_into(entry.file_offset(), entry.size_hint(), match_scratch)?;
                if existing_kv.key() == key {
                    let old_size =
                        aligned_data_entry_size(existing_kv.key().len(), existing_kv.value().len());
                    self.record_recovered_waste(
                        entry,
                        existing_kv.key().len(),
                        existing_kv.value().len(),
                    )?;
                    row.remove(col);
                    let h = self.inner.index_file.header_ref();
                    h.num_removed.fetch_add(1, Ordering::Relaxed);
                    h.waste_bytes.fetch_add(old_size, Ordering::Relaxed);
                    return Ok(());
                }
            }
            Ok(())
        })
    }

    fn record_recovered_waste(&self, entry: EntryPointer, klen: usize, vlen: usize) -> Result<()> {
        let old_aligned_len = aligned_data_entry_waste(klen, vlen);
        self.inner
            .index_file
            .add_file_waste(entry.file_idx(), old_aligned_len);
        Ok(())
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
