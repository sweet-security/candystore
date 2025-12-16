use crate::files::data_file::DataFile;
use crate::store::CandyStore;
use crate::types::{KeyNamespace, ROW_WIDTH, Result};

/// An iterator over the entries in the CandyStore.
pub struct CandyStoreIterator<'a> {
    store: &'a CandyStore,
    row_idx: usize,
    col_idx: usize,
    skip_special: bool,
    user_only: bool,
}

impl<'a> CandyStoreIterator<'a> {
    pub(crate) fn new(store: &'a CandyStore, skip_special: bool, user_only: bool) -> Self {
        Self {
            store,
            row_idx: 0,
            col_idx: 0,
            skip_special,
            user_only,
        }
    }
}

impl<'a> Iterator for CandyStoreIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.row_idx >= self.store.inner.index_file.num_rows() {
                return None;
            }

            let current_row = self.row_idx;
            let current_col = self.col_idx;

            let entry = self
                .store
                .inner
                .index_file
                .get_entry(current_row, current_col);

            self.col_idx += 1;
            if self.col_idx >= ROW_WIDTH {
                self.col_idx = 0;
                self.row_idx += 1;
            }

            if let Some(ptr) = entry {
                if !ptr.is_valid() || (self.skip_special && ptr.is_special_value()) {
                    continue;
                }
                let file_id = ptr.file_id;
                let file_offset = ptr.file_offset;
                let size_hint = ptr.size_hint();

                let data_files_guard = self.store.inner.data_files.read();
                let data_file = data_files_guard.get(&file_id);

                match data_file {
                    Some(f) => match f.read_kv(file_offset, size_hint) {
                        Ok(kvbuf) => {
                            let read_bytes = kvbuf.buf.len() + DataFile::ENTRY_CHECKSUM_LEN;
                            self.store.inner.inner_stats.record_read(read_bytes);

                            if self.user_only && kvbuf.ns() != KeyNamespace::User as u8 {
                                continue;
                            }
                            return Some(Ok((kvbuf.key().to_vec(), kvbuf.value().to_vec())));
                        }
                        Err(e) => {
                            // Read failed. Check if index changed.
                            let new_entry = self
                                .store
                                .inner
                                .index_file
                                .get_entry(current_row, current_col);

                            if let Some(new_ptr) = new_entry
                                && (new_ptr.file_id != ptr.file_id
                                    || new_ptr.file_offset != ptr.file_offset)
                            {
                                // Index changed, retry this slot
                                self.row_idx = current_row;
                                self.col_idx = current_col;
                                continue;
                            }
                            return Some(Err(e));
                        }
                    },
                    None => {
                        // File missing. Check if index changed.
                        let new_entry = self
                            .store
                            .inner
                            .index_file
                            .get_entry(current_row, current_col);

                        if let Some(new_ptr) = new_entry
                            && (new_ptr.file_id != ptr.file_id
                                || new_ptr.file_offset != ptr.file_offset)
                        {
                            // Index changed, retry this slot
                            self.row_idx = current_row;
                            self.col_idx = current_col;
                            continue;
                        }
                        // Index same (dangling pointer) or invalid. Skip.
                        continue;
                    }
                };
            }
        }
    }
}
