use crate::files::data_file::DataFile;
use crate::files::index_file::RowLayout;
use crate::types::{
    CandyError, EntryPointer, HashCoordinates, KeyNamespace, MAX_KEY_LEN, MAX_VALUE_LEN,
    MAX_VALUE_LEN_INTERNAL, Result,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::atomic::Ordering;

use super::CandyStore;

#[derive(Debug, Clone)]
pub enum SetOptions {
    Upsert,
    InsertIfVacant,
    ReplaceIfExists(Option<Vec<u8>>),
}

#[derive(Debug, Default, Clone)]
pub struct SetOutcome {
    pub previous: Option<Vec<u8>>,
    pub wrong_value: bool,
}

struct RowMatch {
    index: usize,
    ptr: EntryPointer,
    value: Vec<u8>,
    read_bytes: u32,
}

impl CandyStore {
    #[inline]
    pub(crate) fn ensure_user_value_len(&self, len: usize) -> Result<()> {
        if len > MAX_VALUE_LEN {
            Err(CandyError::ValueTooLarge(len))
        } else {
            Ok(())
        }
    }

    fn find_match_in_row(
        &self,
        row: &RowLayout,
        hc: HashCoordinates,
        ns: KeyNamespace,
        key: &[u8],
    ) -> (Option<RowMatch>, bool) {
        let mut saw_error = false;
        for (idx, ptr) in row.iter_matches(hc) {
            if !ptr.is_data_pointer() {
                continue;
            }

            let file_id = ptr.file_id;
            let data_files_guard = self.inner.data_files.read();
            let data_file = match data_files_guard.get(&file_id) {
                Some(f) => f,
                None => {
                    saw_error = true;
                    continue;
                }
            };

            let kvbuf = match data_file.read_kv(ptr.file_offset, ptr.size_hint()) {
                Ok(kv) => kv,
                Err(_) => {
                    saw_error = true;
                    continue;
                }
            };

            let read_bytes = kvbuf.buf.len() + DataFile::ENTRY_CHECKSUM_LEN;
            self.inner.inner_stats.record_read(read_bytes);

            if kvbuf.ns() != ns as u8 || kvbuf.key() != key {
                continue;
            }

            return (
                Some(RowMatch {
                    index: idx,
                    ptr,
                    value: kvbuf.value().to_vec(),
                    read_bytes: read_bytes as u32,
                }),
                saw_error,
            );
        }
        (None, saw_error)
    }

    pub(crate) fn _set_with_options(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        value: &[u8],
        opts: SetOptions,
    ) -> Result<SetOutcome> {
        if key.len() > MAX_KEY_LEN {
            return Err(CandyError::KeyTooLarge(key.len()));
        }
        if value.len() > MAX_VALUE_LEN_INTERNAL {
            return Err(CandyError::ValueTooLarge(value.len()));
        }
        let hc = HashCoordinates::from_key(
            self.inner.config.hash_key.0,
            self.inner.config.hash_key.1,
            ns,
            key,
        );

        let (outcome, waste) = self
            .inner
            .index_file
            .operate_on_row_mut(hc, |row, header| {
                if let (
                    Some(RowMatch {
                        index: idx,
                        value: old_value,
                        ..
                    }),
                    _,
                ) = self.find_match_in_row(row, hc, ns, key)
                {
                    match &opts {
                        SetOptions::InsertIfVacant => {
                            return Ok((
                                SetOutcome {
                                    previous: Some(old_value),
                                    wrong_value: false,
                                },
                                None,
                            ));
                        }
                        _ => {
                            if let SetOptions::ReplaceIfExists(Some(expected_val)) = &opts
                                && &old_value != expected_val
                            {
                                return Ok((
                                    SetOutcome {
                                        previous: Some(old_value),
                                        wrong_value: true,
                                    },
                                    None,
                                ));
                            }

                            let old_ptr = row.pointers[idx];
                            let old_checksum = old_ptr.calc_checksum(hc.signature);

                            let (active_id, new_offset, size) =
                                self.inner.append_kv_to_active_file(ns, key, value)?;

                            row.pointers[idx] =
                                EntryPointer::new(active_id, new_offset, hc.row_selector, size);
                            header.num_updates.fetch_add(1, Ordering::Relaxed);

                            let new_checksum = row.pointers[idx].calc_checksum(hc.signature);
                            header
                                .index_checksum
                                .fetch_xor(old_checksum ^ new_checksum, Ordering::Relaxed);

                            header.record_entry_size(key.len() + value.len());

                            let waste = if old_ptr.is_data_pointer() {
                                Some((old_ptr.file_id, old_ptr.size_hint()))
                            } else {
                                None
                            };

                            return Ok((
                                SetOutcome {
                                    previous: Some(old_value),
                                    wrong_value: false,
                                },
                                waste,
                            ));
                        }
                    }
                }

                match opts {
                    SetOptions::ReplaceIfExists(_) => Ok((
                        SetOutcome {
                            previous: None,
                            wrong_value: false,
                        },
                        None,
                    )),
                    SetOptions::InsertIfVacant | SetOptions::Upsert => {
                        if let Some(idx) = row
                            .signatures
                            .iter()
                            .position(|&sig| sig == HashCoordinates::INVALID_SIG)
                        {
                            let (active_id, new_offset, size) =
                                self.inner.append_kv_to_active_file(ns, key, value)?;

                            row.pointers[idx] =
                                EntryPointer::new(active_id, new_offset, hc.row_selector, size);
                            row.signatures[idx] = hc.signature;
                            header.num_inserts.fetch_add(1, Ordering::Relaxed);

                            header.index_checksum.fetch_xor(
                                row.pointers[idx].calc_checksum(hc.signature),
                                Ordering::Relaxed,
                            );

                            header.record_entry_size(key.len() + value.len());

                            Ok((
                                SetOutcome {
                                    previous: None,
                                    wrong_value: false,
                                },
                                None,
                            ))
                        } else {
                            Err(CandyError::SplitRow)
                        }
                    }
                }
            })?;

        self.inner.maybe_rotate_data_file()?;

        if let Some((file_id, wasted_bytes)) = waste {
            use crate::types::SpecialEntryType;
            let _ = self._upsert_special_key(
                KeyNamespace::StatsWastedBytes,
                SpecialEntryType::WastedBytes,
                file_id as u64,
                |old| old.unwrap_or(0) + wasted_bytes as u64,
            );
        }

        Ok(outcome)
    }

    pub(crate) fn _get(&self, ns: KeyNamespace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() > MAX_KEY_LEN {
            return Err(CandyError::KeyTooLarge(key.len()));
        }
        let hc = HashCoordinates::from_key(
            self.inner.config.hash_key.0,
            self.inner.config.hash_key.1,
            ns,
            key,
        );

        let mut attempt = 0;
        loop {
            let mut saw_stale = false;
            let res = self.inner.index_file.operate_on_row(hc, |row| {
                let (found, error) = self.find_match_in_row(row, hc, ns, key);
                if error {
                    saw_stale = true;
                }
                if let Some(RowMatch { value: val, .. }) = found {
                    return Ok(Some(val));
                }
                Ok(None)
            });

            match res {
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) if saw_stale && attempt == 0 => {
                    attempt += 1;
                    continue;
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn _set(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        Ok(self
            ._set_with_options(ns, key, value, SetOptions::Upsert)?
            .previous)
    }

    pub(crate) fn _remove(&self, ns: KeyNamespace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.len() > MAX_KEY_LEN {
            return Err(CandyError::KeyTooLarge(key.len()));
        }
        let hc = HashCoordinates::from_key(
            self.inner.config.hash_key.0,
            self.inner.config.hash_key.1,
            ns,
            key,
        );

        let mut stats_update = None;

        let res = self
            .inner
            .index_file
            .operate_on_row_mut(hc, |row, header| {
                if let (
                    Some(RowMatch {
                        index: idx,
                        ptr,
                        value: old_value,
                        read_bytes: full_size,
                    }),
                    _,
                ) = self.find_match_in_row(row, hc, ns, key)
                {
                    let (active_id, _, _) = self.inner.append_tombstone_to_active_file(ns, key)?;

                    let old_checksum = row.pointers[idx].calc_checksum(hc.signature);

                    row.signatures[idx] = HashCoordinates::INVALID_SIG;
                    row.pointers[idx] = EntryPointer::INVALID_PTR;
                    header.num_deletes.fetch_add(1, Ordering::Relaxed);

                    header
                        .index_checksum
                        .fetch_xor(old_checksum, Ordering::Relaxed);

                    if ptr.file_id != active_id {
                        let wasted = full_size as u64;
                        stats_update = Some((ptr.file_id, wasted));
                    }
                    return Ok(Some(old_value));
                }

                Ok(None)
            })?;

        self.inner.maybe_rotate_data_file()?;

        if let Some((file_id, wasted)) = stats_update {
            self.update_file_stats(file_id, wasted, 1)?;
        }

        Ok(res)
    }

    pub(crate) fn logical_read_guard(
        &self,
        ns: KeyNamespace,
        key: &[u8],
    ) -> parking_lot::RwLockReadGuard<'_, ()> {
        let mut hasher = DefaultHasher::new();
        hasher.write_u8(ns as u8);
        hasher.write(key);
        let hash = hasher.finish();
        let lock_index = (hash as usize) & (self.inner.key_locks.len() - 1);
        self.inner.key_locks[lock_index].read()
    }

    pub(crate) fn logical_write_guard(
        &self,
        ns: KeyNamespace,
        key: &[u8],
    ) -> parking_lot::RwLockWriteGuard<'_, ()> {
        let mut hasher = DefaultHasher::new();
        hasher.write_u8(ns as u8);
        hasher.write(key);
        let hash = hasher.finish();
        let lock_index = (hash as usize) & (self.inner.key_locks.len() - 1);
        self.inner.key_locks[lock_index].write()
    }
}
