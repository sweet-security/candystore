use crate::files::data_file::DataFile;
use crate::files::index_file::IndexFile;
use crate::types::{
    CandyError, Config, HashCoordinates, KeyNamespace, MAX_FILE_ID, RecoveryMode, Result,
    SpecialEntryType,
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::error;

pub(crate) struct InnerStats {
    pub num_positive_lookups: AtomicU64,
    pub num_negative_lookups: AtomicU64,
    pub num_read_ops: AtomicU64,
    pub num_read_bytes: AtomicU64,
    pub num_write_ops: AtomicU64,
    pub num_write_bytes: AtomicU64,
}

impl InnerStats {
    #[inline]
    pub fn record_lookup(&self, found: bool) {
        if found {
            self.num_positive_lookups.fetch_add(1, Ordering::Relaxed);
        } else {
            self.num_negative_lookups.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_read(&self, bytes: usize) {
        self.num_read_ops.fetch_add(1, Ordering::Relaxed);
        self.num_read_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_write(&self, bytes: usize) {
        self.num_write_ops.fetch_add(1, Ordering::Relaxed);
        self.num_write_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

pub(crate) struct CandyStoreInner {
    pub index_file: IndexFile,
    pub active_file_id: AtomicU64,
    pub data_files: RwLock<HashMap<u16, Arc<DataFile>>>,
    pub rotate_lock: Mutex<()>,
    pub rotate_requested: AtomicU64,
    pub config: Config,
    pub dir_path: PathBuf,
    pub key_locks: Vec<RwLock<()>>,
    pub inner_stats: Arc<InnerStats>,
}

impl CandyStoreInner {
    pub(crate) fn get_special_key(
        &self,
        ns: KeyNamespace,
        special_type: SpecialEntryType,
        key: u64,
    ) -> Result<Option<u64>> {
        let key_bytes = key.to_le_bytes();
        let hc = HashCoordinates::from_key(
            self.config.hash_key.0,
            self.config.hash_key.1,
            ns,
            &key_bytes,
        );
        self.index_file.operate_on_row(hc, |row| {
            for (_, ptr) in row.iter_matches(hc) {
                if let Some((type_id, val)) = ptr.get_special_value()
                    && type_id == special_type as u8
                {
                    return Ok(Some(val));
                }
            }
            Ok(None)
        })
    }

    fn find_next_file_id(active_id: u16, data_files: &HashMap<u16, Arc<DataFile>>) -> Result<u16> {
        let mut new_id = active_id + 1;
        if new_id > MAX_FILE_ID {
            new_id = 0;
        }

        let mut remaining_iters = MAX_FILE_ID;
        while data_files.contains_key(&new_id) {
            new_id += 1;
            if new_id > MAX_FILE_ID {
                new_id = 0;
            }
            if remaining_iters == 0 {
                return Err(CandyError::MaxDataFilesReached);
            }
            remaining_iters -= 1;
        }
        Ok(new_id)
    }

    pub(crate) fn maybe_rotate_data_file(&self) -> Result<bool> {
        let requested = self.rotate_requested.load(Ordering::Relaxed);
        if requested == u64::MAX {
            return Ok(false);
        }
        let Some(_guard) = self.rotate_lock.try_lock() else {
            // another thread is already rotating
            return Ok(false);
        };
        let requested = self.rotate_requested.load(Ordering::Relaxed);
        if requested == u64::MAX {
            return Ok(false);
        }

        let res = self._rotate_data_file(requested as u16)?;
        _ = self.rotate_requested.compare_exchange(
            requested,
            u64::MAX,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        Ok(res)
    }

    fn _rotate_data_file(&self, requested_id: u16) -> Result<bool> {
        let active_id = self.active_file_id.load(Ordering::Relaxed) as u16;
        if active_id != requested_id {
            return Ok(false);
        }

        let mut data_files_guard = self.data_files.write();
        if data_files_guard.len() >= MAX_FILE_ID as usize {
            return Err(CandyError::MaxDataFilesReached);
        }

        let active_id = self.active_file_id.load(Ordering::Relaxed) as u16;
        if active_id != requested_id {
            return Ok(false);
        }

        let current_serial = if let Some(old_file) = data_files_guard.get(&active_id) {
            if let Err(e) =
                old_file.flush_checkpoint(old_file.write_offset.load(Ordering::SeqCst), 0)
            {
                error!("Failed to flush old data file during rotation: {}", e);
            }
            old_file.serial
        } else {
            0
        };

        let new_id = Self::find_next_file_id(active_id, &data_files_guard)?;
        let path = self.dir_path.join(format!("data_{:05}.db", new_id));
        let data_file = DataFile::open_trunc(&path, Some(current_serial + 1))?;
        data_files_guard.insert(new_id, Arc::new(data_file));
        self.active_file_id.store(new_id as u64, Ordering::Relaxed);

        Ok(true)
    }

    fn append_entry_to_active_file<F>(&self, append_op: F) -> Result<(u16, u32, u32)>
    where
        F: FnOnce(&DataFile) -> Result<(u32, u32)>,
    {
        let data_files_guard = self.data_files.read();
        let active_id = self.active_file_id.load(Ordering::Relaxed) as u16;
        let active_file = data_files_guard
            .get(&active_id)
            .ok_or(CandyError::MissingDataFile(active_id))?;

        let (new_offset, size) = append_op(active_file)?;
        self.inner_stats.record_write(size as usize);

        if new_offset + size > self.config.max_data_file_size {
            _ = self.rotate_requested.compare_exchange(
                u64::MAX,
                active_id as u64,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }

        Ok((active_id, new_offset, size))
    }

    pub(crate) fn overwrite_inplace(
        &self,
        file_id: u16,
        only_active: bool,
        ns: KeyNamespace,
        key: &[u8],
        value: &[u8],
        entry_offset: u32,
    ) -> Result<bool> {
        let data_files = self.data_files.read();
        let Some(data_file) = data_files.get(&file_id) else {
            return Ok(false);
        };
        if only_active && file_id != self.active_file_id.load(Ordering::Relaxed) as u16 {
            return Ok(false);
        }
        if data_file.is_under_compaction.load(Ordering::SeqCst) {
            return Ok(false);
        }
        data_file.overwrite_inplace(ns, key, value, entry_offset)?;

        // Double check: If compaction started while we were writing, we might have
        // created a race where compaction read the old value but we wrote the new one.
        // By returning false here, we force the caller to append the new value to the
        // active file, ensuring the index is updated and compaction discards its stale read.
        if data_file.is_under_compaction.load(Ordering::SeqCst) {
            return Ok(false);
        }

        self.inner_stats.record_write(key.len() + value.len());
        Ok(true)
    }

    pub(crate) fn append_kv_to_active_file(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        value: &[u8],
    ) -> Result<(u16, u32, u32)> {
        self.append_entry_to_active_file(|f| f.append_kv(ns, key, value))
    }

    pub(crate) fn append_tombstone_to_active_file(
        &self,
        ns: KeyNamespace,
        key: &[u8],
    ) -> Result<(u16, u32, u32)> {
        self.append_entry_to_active_file(|f| f.append_tombstone(ns, key))
    }
}

fn reset_data_files(dir_path: &Path) -> Result<(HashMap<u16, Arc<DataFile>>, u16)> {
    // Delete all files in directory
    for entry in std::fs::read_dir(dir_path).map_err(CandyError::IOError)? {
        let entry = entry.map_err(CandyError::IOError)?;
        let path = entry.path();
        if path.is_file() {
            std::fs::remove_file(path).map_err(CandyError::IOError)?;
        }
    }

    // Create fresh state
    let data_file = DataFile::open_trunc(&dir_path.join("data_00000.db"), Some(0))?;
    let mut map = HashMap::new();
    map.insert(0, Arc::new(data_file));
    Ok((map, 0))
}

pub(super) fn load_data_files(
    dir_path: &Path,
    config: &Config,
) -> Result<(HashMap<u16, Arc<DataFile>>, u16)> {
    let mut data_files = HashMap::new();
    let mut max_serial = 0;
    let mut active_file_id = 0;

    for entry in std::fs::read_dir(dir_path).map_err(CandyError::IOError)? {
        let entry = entry.map_err(CandyError::IOError)?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.starts_with("data_")
            && name.ends_with(".db")
        {
            let id_str = &name[5..name.len() - 3];
            if let Ok(id) = id_str.parse::<u16>() {
                match DataFile::open(&path, None) {
                    Ok(data_file) => {
                        if data_file.serial > max_serial {
                            max_serial = data_file.serial;
                            active_file_id = id;
                        }
                        data_files.insert(id, Arc::new(data_file));
                    }
                    Err(e) => {
                        if config.recovery_mode == RecoveryMode::ClearAllIfCorrupted {
                            tracing::warn!(
                                "Data file {} corrupt: {}. Resetting store.",
                                path.display(),
                                e
                            );
                            // Drop existing handles to allow deletion
                            drop(data_files);
                            return reset_data_files(dir_path);
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    if !data_files.is_empty() {
        if let Some(active_file) = data_files.get(&active_file_id) {
            let start_offset = active_file.checkpoint_offset as u32;
            let mut last_valid_offset = start_offset;
            let mut truncated = false;

            for entry in active_file.iter_entries_from(start_offset) {
                match entry {
                    Ok((offset, size, _)) => {
                        last_valid_offset = offset + size;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Found garbage in active file {}: {}. Truncating.",
                            active_file_id,
                            e
                        );
                        active_file.truncate(last_valid_offset)?;
                        truncated = true;
                        break;
                    }
                }
            }

            if !truncated {
                let current_len = active_file
                    .file
                    .metadata()
                    .map_err(CandyError::IOError)?
                    .len();
                let valid_len = last_valid_offset as u64 + crate::types::PAGE_SIZE as u64;
                if current_len > valid_len {
                    tracing::warn!(
                        "Found trailing garbage in active file {}. Truncating.",
                        active_file_id
                    );
                    active_file.truncate(last_valid_offset)?;
                }
            }
        }
    }

    if data_files.is_empty() {
        let data_file = DataFile::open_trunc(&dir_path.join("data_00000.db"), Some(0))?;
        data_files.insert(0, Arc::new(data_file));
    }

    Ok((data_files, active_file_id))
}

pub(super) fn open_or_recover_index(
    index_path: &Path,
    config: &Config,
) -> Result<(IndexFile, bool)> {
    match IndexFile::open(index_path, config) {
        Ok(f) => Ok((f, false)),
        Err(e) => match config.recovery_mode {
            RecoveryMode::FailIfCorrupted => Err(e),
            RecoveryMode::RebuildIndexIfCorrupted | RecoveryMode::ClearAllIfCorrupted => {
                error!("Failed to open index file: {}. Attempting recovery...", e);
                if index_path.exists() {
                    std::fs::remove_file(index_path).map_err(CandyError::IOError)?;
                }
                Ok((IndexFile::open(index_path, config)?, true))
            }
        },
    }
}
