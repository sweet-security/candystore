use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

use crate::{
    data_file::DataFile,
    index_file::IndexFile,
    internal::{
        DATA_FILE_SIGNATURE, DATA_FILE_VERSION, FILE_OFFSET_ALIGNMENT, INDEX_FILE_SIGNATURE,
        INDEX_FILE_VERSION, MAX_REPRESENTABLE_FILE_SIZE, index_file_path, index_rows_file_path,
        is_resettable_open_error, parse_data_file_idx, read_available_at, sync_dir,
    },
    types::{Config, Error, INITIAL_DATA_FILE_ORDINAL, Result},
};

use super::{CandyStore, OpenState, StoreInner};

impl CandyStore {
    fn recreate_index_files(base_path: &Path) -> Result<()> {
        let mut removed_any = false;
        for path in [index_file_path(base_path), index_rows_file_path(base_path)] {
            match std::fs::remove_file(&path) {
                Ok(()) => removed_any = true,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(Error::IOError(err)),
            }
        }
        if removed_any {
            sync_dir(base_path)?;
        }
        Ok(())
    }

    fn existing_version_if_signature_matches(
        path: &Path,
        signature: &[u8; 8],
    ) -> Result<Option<u32>> {
        let file = match std::fs::File::options().read(true).open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::IOError(err)),
        };

        let header = read_available_at(&file, 12, 0).map_err(Error::IOError)?;
        if header.len() < 12 || &header[0..8] != signature {
            return Ok(None);
        }

        Ok(Some(u32::from_le_bytes(header[8..12].try_into().unwrap())))
    }

    fn has_unrecognized_index_version(base_path: &Path) -> Result<bool> {
        Ok(matches!(
            Self::existing_version_if_signature_matches(
                &index_file_path(base_path),
                INDEX_FILE_SIGNATURE,
            )?,
            Some(version) if version != INDEX_FILE_VERSION
        ))
    }

    fn data_files_use_recognized_versions(base_path: &Path) -> Result<bool> {
        let mut found_any = false;

        for entry in std::fs::read_dir(base_path).map_err(Error::IOError)? {
            let entry = entry.map_err(Error::IOError)?;
            let path = entry.path();
            if parse_data_file_idx(&path).is_none() {
                continue;
            }

            let Some(version) =
                Self::existing_version_if_signature_matches(&path, DATA_FILE_SIGNATURE)?
            else {
                return Ok(false);
            };

            if version != DATA_FILE_VERSION {
                return Ok(false);
            }

            found_any = true;
        }

        Ok(found_any)
    }

    fn should_port_to_current_format(base_path: &Path) -> Result<bool> {
        if !Self::has_unrecognized_index_version(base_path)? {
            return Ok(false);
        }

        Self::data_files_use_recognized_versions(base_path)
    }

    fn build_store(
        base_path: std::path::PathBuf,
        config: Arc<Config>,
        lockfile: fslock::LockFile,
    ) -> Result<Self> {
        let state = Self::open_or_reset_state(&base_path, config.clone())?;
        let num_logical_locks = config.max_concurrency.max(8).next_power_of_two();

        Ok(Self {
            inner: Arc::new(StoreInner::new(base_path, config, state, num_logical_locks)),
            _lockfile: lockfile,
            compaction_thd: parking_lot::Mutex::new(None),
            checkpoint_thd: parking_lot::Mutex::new(None),
            allow_clean_shutdown: std::sync::atomic::AtomicBool::new(true),
        })
    }

    fn clear_db_files(base_path: &Path) -> Result<()> {
        let mut removed_any = false;
        for entry in std::fs::read_dir(base_path).map_err(Error::IOError)? {
            let entry = entry.map_err(Error::IOError)?;
            let path = entry.path();
            if path.file_name().and_then(|name| name.to_str()) == Some(".lockfile") {
                continue;
            }

            let file_type = entry.file_type().map_err(Error::IOError)?;
            if file_type.is_dir() {
                std::fs::remove_dir_all(&path).map_err(Error::IOError)?;
                removed_any = true;
            } else if file_type.is_file() || file_type.is_symlink() {
                std::fs::remove_file(&path).map_err(Error::IOError)?;
                removed_any = true;
            }
        }
        if removed_any {
            sync_dir(base_path)?;
        }
        Ok(())
    }

    fn open_state(base_path: &Path, config: Arc<Config>) -> Result<OpenState> {
        let index_file = IndexFile::open(base_path, config.clone())?;
        let mut data_files = HashMap::new();
        let mut file_ordinals = Vec::new();
        let mut seen_ordinals = HashSet::new();
        let mut active_file_idx = 0;
        let mut active_file_ordinal = INITIAL_DATA_FILE_ORDINAL;

        for entry in std::fs::read_dir(base_path).map_err(Error::IOError)? {
            let entry = entry.map_err(Error::IOError)?;
            let path = entry.path();
            let Some(file_idx) = parse_data_file_idx(&path) else {
                continue;
            };
            let file_ordinal = DataFile::read_ordinal(base_path, file_idx)?;
            if !seen_ordinals.insert(file_ordinal) {
                return Err(crate::internal::invalid_data_error(
                    "duplicate data file ordinal",
                ));
            }
            if file_ordinals.is_empty() || file_ordinal > active_file_ordinal {
                active_file_idx = file_idx;
                active_file_ordinal = file_ordinal;
            }
            file_ordinals.push((file_idx, file_ordinal));
        }

        for (file_idx, file_ordinal) in file_ordinals {
            let validate_tail = file_ordinal == active_file_ordinal;
            let data_file = Arc::new(DataFile::open(
                base_path,
                config.clone(),
                file_idx,
                validate_tail,
            )?);
            data_files.insert(file_idx, data_file);
        }

        if data_files.is_empty() {
            let data_file = Arc::new(DataFile::create(
                base_path,
                config.clone(),
                active_file_idx,
                active_file_ordinal,
            )?);
            data_files.insert(active_file_idx, data_file);
        }

        Ok(OpenState {
            index_file,
            data_files,
            active_file_idx,
            active_file_ordinal,
        })
    }

    fn acquire_lockfile(base_path: &Path) -> Result<fslock::LockFile> {
        let lockfile_path = base_path.join(".lockfile");
        let mut lockfile = fslock::LockFile::open(&lockfile_path).map_err(Error::IOError)?;
        if !lockfile.try_lock().unwrap_or(false) {
            let content =
                String::from_utf8_lossy(&std::fs::read(&lockfile_path).unwrap_or("<empty>".into()))
                    .into_owned();

            return Err(Error::LockfileTaken(lockfile_path, content));
        }

        let content = format!(
            "[{}] {}",
            std::process::id(),
            std::env::args().collect::<Vec<_>>().join(" ")
        );
        _ = std::fs::write(&lockfile_path, content).map_err(Error::IOError);
        Ok(lockfile)
    }

    fn open_or_reset_state(base_path: &Path, config: Arc<Config>) -> Result<OpenState> {
        match Self::open_state(base_path, config.clone()) {
            Ok(state) => Ok(state),
            Err(err) => {
                if config.port_to_current_format && Self::should_port_to_current_format(base_path)?
                {
                    Self::recreate_index_files(base_path)?;
                    return Self::open_state(base_path, config);
                }

                if config.reset_on_invalid_data && is_resettable_open_error(&err) {
                    Self::clear_db_files(base_path)?;
                    return Self::open_state(base_path, config);
                }

                Err(err)
            }
        }
    }

    fn normalize_config_for_path(base_path: &Path, config: Config) -> Result<Arc<Config>> {
        let max_data_file_size = config.max_data_file_size.min(MAX_REPRESENTABLE_FILE_SIZE);
        let mut normalized = Config {
            max_data_file_size,
            compaction_min_threshold: config
                .compaction_min_threshold
                .min((max_data_file_size as f64 * 0.8) as u32),
            remap_scaler: config.remap_scaler.clamp(1, 4),
            checkpoint_interval: config.checkpoint_interval.map(|d| {
                if d.is_zero() {
                    Duration::from_millis(100)
                } else {
                    d
                }
            }),
            checkpoint_delta_bytes: config
                .checkpoint_delta_bytes
                .map(|b| b.max(FILE_OFFSET_ALIGNMENT as usize)),
            ..config
        };

        match IndexFile::existing_hash_key(base_path) {
            Ok(Some(hash_key)) => normalized.hash_key = hash_key,
            Ok(None) => {}
            Err(err) if is_resettable_open_error(&err) => {}
            Err(err) => return Err(err),
        }

        Ok(Arc::new(normalized))
    }

    /// Opens a store at `path`, creating it if needed.
    ///
    /// If `config.port_to_current_format` is enabled, opening may recreate the
    /// index files when their format is outdated but the data files are still
    /// recognized. If `config.reset_on_invalid_data` is enabled, opening may
    /// remove all contents and recreate fresh store files when the on-disk
    /// data is corrupt. While the store is open, the active `.lockfile` is
    /// preserved so the directory remains locked against concurrent opens.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path).map_err(Error::IOError)?;

        let lockfile = Self::acquire_lockfile(&base_path)?;
        let config = Self::normalize_config_for_path(&base_path, config)?;

        let store = Self::build_store(base_path.clone(), config.clone(), lockfile)?;
        match store.recover_index() {
            Ok(()) => {
                store.start_checkpoint_worker();
                store.start_compaction();
                Ok(store)
            }
            Err(err) if config.reset_on_invalid_data && is_resettable_open_error(&err) => {
                let store = std::mem::ManuallyDrop::new(store);
                let inner = unsafe { std::ptr::read(&store.inner) };
                let lockfile = unsafe { std::ptr::read(&store._lockfile) };
                let compaction_thd = unsafe { std::ptr::read(&store.compaction_thd) };
                let checkpoint_thd = unsafe { std::ptr::read(&store.checkpoint_thd) };
                let _allow_clean_shutdown = unsafe { std::ptr::read(&store.allow_clean_shutdown) };
                drop(compaction_thd);
                drop(checkpoint_thd);
                drop(inner);

                Self::clear_db_files(&base_path)?;

                let recovered = Self::build_store(base_path, config, lockfile)?;
                recovered.recover_index()?;
                recovered.start_checkpoint_worker();
                recovered.start_compaction();
                Ok(recovered)
            }
            Err(err) => Err(err),
        }
    }

    /// Clears the store and recreates a fresh empty database in the same
    /// directory.
    ///
    /// This removes all directory contents, including unrelated files and
    /// subdirectories, before recreating the store files. While the store is
    /// open, the active `.lockfile` is preserved so the directory remains
    /// locked against concurrent opens.
    pub fn clear(&self) -> Result<()> {
        // stop bg thread
        self.stop_compaction();
        self.stop_checkpoint_worker();

        // now we're single-threaded. take all locks and clear state
        self.inner.reset()?;

        self.allow_clean_shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.start_checkpoint_worker();
        self.start_compaction();

        Ok(())
    }
}
