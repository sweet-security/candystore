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
        FILE_OFFSET_ALIGNMENT, MAX_REPRESENTABLE_FILE_SIZE, is_resettable_open_error,
        parse_data_file_idx, sync_dir,
    },
    types::{Config, Error, INITIAL_DATA_FILE_ORDINAL, Result},
};

use super::{CandyStore, OpenState, StoreInner};

impl CandyStore {
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
        let mut seen_ordinals = HashSet::new();
        let mut active_file_idx = 0;
        let mut active_file_ordinal = INITIAL_DATA_FILE_ORDINAL;

        for entry in std::fs::read_dir(base_path).map_err(Error::IOError)? {
            let entry = entry.map_err(Error::IOError)?;
            let path = entry.path();
            let Some(file_idx) = parse_data_file_idx(&path) else {
                continue;
            };
            let data_file = Arc::new(DataFile::open(base_path, config.clone(), file_idx)?);
            if !seen_ordinals.insert(data_file.file_ordinal) {
                return Err(crate::internal::invalid_data_error(
                    "duplicate data file ordinal",
                ));
            }
            if data_files.is_empty() || data_file.file_ordinal > active_file_ordinal {
                active_file_idx = file_idx;
                active_file_ordinal = data_file.file_ordinal;
            }
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
            Err(err) if config.reset_on_invalid_data && is_resettable_open_error(&err) => {
                Self::clear_db_files(base_path)?;
                Self::open_state(base_path, config)
            }
            Err(err) => Err(err),
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
    /// If `config.reset_on_invalid_data` is enabled, opening may remove all
    /// contents and recreate fresh store files when the on-disk data is
    /// corrupt. While the store is open, the active `.lockfile` is preserved
    /// so the directory remains locked against concurrent opens.
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
