use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    data_file::DataFile,
    index_file::IndexFile,
    internal::{MAX_REPRESENTABLE_FILE_SIZE, is_resettable_open_error, parse_data_file_idx},
    types::{Config, Error, INITIAL_DATA_FILE_ORDINAL, RebuildStrategy, Result},
};

use super::{CandyStore, DirtyOpenAction, OpenState, StoreInner};

impl CandyStore {
    fn clear_db_files(base_path: &Path) -> Result<()> {
        Self::clear_directory_contents(base_path)
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

        let was_clean_shutdown = {
            let header = index_file.header_ref();
            let was_clean = header.dirty.load(Ordering::Acquire) == 0;
            header.dirty.store(1, Ordering::Release);
            index_file.flush_header()?;
            was_clean
        };

        Ok(OpenState {
            index_file,
            data_files,
            active_file_idx,
            active_file_ordinal,
            was_clean_shutdown,
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
                let mut state = Self::open_state(base_path, config)?;
                state.was_clean_shutdown = false;
                Ok(state)
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

    fn resolve_dirty_open(
        base_path: &Path,
        config: Arc<Config>,
        state: OpenState,
    ) -> Result<(OpenState, DirtyOpenAction)> {
        if state.was_clean_shutdown {
            return Ok((state, DirtyOpenAction::None));
        }

        let action = match config.rebuild_strategy {
            RebuildStrategy::FailIfDirty => return Err(Error::DirtyIndex),
            RebuildStrategy::RebuildIfDirty => DirtyOpenAction::RebuildIndex,
            RebuildStrategy::ResetDBIfDirty => DirtyOpenAction::ResetDb,
            RebuildStrategy::TrustDirtyIndexIfChecksumCorrectOrFail => {
                state.index_file.verify_row_checksums()?;
                DirtyOpenAction::TrustIndex
            }
            RebuildStrategy::TrustDirtyIndexIfChecksumCorrectOrRebuild => {
                match state.index_file.verify_row_checksums() {
                    Ok(()) => DirtyOpenAction::TrustIndex,
                    Err(Error::IOError(io_err))
                        if io_err.kind() == std::io::ErrorKind::InvalidData =>
                    {
                        DirtyOpenAction::RebuildIndex
                    }
                    Err(err) => return Err(err),
                }
            }
        };

        if matches!(action, DirtyOpenAction::ResetDb) {
            drop(state);
            Self::clear_db_files(base_path)?;
            let mut reset_state = Self::open_state(base_path, config)?;
            reset_state.was_clean_shutdown = false;
            return Ok((reset_state, DirtyOpenAction::ResetDb));
        }

        Ok((state, action))
    }

    /// Opens a store at `path`, creating it if needed.
    ///
    /// If `config.reset_on_invalid_data` is enabled, or if
    /// `config.rebuild_strategy` is `ResetDBIfDirty`, opening may reset the
    /// database directory by removing all contents and recreating fresh store
    /// files. While the store is open, the active `.lockfile` is preserved so
    /// the directory remains locked against concurrent opens.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path).map_err(Error::IOError)?;

        let lockfile = Self::acquire_lockfile(&base_path)?;
        let config = Self::normalize_config_for_path(&base_path, config)?;

        let state = Self::open_or_reset_state(&base_path, config.clone())?;
        let (state, dirty_open_action) =
            Self::resolve_dirty_open(&base_path, config.clone(), state)?;
        let was_clean_shutdown = state.was_clean_shutdown;
        let num_logical_locks = config.max_concurrency.max(8).next_power_of_two();

        let mut store = Self {
            inner: Arc::new(StoreInner::new(
                base_path,
                config.clone(),
                state,
                num_logical_locks,
            )),
            _lockfile: lockfile,
            compaction_thd: None,
            allow_clean_shutdown: was_clean_shutdown,
            was_clean_shutdown,
        };

        if !was_clean_shutdown {
            match dirty_open_action {
                DirtyOpenAction::None | DirtyOpenAction::ResetDb | DirtyOpenAction::TrustIndex => {}
                DirtyOpenAction::RebuildIndex => store.recover_index()?,
            }
            store.allow_clean_shutdown = true;
        }

        store.start_compaction();
        Ok(store)
    }

    /// Clears the store and recreates a fresh empty database in the same
    /// directory.
    ///
    /// This removes all directory contents, including unrelated files and
    /// subdirectories, before recreating the store files. While the store is
    /// open, the active `.lockfile` is preserved so the directory remains
    /// locked against concurrent opens.
    pub fn clear(&mut self) -> Result<()> {
        let base_path = self.inner.base_path.clone();
        let config = self.inner.config.clone();
        let num_logical_locks = self.inner.logical_locks.len();

        self.stop_compaction();

        self.inner.data_files.write().clear();
        Self::clear_db_files(base_path.as_path())?;

        let state = Self::open_state(base_path.as_path(), config.clone())?;
        self.inner = Arc::new(StoreInner::new(base_path, config, state, num_logical_locks));

        self.allow_clean_shutdown = true;
        self.was_clean_shutdown = true;
        self.start_compaction();

        Ok(())
    }
}
