mod api;
mod background;
mod inner;
mod iterator;
pub(crate) mod ops;
mod recovery;
mod special;

use crate::{
    store::inner::InnerStats,
    types::{CandyError, Config, MAX_DATA_FILE_SIZE, Result},
};
use fslock::LockFile;
use parking_lot::{Condvar, Mutex, RwLock};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::thread::JoinHandle;

pub use api::{GetOrCreateStatus, ReplaceStatus, SetStatus};

pub(crate) use inner::CandyStoreInner;
use inner::{load_data_files, open_or_recover_index};
use recovery::check_consistency_and_recover;

/// The main CandyStore struct.
///
/// CandyStore is a persistent, thread-safe key-value store optimized for high performance.
/// It supports basic key-value operations, as well as lists and queues.
pub struct CandyStore {
    pub(crate) inner: Arc<CandyStoreInner>,
    lock_file: LockFile,
    compaction_thread: Option<JoinHandle<()>>,
    flush_thread: Option<JoinHandle<()>>,
    shutdown: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for CandyStore {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.shutdown;
        let mut shutdown = lock.lock();
        *shutdown = true;
        cvar.notify_all();
        drop(shutdown);

        if let Some(handle) = self.compaction_thread.take() {
            _ = handle.join();
        }
        if let Some(handle) = self.flush_thread.take() {
            _ = handle.join();
        }

        _ = self.flush();

        _ = self.lock_file.unlock();
    }
}

impl CandyStore {
    fn acquire_lock(dir_path: &Path) -> Result<LockFile> {
        let lock_path = dir_path.join(".lock");
        let mut lock = LockFile::open(&lock_path).map_err(CandyError::IOError)?;

        match lock.try_lock() {
            Ok(true) => {
                let content = format!(
                    "pid={} exe={:?}\n",
                    std::process::id(),
                    std::env::current_exe()
                        .unwrap_or("<missing>".into())
                        .display()
                );
                let _ = std::fs::write(&lock_path, content);
                Ok(lock)
            }
            Ok(false) => {
                let contents = std::fs::read_to_string(lock_path).unwrap_or("owner unknown".into());
                Err(CandyError::DatabaseLocked(contents))
            }
            Err(e) => Err(CandyError::IOError(e)),
        }
    }

    pub fn open(dir_path: impl AsRef<Path>, mut config: Config) -> Result<Self> {
        let dir_path = dir_path.as_ref();
        std::fs::create_dir_all(dir_path).map_err(CandyError::IOError)?;

        let lock_file = Self::acquire_lock(dir_path)?;

        config.max_data_file_size = config.max_data_file_size.min(MAX_DATA_FILE_SIZE);
        config.remapping_scaler = config.remapping_scaler.min(2);
        config.max_concurrency = config.max_concurrency.clamp(1, 256).next_power_of_two();

        let (data_files, active_file_id) = load_data_files(dir_path, &config)?;

        let index_path = dir_path.join("index.db");
        let (index_file, needs_rebuild) = open_or_recover_index(&index_path, &config)?;

        check_consistency_and_recover(&index_file, &data_files, &config, needs_rebuild)?;

        let key_locks = (0..config.max_concurrency)
            .map(|_| RwLock::new(()))
            .collect();

        let inner = Arc::new(CandyStoreInner {
            index_file,
            active_file_id: AtomicU64::new(active_file_id as u64),
            data_files: parking_lot::RwLock::new(data_files),
            rotate_lock: Mutex::new(()),
            rotate_requested: AtomicU64::new(u64::MAX),
            config,
            dir_path: dir_path.to_path_buf(),
            key_locks,
            inner_stats: Arc::new(InnerStats {
                num_positive_lookups: AtomicU64::new(0),
                num_negative_lookups: AtomicU64::new(0),
                num_read_ops: AtomicU64::new(0),
                num_read_bytes: AtomicU64::new(0),
                num_write_ops: AtomicU64::new(0),
                num_write_bytes: AtomicU64::new(0),
            }),
        });

        let shutdown = Arc::new((Mutex::new(false), Condvar::new()));
        let compaction_thread = {
            let inner = inner.clone();
            let shutdown = shutdown.clone();
            Some(
                std::thread::Builder::new()
                    .name("candy_compact".into())
                    .stack_size(256 * 1024)
                    .spawn(move || {
                        background::compaction_loop(inner, shutdown);
                    })
                    .unwrap(),
            )
        };

        let flush_thread = if inner.config.flush_interval.is_some() {
            let inner = inner.clone();
            let shutdown = shutdown.clone();
            Some(
                std::thread::Builder::new()
                    .name("candy_flush".into())
                    .stack_size(256 * 1024)
                    .spawn(move || {
                        background::flush_loop(inner, shutdown);
                    })
                    .unwrap(),
            )
        } else {
            None
        };

        Ok(Self {
            inner,
            lock_file,
            compaction_thread,
            flush_thread,
            shutdown,
        })
    }
}
