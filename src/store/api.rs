use super::ops::SetOptions;
use crate::store::iterator::CandyStoreIterator;
use crate::types::{BIG_NS, CandyError, KeyNamespace, ROW_WIDTH, Result, SpecialEntryType, Stats};
use std::sync::atomic::Ordering;

use super::CandyStore;
use super::inner::load_data_files;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Status of a replace operation.
pub enum ReplaceStatus {
    /// The value was replaced, returning the previous value.
    PrevValue(Vec<u8>),
    /// The value was not replaced because the existing value did not match the expected value.
    WrongValue(Vec<u8>),
    /// The key does not exist.
    DoesNotExist,
}

impl ReplaceStatus {
    pub fn was_replaced(&self) -> bool {
        matches!(*self, Self::PrevValue(_))
    }

    pub fn failed(&self) -> bool {
        !matches!(*self, Self::PrevValue(_))
    }

    pub fn is_key_missing(&self) -> bool {
        matches!(*self, Self::DoesNotExist)
    }

    pub fn is_wrong_value(&self) -> bool {
        matches!(*self, Self::WrongValue(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Status of a set operation.
pub enum SetStatus {
    /// The value was replaced, returning the previous value.
    PrevValue(Vec<u8>),
    /// A new key-value pair was created.
    CreatedNew,
}

impl SetStatus {
    pub fn was_created(&self) -> bool {
        matches!(*self, Self::CreatedNew)
    }

    pub fn was_replaced(&self) -> bool {
        matches!(*self, Self::PrevValue(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Status of a get_or_create operation.
pub enum GetOrCreateStatus {
    /// The key already existed, returning the existing value.
    ExistingValue(Vec<u8>),
    /// A new key-value pair was created with the default value.
    CreatedNew(Vec<u8>),
}

impl GetOrCreateStatus {
    pub fn was_created(&self) -> bool {
        matches!(*self, Self::CreatedNew(_))
    }

    pub fn already_exists(&self) -> bool {
        matches!(*self, Self::ExistingValue(_))
    }

    pub fn value(self) -> Vec<u8> {
        match self {
            Self::CreatedNew(val) => val,
            Self::ExistingValue(val) => val,
        }
    }
}

impl CandyStore {
    /// Flushes all pending writes to disk.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    pub fn flush(&self) -> Result<()> {
        let data_files_guard = self.inner.data_files.write();
        let active_id = self.inner.active_file_id.load(Ordering::Relaxed) as u16;
        let Some(data_file) = data_files_guard.get(&active_id).cloned() else {
            return Err(CandyError::MissingDataFile(active_id));
        };
        drop(data_files_guard);

        data_file.flush_checkpoint(data_file.write_offset.load(Ordering::SeqCst), 0)?;
        self.inner.index_file.flush()?;
        Ok(())
    }

    /// Returns current statistics about the store.
    ///
    /// # Returns
    ///
    /// A `Stats` struct containing current statistics.
    pub fn stats(&self) -> Stats {
        let idx_stats = self.inner.index_file.stats();
        let positive_lookups = self
            .inner
            .inner_stats
            .num_positive_lookups
            .load(Ordering::Relaxed);
        let negative_lookups = self
            .inner
            .inner_stats
            .num_negative_lookups
            .load(Ordering::Relaxed);
        let read_ops = self.inner.inner_stats.num_read_ops.load(Ordering::Relaxed);
        let read_bytes = self
            .inner
            .inner_stats
            .num_read_bytes
            .load(Ordering::Relaxed);
        let write_ops = self.inner.inner_stats.num_write_ops.load(Ordering::Relaxed);
        let write_bytes = self
            .inner
            .inner_stats
            .num_write_bytes
            .load(Ordering::Relaxed);

        let data_files = self.inner.data_files.read();
        let mut total_data_bytes = 0;
        let mut total_wasted_bytes = 0;

        for (id, file) in data_files.iter() {
            total_data_bytes += file.write_offset.load(Ordering::Relaxed);
            if let Ok(Some(wasted)) = self._get_special_key(
                KeyNamespace::StatsWastedBytes,
                SpecialEntryType::WastedBytes,
                *id as u64,
            ) {
                total_wasted_bytes += wasted;
            }
        }

        Stats {
            num_data_files: data_files.len(),
            num_rows: self.inner.index_file.num_rows(),
            num_compactions: idx_stats.num_compacted_files as usize,

            occupied_bytes: total_data_bytes as usize,
            wasted_bytes: total_wasted_bytes as usize,

            num_inserts: idx_stats.num_inserts as usize,
            num_updates: idx_stats.num_updates as usize,
            num_positive_lookups: positive_lookups as usize,
            num_negative_lookups: negative_lookups as usize,
            num_removals: idx_stats.num_deletes as usize,

            num_read_ops: read_ops as usize,
            num_read_bytes: read_bytes as usize,
            num_write_ops: write_ops as usize,
            num_write_bytes: write_bytes as usize,

            entries_under_128: idx_stats.entries_under_128 as usize,
            entries_under_1k: idx_stats.entries_under_1k as usize,
            entries_under_8k: idx_stats.entries_under_8k as usize,
            entries_under_32k: idx_stats.entries_under_32k as usize,
            entries_over_32k: idx_stats.entries_over_32k as usize,
        }
    }

    /// Retrieves the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// The value associated with the key, or `None` if the key does not exist.
    pub fn get<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let res = self._get(KeyNamespace::User, key.as_ref())?;
        self.inner.inner_stats.record_lookup(res.is_some());
        Ok(res)
    }

    /// Sets the value for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set.
    /// * `val` - The value to store.
    ///
    /// # Returns
    ///
    /// A `SetStatus` indicating whether the value was created or replaced.
    pub fn set<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<SetStatus> {
        let key = key.as_ref();
        let val = val.as_ref();
        self.ensure_user_value_len(val.len())?;
        let outcome = self._set_with_options(KeyNamespace::User, key, val, SetOptions::Upsert)?;
        self.inner
            .inner_stats
            .record_lookup(outcome.previous.is_some());

        Ok(match outcome.previous {
            Some(prev) => SetStatus::PrevValue(prev),
            None => SetStatus::CreatedNew,
        })
    }

    /// Removes the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// The removed value, or `None` if the key did not exist.
    pub fn remove<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let res = self._remove(KeyNamespace::User, key.as_ref())?;
        self.inner.inner_stats.record_lookup(res.is_some());
        Ok(res)
    }

    /// Checks if the store contains the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check.
    ///
    /// # Returns
    ///
    /// `true` if the key exists, `false` otherwise.
    pub fn contains<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<bool> {
        self._get(KeyNamespace::User, key.as_ref())
            .map(|v| v.is_some())
    }

    /// Atomically fetch the current value or create it if absent.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up or create.
    /// * `default_val` - The value to set if the key does not exist.
    ///
    /// # Returns
    ///
    /// A `GetOrCreateStatus` indicating whether the value was retrieved or created.
    pub fn get_or_create<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        default_val: &B2,
    ) -> Result<GetOrCreateStatus> {
        let key = key.as_ref();
        let default_val = default_val.as_ref();
        self.ensure_user_value_len(default_val.len())?;
        let outcome = self._set_with_options(
            KeyNamespace::User,
            key,
            default_val,
            SetOptions::InsertIfVacant,
        )?;

        Ok(match outcome.previous {
            Some(existing) => GetOrCreateStatus::ExistingValue(existing),
            None => GetOrCreateStatus::CreatedNew(default_val.to_vec()),
        })
    }

    /// Atomically replace the value only if the key already exists.
    /// Returns the previous value when replaced, or None if missing.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to replace.
    /// * `val` - The new value.
    /// * `expected_val` - If provided, the replacement only happens if the current value matches this.
    ///
    /// # Returns
    ///
    /// A `ReplaceStatus` indicating the outcome of the operation.
    pub fn replace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
        expected_val: Option<&B2>,
    ) -> Result<ReplaceStatus> {
        let key = key.as_ref();
        let val = val.as_ref();
        self.ensure_user_value_len(val.len())?;

        let expected = expected_val.map(|v| v.as_ref().to_vec());
        let outcome = self._set_with_options(
            KeyNamespace::User,
            key,
            val,
            SetOptions::ReplaceIfExists(expected),
        )?;

        Ok(match outcome.previous {
            None => ReplaceStatus::DoesNotExist,
            Some(prev) if outcome.wrong_value => ReplaceStatus::WrongValue(prev),
            Some(prev) => ReplaceStatus::PrevValue(prev),
        })
    }

    /// Sets a value that might be larger than the standard limit by splitting it into chunks.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set.
    /// * `val` - The large value.
    ///
    /// # Returns
    ///
    /// `true` on success, or an error if the operation fails.
    pub fn set_big<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<bool> {
        let key = key.as_ref();
        let val = val.as_ref();
        self.queue_set_big_with_ns(BIG_NS, key, val)
    }

    /// Retrieves a large value that was stored using `set_big`.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve.
    ///
    /// # Returns
    ///
    /// The value associated with the key, or `None` if the key does not exist.
    pub fn get_big<B: AsRef<[u8]> + ?Sized>(&self, key: &B) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        self.queue_get_big_with_ns(BIG_NS, key)
    }

    /// Removes a large value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// `true` if the value existed and was removed, `false` otherwise.
    pub fn remove_big<B1: AsRef<[u8]> + ?Sized>(&self, key: &B1) -> Result<bool> {
        self.queue_discard_with_ns(BIG_NS, key.as_ref())
    }

    /// Returns the approximate number of items in the store.
    ///
    /// # Returns
    ///
    /// The approximate number of items in the store.
    pub fn num_items(&self) -> usize {
        let stats = self.inner.index_file.stats();
        stats.num_inserts.saturating_sub(stats.num_deletes) as usize
    }

    /// Shrinks the index file if the wasted space exceeds the threshold. This is a blocking operation,
    /// no other operations can proceed while this is running.
    ///
    /// # Arguments
    ///
    /// * `min_wasted_pct` - The minimum percentage of wasted space required to trigger a shrink (0.0 - 1.0).
    ///   It makes sense to use at least 0.75 (75%) to avoid thrashing.
    ///
    /// # Returns
    ///
    /// The new number of rows in the index file.
    pub fn shrink_index_blocking(&self, min_wasted_pct: f64) -> Result<usize> {
        // Clamp to sane bounds to avoid surprises from caller inputs.
        let min_wasted_pct = min_wasted_pct.clamp(0.0, 1.0);

        let current_rows = self.inner.index_file.num_rows();
        if current_rows == 0 {
            return Ok(0);
        }

        // Aim for ~80% occupancy when estimating required rows.
        let required_rows = self.num_items() / (ROW_WIDTH * 8 / 10);

        let min_rows_cfg = self
            .inner
            .config
            .initial_capacity
            .div_ceil(ROW_WIDTH)
            .max(1);

        let min_rows = required_rows.max(min_rows_cfg).max(1);

        let reclaimable_rows = current_rows.saturating_sub(min_rows);
        let reclaimable_pct = reclaimable_rows as f64 / current_rows as f64;

        if reclaimable_pct < min_wasted_pct {
            return Ok(current_rows);
        }

        self.inner.index_file.shrink(min_rows_cfg)
    }

    /// Returns the total capacity of the index in bytes.
    ///
    /// # Returns
    ///
    /// The total capacity of the index in bytes.
    pub fn capacity(&self) -> usize {
        self.inner.index_file.num_rows() * ROW_WIDTH
    }

    /// Returns an iterator over all key-value pairs in the store.
    ///
    /// # Returns
    ///
    /// An iterator over all key-value pairs in the store.
    pub fn iter(&self) -> CandyStoreIterator<'_> {
        CandyStoreIterator::new(self, true, true)
    }

    /// Clears all data from the store.
    ///
    /// This operation deletes all data files and resets the index.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the operation fails.
    pub fn clear(&self) -> Result<()> {
        let _key_guards = self
            .inner
            .key_locks
            .iter()
            .map(|l| l.write())
            .collect::<Vec<_>>();
        let _row_guards = self
            .inner
            .index_file
            .row_locks
            .iter()
            .map(|l| l.write())
            .collect::<Vec<_>>();

        self.flush()?;

        {
            let mut data_files = self.inner.data_files.write();
            data_files.clear();
        }

        for entry in std::fs::read_dir(&self.inner.dir_path).map_err(CandyError::IOError)? {
            let entry = entry.map_err(CandyError::IOError)?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with("data_")
                && name.ends_with(".db")
            {
                let _ = std::fs::remove_file(&path);
            }
        }

        self.inner.index_file.reset(&self.inner.config)?;

        let (data_files, active_id) = load_data_files(&self.inner.dir_path, &self.inner.config)?;
        {
            let mut data_files_guard = self.inner.data_files.write();
            *data_files_guard = data_files;
        }
        self.inner
            .active_file_id
            .store(active_id as u64, Ordering::Relaxed);

        Ok(())
    }
}
