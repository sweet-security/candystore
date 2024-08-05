use std::ops::Bound;
use std::sync::atomic::Ordering;

use crate::hashing::{PartedHash, USER_NAMESPACE};
use crate::shard::{InsertMode, InsertStatus, Shard};
use crate::store::VickyStore;
use crate::{Result, VickyError};

impl VickyStore {
    fn compact(&self, shard_end: u32, write_offset: u32) -> Result<bool> {
        let mut guard = self.shards.write().unwrap();
        // it's possible that another thread already compacted this shard
        if guard
            .get(&shard_end)
            .unwrap()
            .header
            .write_offset
            .load(Ordering::Relaxed)
            < write_offset
        {
            return Ok(false);
        }

        let removed_shard = guard.remove(&shard_end).unwrap();
        let orig_filename = self.dir_path.join(format!(
            "shard_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let tmpfile = self.dir_path.join(format!(
            "compact_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let compacted_shard = Shard::open(
            tmpfile.clone(),
            removed_shard.span.clone(),
            true,
            self.config.clone(),
        )?;

        self.num_compactions.fetch_add(1, Ordering::SeqCst);

        for res in removed_shard.unlocked_iter() {
            let (k, v) = res?;
            // XXX: this will not work with namespaces
            let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, &k);

            let status = compacted_shard.insert(ph, &k, &v, InsertMode::Overwrite)?;
            assert!(matches!(status, InsertStatus::Added), "{status:?}");
        }

        std::fs::rename(tmpfile, &orig_filename)?;
        guard.insert(shard_end, compacted_shard);
        Ok(true)
    }

    fn split(&self, shard_start: u32, shard_end: u32) -> Result<bool> {
        let mut guard = self.shards.write().unwrap();
        // it's possible that another thread already split this range - check if midpoint exists, and if so, bail out
        let midpoint = shard_start / 2 + shard_end / 2;
        if guard.contains_key(&midpoint) {
            return Ok(false);
        }

        let removed_shard = guard.remove(&shard_end).unwrap();

        let bottomfile = self
            .dir_path
            .join(format!("bottom_{:04x}-{:04x}", shard_start, midpoint));
        let topfile = self
            .dir_path
            .join(format!("top_{:04x}-{:04x}", midpoint, shard_end));

        let bottom_shard = Shard::open(
            bottomfile.clone(),
            shard_start..midpoint,
            true,
            self.config.clone(),
        )?;
        let top_shard = Shard::open(
            topfile.clone(),
            midpoint..shard_end,
            true,
            self.config.clone(),
        )?;

        for res in removed_shard.unlocked_iter() {
            let (k, v) = res?;

            let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, &k);
            let status = if (ph.shard_selector as u32) < midpoint {
                bottom_shard.insert(ph, &k, &v, InsertMode::Overwrite)?
            } else {
                top_shard.insert(ph, &k, &v, InsertMode::Overwrite)?
            };
            assert!(matches!(status, InsertStatus::Added), "{status:?}");
        }

        self.num_splits.fetch_add(1, Ordering::SeqCst);

        // this is not atomic, so when loading, we need to take the larger span if it exists and
        // delete the partial ones
        std::fs::rename(
            bottomfile,
            self.dir_path
                .join(format!("shard_{:04x}-{:04x}", shard_start, midpoint)),
        )?;
        std::fs::rename(
            topfile,
            self.dir_path
                .join(format!("shard_{:04x}-{:04x}", midpoint, shard_end)),
        )?;
        std::fs::remove_file(
            self.dir_path
                .join(format!("shard_{:04x}-{:04x}", shard_start, shard_end)),
        )
        .unwrap();

        guard.insert(midpoint, bottom_shard);
        guard.insert(shard_end, top_shard);

        Ok(true)
    }

    fn try_insert(
        &self,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<(InsertStatus, u32, u32)> {
        let guard = self.shards.read().unwrap();
        let cursor = guard.lower_bound(Bound::Excluded(&(ph.shard_selector as u32)));
        let shard_start = cursor
            .peek_prev()
            .map(|(&shard_start, _)| shard_start)
            .unwrap_or(0);
        let (shard_end, shard) = cursor.peek_next().unwrap();
        let status = shard.insert(ph, key, val, mode)?;

        Ok((status, shard_start, *shard_end))
    }

    pub(crate) fn insert_internal(
        &self,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<Option<Vec<u8>>> {
        if key.len() > u16::MAX as usize {
            return Err(Box::new(VickyError::KeyTooLong));
        }
        if val.len() > u16::MAX as usize {
            return Err(Box::new(VickyError::ValueTooLong));
        }

        loop {
            let (status, shard_start, shard_end) = self.try_insert(ph, key, val, mode)?;

            match status {
                InsertStatus::Added => {
                    self.num_entries.fetch_add(1, Ordering::SeqCst);
                    return Ok(None);
                }
                InsertStatus::Replaced => {
                    return Ok(None);
                }
                InsertStatus::AlreadyExists(existing_val) => {
                    return Ok(Some(existing_val));
                }
                InsertStatus::CompactionNeeded(write_offset) => {
                    self.compact(shard_end, write_offset)?;
                    // retry
                }
                InsertStatus::SplitNeeded => {
                    self.split(shard_start, shard_end)?;
                    // retry
                }
            }
        }
    }

    /// Inserts a key-value pair, creating it or replacing an existing pair. Note that if the program crashed
    /// while or "right after" this operation, or if the operating system is unable to flush the page cache,
    /// you may lose some data. However, you will still be in a consistent state, where you will get a previous
    /// version of the state.
    ///
    /// While this method is O(1) amortized, every so often it will trigger either a shard compaction or a
    /// shard split, which requires rewriting the whole shard.
    pub fn insert<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<()> {
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key.as_ref());
        self.insert_internal(ph, key.as_ref(), val.as_ref(), InsertMode::Overwrite)?;
        Ok(())
    }

    /// Gets the value of an entry or inserts the given default value. If the value existed, returns `Some(value)`.
    /// If the value was created by this operation, `None`` is returned
    pub fn get_or_insert_default<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        default_val: &B2,
    ) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key.as_ref());
        self.insert_internal(
            ph,
            key.as_ref(),
            default_val.as_ref(),
            InsertMode::GetOrInsert,
        )
    }

    /// Modifies an existing entry in-place, instead of creating a version. Note that the key must exist
    /// and `patch.len() + patch_offset` must be less than or equal to the current value's length.
    ///
    /// This is operation is NOT crash-safe as it overwrites existing data, and thus may produce inconsistent
    /// results on crashes (reading part old data, part new data).
    ///
    /// This method will never trigger a shard split or a shard compaction.
    pub fn modify_inplace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        patch: &B2,
        patch_offset: usize,
    ) -> Result<()> {
        let key = key.as_ref();
        let patch = patch.as_ref();
        let ph = PartedHash::from_buffer(USER_NAMESPACE, &self.config.secret_key, key);
        self.shards
            .read()
            .unwrap()
            .lower_bound(Bound::Excluded(&(ph.shard_selector as u32)))
            .peek_next()
            .unwrap()
            .1
            .modify_inplace(ph, key, patch, patch_offset)
    }
}
