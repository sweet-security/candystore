use std::ops::Bound;
use std::sync::atomic::Ordering;

use crate::hashing::PartedHash;
use crate::shard::{InsertMode, InsertStatus, Shard};
use crate::store::VickyStore;
use crate::{Result, VickyError, MAX_TOTAL_KEY_SIZE, MAX_VALUE_SIZE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceStatus {
    PrevValue(Vec<u8>),
    DoesNotExist,
}
impl ReplaceStatus {
    pub fn was_replaced(&self) -> bool {
        matches!(*self, Self::PrevValue(_))
    }
    pub fn is_key_missing(&self) -> bool {
        matches!(*self, Self::DoesNotExist)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModifyStatus {
    PrevValue(Vec<u8>),
    DoesNotExist,
    WrongLength(usize, usize),
    ValueTooLong(usize, usize, usize),
    ValueMismatch(Vec<u8>),
}
impl ModifyStatus {
    pub fn was_replaced(&self) -> bool {
        matches!(*self, Self::PrevValue(_))
    }
    pub fn is_mismatch(&self) -> bool {
        matches!(*self, Self::ValueMismatch(_))
    }
    pub fn is_too_long(&self) -> bool {
        matches!(*self, Self::ValueTooLong(_, _, _))
    }
    pub fn is_wrong_length(&self) -> bool {
        matches!(*self, Self::WrongLength(_, _))
    }
    pub fn is_key_missing(&self) -> bool {
        matches!(*self, Self::DoesNotExist)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetStatus {
    PrevValue(Vec<u8>),
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
pub enum GetOrCreateStatus {
    ExistingValue(Vec<u8>),
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
            let ph = PartedHash::new(&self.config.hash_seed, &k);
            let status = compacted_shard.insert(ph, &k, &v, InsertMode::Set)?;
            if !matches!(status, InsertStatus::Added) {
                return Err(Box::new(VickyError::CompactionFailed(format!(
                    "{ph:?} [{}..{}] shard {status:?} k={k:?} v={v:?}",
                    removed_shard.span.start, removed_shard.span.end
                ))));
            }
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

            let ph = PartedHash::new(&self.config.hash_seed, &k);
            let status = if (ph.shard_selector() as u32) < midpoint {
                bottom_shard.insert(ph, &k, &v, InsertMode::Set)?
            } else {
                top_shard.insert(ph, &k, &v, InsertMode::Set)?
            };
            if !matches!(status, InsertStatus::Added) {
                return Err(Box::new(VickyError::SplitFailed(format!(
                    "{ph:?} {status:?} [{shard_start} {midpoint} {shard_end}] k={k:?} v={v:?}",
                ))));
            }
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
        )?;

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
        let cursor = guard.lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)));
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
        full_key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<Option<Vec<u8>>> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);

        if full_key.len() > MAX_TOTAL_KEY_SIZE as usize {
            return Err(Box::new(VickyError::KeyTooLong));
        }
        if val.len() > MAX_VALUE_SIZE as usize {
            return Err(Box::new(VickyError::ValueTooLong));
        }

        loop {
            let (status, shard_start, shard_end) = self.try_insert(ph, &full_key, val, mode)?;

            match status {
                InsertStatus::Added => {
                    self.num_entries.fetch_add(1, Ordering::SeqCst);
                    return Ok(None);
                }
                InsertStatus::KeyDoesNotExist => {
                    return Ok(None);
                }
                InsertStatus::Replaced(existing_val) => {
                    return Ok(Some(existing_val));
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

    pub(crate) fn set_raw(&self, full_key: &[u8], val: &[u8]) -> Result<SetStatus> {
        if let Some(prev) = self.insert_internal(full_key, val, InsertMode::Set)? {
            Ok(SetStatus::PrevValue(prev))
        } else {
            Ok(SetStatus::CreatedNew)
        }
    }

    /// Inserts a key-value pair, creating it or replacing an existing pair. Note that if the program crashed
    /// while or "right after" this operation, or if the operating system is unable to flush the page cache,
    /// you may lose some data. However, you will still be in a consistent state, where you will get a previous
    /// version of the state.
    ///
    /// While this method is O(1) amortized, every so often it will trigger either a shard compaction or a
    /// shard split, which requires rewriting the whole shard. However, unlike LSM trees, this operation is
    /// constant in size
    pub fn set<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<SetStatus> {
        self.set_raw(&self.make_user_key(key.as_ref()), val.as_ref())
    }

    pub fn replace_raw(&self, full_key: &[u8], val: &[u8]) -> Result<ReplaceStatus> {
        if let Some(prev) = self.insert_internal(full_key, val, InsertMode::Replace)? {
            Ok(ReplaceStatus::PrevValue(prev))
        } else {
            Ok(ReplaceStatus::DoesNotExist)
        }
    }

    /// Replaces the value of an existing key with a new value. If the key existed, returns
    /// `PrevValue(value)` with its old value, and if it did not, returns `DoesNotExist` but
    /// does not create the key.
    ///
    /// See [Self::set] for more details
    pub fn replace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        val: &B2,
    ) -> Result<ReplaceStatus> {
        self.replace_raw(&self.make_user_key(key.as_ref()), val.as_ref())
    }

    pub fn get_or_create_raw(
        &self,
        full_key: &[u8],
        default_val: &[u8],
    ) -> Result<GetOrCreateStatus> {
        let res = self.insert_internal(&full_key, default_val, InsertMode::GetOrCreate)?;
        if let Some(prev) = res {
            Ok(GetOrCreateStatus::ExistingValue(prev))
        } else {
            Ok(GetOrCreateStatus::CreatedNew(vec![]))
        }
    }

    /// Gets the value of the given key or creates it with the given default value. If the key did not exist,
    /// returns `CreatedNew(default_val)`, and if it did, returns `ExistingValue(value)`.
    /// This is done atomically, so it can be used to create a key only if it did not exist before,
    /// like `open` with `O_EXCL`.
    ///
    /// See [Self::set] for more details
    pub fn get_or_create<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        default_val: &B2,
    ) -> Result<GetOrCreateStatus> {
        match self.get_or_create_raw(&self.make_user_key(key.as_ref()), default_val.as_ref())? {
            GetOrCreateStatus::CreatedNew(_) => Ok(GetOrCreateStatus::CreatedNew(
                default_val.as_ref().to_owned(),
            )),
            v @ GetOrCreateStatus::ExistingValue(_) => Ok(v),
        }
    }

    // this is NOT crash safe (may produce inconsistent results)
    pub(crate) fn modify_inplace_raw(
        &self,
        full_key: &[u8],
        patch: &[u8],
        patch_offset: usize,
        expected: Option<&[u8]>,
    ) -> Result<ModifyStatus> {
        self.operate_on_key_mut(full_key, |shard, row, _ph, idx_kv| {
            let Some((idx, _k, v)) = idx_kv else {
                return Ok(ModifyStatus::DoesNotExist);
            };

            let (klen, vlen, offset) = Shard::extract_offset_and_size(row.offsets_and_sizes[idx]);
            if patch_offset + patch.len() > vlen as usize {
                return Ok(ModifyStatus::ValueTooLong(patch_offset, patch.len(), vlen));
            }

            if let Some(expected) = expected {
                if &v[patch_offset..patch_offset + patch.len()] != expected {
                    return Ok(ModifyStatus::ValueMismatch(v));
                }
            }

            shard.write_raw(patch, offset + klen as u64 + patch_offset as u64)?;
            Ok(ModifyStatus::PrevValue(v))
        })
    }

    /// Modifies an existing entry in-place, instead of creating a new version. Note that the key must exist
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
        expected: Option<&B2>,
    ) -> Result<ModifyStatus> {
        self.modify_inplace_raw(
            &self.make_user_key(key.as_ref()),
            patch.as_ref(),
            patch_offset,
            expected.map(|b| b.as_ref()),
        )
    }

    pub(crate) fn replace_inplace_raw(
        &self,
        full_key: &[u8],
        new_value: &[u8],
    ) -> Result<ModifyStatus> {
        self.operate_on_key_mut(full_key, |shard, row, _ph, idx_kv| {
            let Some((idx, _k, v)) = idx_kv else {
                return Ok(ModifyStatus::DoesNotExist);
            };

            let (klen, vlen, offset) = Shard::extract_offset_and_size(row.offsets_and_sizes[idx]);
            if new_value.len() != vlen as usize {
                return Ok(ModifyStatus::WrongLength(new_value.len(), vlen));
            }

            shard.write_raw(new_value, offset + klen as u64)?;
            return Ok(ModifyStatus::PrevValue(v));
        })
    }

    /// Modifies an existing entry in-place, instead of creating a new version. Note that the key must exist
    /// and `new_value.len()` must match exactly the existing value's length. This is mostly useful for binary
    /// data.
    ///
    /// This is operation is NOT crash-safe as it overwrites existing data, and thus may produce inconsistent
    /// results on crashes (reading part old data, part new data).
    ///
    /// This method will never trigger a shard split or a shard compaction.
    pub fn replace_inplace<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        key: &B1,
        new_value: &B2,
    ) -> Result<ModifyStatus> {
        self.replace_inplace_raw(&self.make_user_key(key.as_ref()), new_value.as_ref())
    }
}
