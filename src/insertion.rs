use anyhow::{anyhow, Context};
use std::ops::Bound;
use std::sync::atomic::Ordering;

use crate::hashing::PartedHash;
use crate::shard::{InsertMode, InsertStatus, Shard};
use crate::store::CandyStore;
use crate::{CandyError, Result, MAX_TOTAL_KEY_SIZE, MAX_VALUE_SIZE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceStatus {
    PrevValue(Vec<u8>),
    WrongValue(Vec<u8>),
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

impl CandyStore {
    fn compact(&self, shard_end: u32, write_offset: u32) -> Result<bool> {
        let mut guard = self.shards.write();
        // it's possible that another thread already compacted this shard
        if guard
            .get(&shard_end)
            .with_context(|| format!("missing shard {shard_end}"))?
            .get_write_offset()
            < write_offset
        {
            return Ok(false);
        }

        let removed_shard = guard
            .remove(&shard_end)
            .with_context(|| format!("missing shard {shard_end}"))?;
        let orig_filename = self.dir_path.join(format!(
            "shard_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let tmpfile = self.dir_path.join(format!(
            "compact_{:04x}-{:04x}",
            removed_shard.span.start, removed_shard.span.end
        ));
        let mut compacted_shard = Shard::open(
            tmpfile.clone(),
            removed_shard.span.clone(),
            true,
            self.config.clone(),
        )?;

        self.num_compactions.fetch_add(1, Ordering::SeqCst);

        removed_shard.compact_into(&mut compacted_shard)?;

        std::fs::rename(tmpfile, &orig_filename)?;
        guard.insert(shard_end, compacted_shard);
        Ok(true)
    }

    fn split(&self, shard_start: u32, shard_end: u32) -> Result<bool> {
        let mut guard = self.shards.write();
        // it's possible that another thread already split this range - check if midpoint exists, and if so, bail out
        let midpoint = shard_start / 2 + shard_end / 2;
        if guard.contains_key(&midpoint) {
            return Ok(false);
        }

        let removed_shard = guard
            .remove(&shard_end)
            .with_context(|| format!("missing shard {shard_end}"))?;

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

        removed_shard.split_into(&bottom_shard, &top_shard)?;

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
        let guard = self.shards.read();
        let cursor = guard.lower_bound(Bound::Excluded(&(ph.shard_selector() as u32)));
        let shard_start = cursor
            .peek_prev()
            .map(|(&shard_start, _)| shard_start)
            .unwrap_or(0);
        let (shard_end, shard) = cursor.peek_next().with_context(|| {
            format!(
                "missing shard for selector 0x{:04x} start=0x{:04x}",
                ph.shard_selector(),
                shard_start
            )
        })?;
        let status = shard.insert(ph, key, val, mode)?;

        Ok((status, shard_start, *shard_end))
    }

    pub(crate) fn insert_internal(
        &self,
        full_key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<InsertStatus> {
        let ph = PartedHash::new(&self.config.hash_seed, full_key);

        if full_key.len() > MAX_TOTAL_KEY_SIZE as usize {
            return Err(anyhow!(CandyError::KeyTooLong(full_key.len())));
        }
        if val.len() > MAX_VALUE_SIZE as usize {
            return Err(anyhow!(CandyError::ValueTooLong(val.len())));
        }
        if full_key.len() + val.len() > self.config.max_shard_size as usize {
            return Err(anyhow!(CandyError::EntryCannotFitInShard(
                full_key.len() + val.len(),
                self.config.max_shard_size as usize
            )));
        }

        loop {
            let (status, shard_start, shard_end) = self.try_insert(ph, &full_key, val, mode)?;

            match status {
                InsertStatus::Added => {
                    self.num_entries.fetch_add(1, Ordering::SeqCst);
                    return Ok(status);
                }
                InsertStatus::KeyDoesNotExist => {
                    return Ok(status);
                }
                InsertStatus::Replaced(_) => {
                    return Ok(status);
                }
                InsertStatus::AlreadyExists(_) => {
                    return Ok(status);
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
        match self.insert_internal(full_key, val, InsertMode::Set)? {
            InsertStatus::Added => Ok(SetStatus::CreatedNew),
            InsertStatus::Replaced(v) => Ok(SetStatus::PrevValue(v)),
            InsertStatus::AlreadyExists(v) => Ok(SetStatus::PrevValue(v)),
            InsertStatus::KeyDoesNotExist => unreachable!(),
            InsertStatus::CompactionNeeded(_) => unreachable!(),
            InsertStatus::SplitNeeded => unreachable!(),
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
        self.owned_set(key.as_ref().to_owned(), val.as_ref())
    }

    /// Same as [Self::set], but the key passed owned to this function
    pub fn owned_set(&self, key: Vec<u8>, val: &[u8]) -> Result<SetStatus> {
        self.set_raw(&self.make_user_key(key), val)
    }

    pub(crate) fn replace_raw(
        &self,
        full_key: &[u8],
        val: &[u8],
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        match self.insert_internal(full_key, val, InsertMode::Replace(expected_val))? {
            InsertStatus::Added => unreachable!(),
            InsertStatus::Replaced(v) => Ok(ReplaceStatus::PrevValue(v)),
            InsertStatus::AlreadyExists(v) => Ok(ReplaceStatus::WrongValue(v)),
            InsertStatus::KeyDoesNotExist => Ok(ReplaceStatus::DoesNotExist),
            InsertStatus::CompactionNeeded(_) => unreachable!(),
            InsertStatus::SplitNeeded => unreachable!(),
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
        expected_val: Option<&B2>,
    ) -> Result<ReplaceStatus> {
        self.owned_replace(
            key.as_ref().to_owned(),
            val.as_ref(),
            expected_val.map(|ev| ev.as_ref()),
        )
    }

    /// Same as [Self::replace], but the key passed owned to this function
    pub fn owned_replace(
        &self,
        key: Vec<u8>,
        val: &[u8],
        expected_val: Option<&[u8]>,
    ) -> Result<ReplaceStatus> {
        self.replace_raw(&self.make_user_key(key), val.as_ref(), expected_val)
    }

    pub(crate) fn get_or_create_raw(
        &self,
        full_key: &[u8],
        default_val: Vec<u8>,
    ) -> Result<GetOrCreateStatus> {
        match self.insert_internal(full_key, &default_val, InsertMode::GetOrCreate)? {
            InsertStatus::Added => Ok(GetOrCreateStatus::CreatedNew(default_val)),
            InsertStatus::AlreadyExists(v) => Ok(GetOrCreateStatus::ExistingValue(v)),
            InsertStatus::Replaced(_) => unreachable!(),
            InsertStatus::KeyDoesNotExist => unreachable!(),
            InsertStatus::CompactionNeeded(_) => unreachable!(),
            InsertStatus::SplitNeeded => unreachable!(),
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
        self.owned_get_or_create(key.as_ref().to_owned(), default_val.as_ref().to_owned())
    }

    /// Same as [Self::get_or_create], but the `key` and `default_val` are passed owned to this function
    pub fn owned_get_or_create(
        &self,
        key: Vec<u8>,
        default_val: Vec<u8>,
    ) -> Result<GetOrCreateStatus> {
        self.get_or_create_raw(&self.make_user_key(key), default_val)
    }
}
