use simd_itertools::PositionSimd;
use std::ops::Range;
use std::sync::atomic::AtomicU32;
use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use memmap::{MmapMut, MmapOptions};

use crate::hashing::{PartedHash, INVALID_SIG};
use crate::{Config, Result, VickyError};

//
// these numbers were chosen according to the simulation, as they allow for 90% utilization of the shard with
// virtually zero chance of in-row collisions and "smallish" shard size: shards start at 384KB and
// can hold 32K entries, and since we're limited at 4GB file sizes, we can key-value pairs of up to 128KB
// (keys and values are limited to 64KB each anyway)
//
// other good combinations are 32/512, 32/1024, 64/256, 64/1024, 128/512, 256/256
//
pub(crate) const NUM_ROWS: usize = 64;
pub(crate) const ROW_WIDTH: usize = 512;

#[repr(C)]
pub(crate) struct ShardRow {
    pub signatures: [u32; ROW_WIDTH],
    pub offsets_and_sizes: [u64; ROW_WIDTH], // | key_size: 16 | val_size: 16 | file_offset: 32 |
}

#[repr(C, align(4096))]
pub(crate) struct PageAligned<T>(pub T);

#[repr(C)]
pub(crate) struct ShardHeader {
    pub num_inserted: AtomicU64,
    pub num_deleted: AtomicU64,
    pub wasted_bytes: AtomicU64,
    pub write_offset: AtomicU32,
    pub rows: PageAligned<[ShardRow; NUM_ROWS]>,
}

const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;
const _: () = assert!(HEADER_SIZE % 4096 == 0);

#[derive(Debug)]
pub(crate) enum InsertStatus {
    Added,
    Replaced,
    CompactionNeeded(u32),
    SplitNeeded,
    AlreadyExists(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum InsertMode {
    Overwrite,
    GetOrInsert,
}

pub(crate) struct ByHashIterator<'a> {
    shard: &'a Shard,
    _guard: RwLockReadGuard<'a, ()>,
    row: &'a ShardRow,
    signature: u32,
    start_idx: usize,
}

type Entry = (Vec<u8>, Vec<u8>);

impl<'a> Iterator for ByHashIterator<'a> {
    type Item = Result<Entry>;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(idx) = self.row.signatures[self.start_idx..]
            .iter()
            .position_simd(self.signature)
        {
            self.start_idx = idx + 1;
            return Some(self.shard.read_kv(self.row.offsets_and_sizes[idx]));
        }
        None
    }
}

pub(crate) struct Shard {
    pub(crate) span: Range<u32>,
    file: File,
    config: Arc<Config>,
    #[allow(dead_code)]
    mmap: MmapMut, // needed to prevent it from dropping
    pub(crate) header: &'static mut ShardHeader,
    pub(crate) row_locks: Vec<RwLock<()>>,
}

impl Shard {
    pub(crate) const EXPECTED_CAPACITY: usize = (NUM_ROWS * ROW_WIDTH * 9) / 10; // ~ 29,500

    pub(crate) fn open(
        filename: PathBuf,
        span: Range<u32>,
        truncate: bool,
        config: Arc<Config>,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(&filename)?;
        let md = file.metadata()?;
        if md.len() < HEADER_SIZE {
            // when creating, set the file's length so that we won't need to extend it every time we write
            file.set_len(HEADER_SIZE + config.max_shard_size as u64)?;
        }

        let mut mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ShardHeader) };
        let mut row_locks = vec![];
        for _ in 0..NUM_ROWS {
            row_locks.push(RwLock::new(()));
        }

        Ok(Self {
            file,
            config,
            span,
            mmap,
            header,
            row_locks,
        })
    }

    pub(crate) fn flush(&self) -> Result<()> {
        //self.mmap.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    #[inline]
    fn extract_offset_and_size(offset_and_size: u64) -> (usize, usize, u64) {
        let klen = (offset_and_size >> 48) as usize;
        let vlen = ((offset_and_size >> 32) & 0xffff) as usize;
        let offset = (offset_and_size as u32) as u64;
        (klen, vlen, offset)
    }

    // reading doesn't require holding any locks - we only ever extend the file, never overwrite data
    pub(crate) fn read_kv(&self, offset_and_size: u64) -> Result<Entry> {
        let (klen, vlen, offset) = Self::extract_offset_and_size(offset_and_size);

        let mut buf = vec![0u8; klen + vlen];
        self.file.read_exact_at(&mut buf, HEADER_SIZE + offset)?;

        let val = buf[klen..klen + vlen].to_owned();
        buf.truncate(klen);

        Ok((buf, val))
    }

    // writing doesn't require holding any locks since we write with an offset
    fn write_kv(&self, key: &[u8], val: &[u8]) -> Result<u64> {
        let mut buf = vec![0u8; key.len() + val.len()];
        buf[..key.len()].copy_from_slice(key);
        buf[key.len()..].copy_from_slice(val);

        // atomically allocate some area. it may leak if the IO below fails or if we crash before updating the
        // offsets_and_size array, but we're okay with leaks
        let write_offset = self
            .header
            .write_offset
            .fetch_add(buf.len() as u32, Ordering::SeqCst) as u64;

        // now writing can be non-atomic (pwrite)
        self.file.write_all_at(&buf, HEADER_SIZE + write_offset)?;

        Ok(((key.len() as u64) << 48) | ((val.len() as u64) << 32) | write_offset)
    }

    pub(crate) fn read_at(&self, row_idx: usize, entry_idx: usize) -> Option<Result<Entry>> {
        let _guard = self.row_locks[row_idx].read().unwrap();
        let row = &self.header.rows.0[row_idx];
        if row.signatures[entry_idx] != INVALID_SIG {
            Some(self.read_kv(row.offsets_and_sizes[entry_idx]))
        } else {
            None
        }
    }

    pub(crate) fn unlocked_iter<'b>(&'b self) -> impl Iterator<Item = Result<Entry>> + 'b {
        self.header.rows.0.iter().flat_map(|row| {
            row.signatures
                .iter()
                .enumerate()
                .filter_map(|(idx, &sig)| (sig != INVALID_SIG).then_some(idx))
                .map(|idx| self.read_kv(row.offsets_and_sizes[idx]))
        })
    }

    pub(crate) fn iter_by_hash<'a>(&'a self, ph: PartedHash) -> ByHashIterator<'a> {
        let row_idx = (ph.row_selector as usize) % NUM_ROWS;
        let guard = self.row_locks[row_idx].read().unwrap();
        let row = &self.header.rows.0[row_idx];
        ByHashIterator {
            shard: &self,
            _guard: guard,
            row,
            signature: ph.signature,
            start_idx: 0,
        }
    }

    pub(crate) fn get(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for res in self.iter_by_hash(ph) {
            let (k, v) = res?;
            if key == k {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    fn try_replace(
        &self,
        row: &mut ShardRow,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<(bool, Option<Vec<u8>>)> {
        let mut start = 0;
        while let Some(idx) = row.signatures[start..].iter().position_simd(ph.signature) {
            let (k, v) = self.read_kv(row.offsets_and_sizes[idx])?;
            if key == k {
                match mode {
                    InsertMode::GetOrInsert => {
                        // no-op, key already exists
                        return Ok((true, Some(v)));
                    }
                    InsertMode::Overwrite => {
                        // optimization
                        if val != v {
                            row.offsets_and_sizes[idx] = self.write_kv(key, val)?;
                            self.header
                                .wasted_bytes
                                .fetch_add((k.len() + v.len()) as u64, Ordering::SeqCst);
                        }
                        return Ok((true, None));
                    }
                }
            }
            start = idx + 1;
        }
        Ok((false, None))
    }

    fn get_row_mut(&self, ph: PartedHash) -> (RwLockWriteGuard<()>, &mut ShardRow) {
        let row_idx = (ph.row_selector as usize) % NUM_ROWS;
        let guard = self.row_locks[row_idx].write().unwrap();
        // this is safe because we hold a write lock on the row. the row sits in an mmap, so it can't be
        // owned by the lock itself
        #[allow(invalid_reference_casting)]
        let row =
            unsafe { &mut *(&self.header.rows.0[row_idx] as *const ShardRow as *mut ShardRow) };
        (guard, row)
    }

    pub(crate) fn insert(
        &self,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<InsertStatus> {
        if self.header.write_offset.load(Ordering::Relaxed) as u64 + (key.len() + val.len()) as u64
            > self.config.max_shard_size as u64
        {
            if self.header.wasted_bytes.load(Ordering::Relaxed)
                > self.config.min_compaction_threashold as u64
            {
                return Ok(InsertStatus::CompactionNeeded(
                    self.header.write_offset.load(Ordering::Relaxed),
                ));
            } else {
                return Ok(InsertStatus::SplitNeeded);
            }
        }

        // maybe do the write before taking the row lock?

        // see if we replace an existing key
        let (_guard, row) = self.get_row_mut(ph);

        let (found, existing_val) = self.try_replace(row, ph, key, val, mode)?;
        if found {
            if let Some(existing_val) = existing_val {
                return Ok(InsertStatus::AlreadyExists(existing_val));
            } else {
                return Ok(InsertStatus::Replaced);
            }
        }

        // find an empty slot
        if let Some(idx) = row.signatures.iter().position_simd(INVALID_SIG) {
            let new_off = self.write_kv(key, val)?;

            row.offsets_and_sizes[idx] = new_off;
            std::sync::atomic::fence(Ordering::SeqCst);
            row.signatures[idx] = ph.signature;
            self.header.num_inserted.fetch_add(1, Ordering::SeqCst);
            Ok(InsertStatus::Added)
        } else {
            // no room in this row, must split
            Ok(InsertStatus::SplitNeeded)
        }
    }

    // this is NOT crash safe (may produce inconsistent results)
    pub(crate) fn modify_inplace(
        &self,
        ph: PartedHash,
        key: &[u8],
        patch: &[u8],
        patch_offset: usize,
    ) -> Result<()> {
        let (_guard, row) = self.get_row_mut(ph);

        let mut start = 0;
        while let Some(idx) = row.signatures[start..].iter().position_simd(ph.signature) {
            let (k, _) = self.read_kv(row.offsets_and_sizes[idx])?;
            if key == k {
                let (klen, vlen, offset) =
                    Self::extract_offset_and_size(row.offsets_and_sizes[idx]);
                if patch_offset + patch.len() > vlen as usize {
                    return Err(Box::new(VickyError::ValueTooLong));
                }

                self.file.write_all_at(
                    patch,
                    HEADER_SIZE + offset + klen as u64 + patch_offset as u64,
                )?;

                return Ok(());
            }
            start = idx + 1;
        }
        Err(Box::new(VickyError::KeyNotFound))
    }

    pub(crate) fn remove(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (_guard, row) = self.get_row_mut(ph);
        let mut start = 0;
        while let Some(idx) = row.signatures[start..].iter().position_simd(ph.signature) {
            let (k, v) = self.read_kv(row.offsets_and_sizes[idx])?;
            if key == k {
                row.signatures[idx] = INVALID_SIG;
                // we managed to remove this key
                self.header.num_deleted.fetch_add(1, Ordering::Relaxed);
                self.header
                    .wasted_bytes
                    .fetch_add((k.len() + v.len()) as u64, Ordering::Relaxed);
                return Ok(Some(v));
            }
            start = idx + 1;
        }

        Ok(None)
    }
}
