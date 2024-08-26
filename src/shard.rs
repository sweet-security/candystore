use anyhow::bail;
use bytemuck::{bytes_of_mut, Pod, Zeroable};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use simd_itertools::PositionSimd;
use std::{
    fs::{File, OpenOptions},
    io::Read,
    ops::Range,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use memmap::{MmapMut, MmapOptions};

use crate::{
    hashing::{PartedHash, INVALID_SIG},
    stats::InternalStats,
    store::InternalConfig,
};
use crate::{CandyError, Result};

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
struct ShardRow {
    signatures: [u32; ROW_WIDTH],
    offsets_and_sizes: [u64; ROW_WIDTH], // | key_size: 16 | val_size: 16 | file_offset: 32 |
}

impl ShardRow {
    #[inline]
    fn lookup(&self, sig: u32, start_idx: &mut usize) -> Option<usize> {
        if let Some(rel_idx) = self.signatures[*start_idx..].iter().position_simd(sig) {
            let abs_idx = rel_idx + *start_idx;
            *start_idx = abs_idx + 1;
            Some(abs_idx)
        } else {
            None
        }
    }
}

#[test]
fn test_row_lookup() -> Result<()> {
    let mut row = ShardRow {
        signatures: [0; ROW_WIDTH],
        offsets_and_sizes: [0; ROW_WIDTH],
    };

    row.signatures[7] = 123;
    row.signatures[8] = 123;
    row.signatures[9] = 123;
    row.signatures[90] = 123;
    row.signatures[ROW_WIDTH - 1] = 999;

    let mut start = 0;
    assert_eq!(row.lookup(123, &mut start), Some(7));
    assert_eq!(start, 8);
    assert_eq!(row.lookup(123, &mut start), Some(8));
    assert_eq!(start, 9);
    assert_eq!(row.lookup(123, &mut start), Some(9));
    assert_eq!(start, 10);
    assert_eq!(row.lookup(123, &mut start), Some(90));
    assert_eq!(start, 91);
    assert_eq!(row.lookup(123, &mut start), None);
    assert_eq!(start, 91);

    start = 0;
    assert_eq!(row.lookup(0, &mut start), Some(0));
    assert_eq!(start, 1);

    start = 0;
    assert_eq!(row.lookup(999, &mut start), Some(ROW_WIDTH - 1));
    assert_eq!(start, ROW_WIDTH);

    assert_eq!(row.lookup(999, &mut start), None);
    assert_eq!(start, ROW_WIDTH);

    Ok(())
}

#[repr(C, align(4096))]
struct PageAligned<T>(T);

pub(crate) const SHARD_FILE_MAGIC: [u8; 8] = *b"CandyStr";
pub(crate) const SHARD_FILE_VERSION: u64 = 0x08;

#[derive(Clone, Copy, Default, Debug, Pod, Zeroable)]
#[repr(C)]
struct MetaHeader {
    magic: [u8; 8],
    version: u64,
}

#[repr(C)]
struct ShardHeader {
    metadata: MetaHeader,
    wasted_bytes: AtomicU64,
    write_offset: AtomicU64,
    num_entries: AtomicU64,
    rows: PageAligned<[ShardRow; NUM_ROWS]>,
}

pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;
const _: () = assert!(HEADER_SIZE % 4096 == 0);

#[derive(Debug)]
pub(crate) enum InsertStatus {
    Added,
    Replaced(Vec<u8>),
    KeyDoesNotExist,
    CompactionNeeded(u64),
    SplitNeeded,
    AlreadyExists(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum InsertMode<'a> {
    Set,
    Replace(Option<&'a [u8]>),
    GetOrCreate,
    MustCreate,
}

pub(crate) type KVPair = (Vec<u8>, Vec<u8>);

// Note: it's possible to reduce the number row_locks, it we make them per-store rather than per-shard.
// the trivial way that would be to use NUM_ROWS (without risking deadlocks), which means you can have 64
// concurrent operations. if you'd want more concurrency, it's possible to take the number of shards,
// rounded down to the nearest power of two, and add that many MSBs from the shard selector to create a
// shard+row combination that would be safe from deadlocks. however, it seems that holding 64 locks for
// 64MB isn't that much, and you'd still need a RW lock per shard anyway.

pub(crate) struct Shard {
    pub(crate) span: Range<u32>,
    file: File,
    pub(crate) config: Arc<InternalConfig>,
    #[allow(dead_code)]
    mmap: MmapMut, // needed to prevent it from dropping
    header: &'static mut ShardHeader,
    row_locks: Vec<RwLock<()>>,
    #[cfg(feature = "flush_aggregation")]
    sync_agg_mutex: parking_lot::Mutex<()>,
    #[cfg(feature = "flush_aggregation")]
    in_sync_agg_delay: std::sync::atomic::AtomicBool,
    stats: Arc<InternalStats>,
}

enum TryReplaceStatus<'a> {
    KeyDoesNotExist(RwLockWriteGuard<'a, ()>),
    KeyExistsNotReplaced(Vec<u8>),
    KeyExistsReplaced(Vec<u8>),
}

impl Shard {
    pub(crate) const EXPECTED_CAPACITY: usize = (NUM_ROWS * ROW_WIDTH * 9) / 10; // ~ 29,500

    pub(crate) fn open(
        filename: PathBuf,
        span: Range<u32>,
        truncate: bool,
        config: Arc<InternalConfig>,
        stats: Arc<InternalStats>,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(&filename)?;

        let mut file_size = file.metadata()?.len();
        if file_size != 0 {
            let mut meta_header = MetaHeader::default();
            let sz = file.read(bytes_of_mut(&mut meta_header))?;
            if sz != size_of::<MetaHeader>()
                || meta_header.magic != SHARD_FILE_MAGIC
                || meta_header.version != SHARD_FILE_VERSION
            {
                if config.clear_on_unsupported_version {
                    file.set_len(0)?;
                    file_size = 0;
                } else {
                    bail!(
                        "{filename:?} unsupported magic={:?} version=0x{:016x} size={}",
                        meta_header.magic,
                        meta_header.version,
                        file_size,
                    );
                }
            }

            if file_size != 0 && file_size < HEADER_SIZE {
                if config.clear_on_unsupported_version {
                    file.set_len(0)?;
                    file_size = 0;
                } else {
                    bail!("corrupt shard file (size={})", file_size);
                }
            }
        }

        if file_size == 0 {
            if config.truncate_up {
                // when creating, set the file's length so that we won't need to extend it every time we write
                // (saves on file metadata updates)
                file.set_len(HEADER_SIZE + config.max_shard_size as u64)?;
            } else {
                file.set_len(HEADER_SIZE)?;
            }
        }

        let mut mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        if cfg!(target_family = "unix") {
            if config.mlock_headers {
                unsafe { libc::mlock(mmap.as_ptr() as *const _, mmap.len()) };
            }
        }

        let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ShardHeader) };
        header.metadata.magic = SHARD_FILE_MAGIC;
        header.metadata.version = SHARD_FILE_VERSION;

        if file_size > 0 {
            // if the shard existed before, update the stats
            stats.num_inserts.fetch_add(
                header.num_entries.load(Ordering::Relaxed) as usize,
                Ordering::Relaxed,
            );
        }

        let mut row_locks = Vec::with_capacity(NUM_ROWS);
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
            #[cfg(feature = "flush_aggregation")]
            sync_agg_mutex: parking_lot::Mutex::new(()),
            #[cfg(feature = "flush_aggregation")]
            in_sync_agg_delay: std::sync::atomic::AtomicBool::new(false),
            stats,
        })
    }

    pub(crate) fn flush(&self) -> Result<()> {
        //self.mmap.flush()? -- fdatasync should take care of that as well
        self.file.sync_data()?;
        Ok(())
    }

    #[inline]
    fn extract_offset_and_size(offset_and_size: u64) -> (usize, usize, u64) {
        let klen = (offset_and_size >> 48) as usize;
        debug_assert_eq!(klen >> 14, 0, "attempting to read a special key");
        let vlen = ((offset_and_size >> 32) & 0xffff) as usize;
        let offset = (offset_and_size as u32) as u64;
        (klen, vlen, offset)
    }

    // reading doesn't require holding any locks - we only ever extend the file, never overwrite data
    fn read_kv(&self, offset_and_size: u64) -> Result<KVPair> {
        let (klen, vlen, offset) = Self::extract_offset_and_size(offset_and_size);
        let mut buf = vec![0u8; klen + vlen];
        self.file.read_exact_at(&mut buf, HEADER_SIZE + offset)?;

        self.stats
            .num_read_bytes
            .fetch_add(buf.len(), Ordering::Relaxed);
        self.stats.num_read_ops.fetch_add(1, Ordering::Relaxed);

        let val = buf[klen..klen + vlen].to_owned();
        buf.truncate(klen);

        Ok((buf, val))
    }

    // writing doesn't require holding any locks since we write with an offset
    fn write_kv(&self, key: &[u8], val: &[u8]) -> Result<u64> {
        let entry_size = key.len() + val.len();
        let mut buf = vec![0u8; entry_size];
        buf[..key.len()].copy_from_slice(key);
        buf[key.len()..].copy_from_slice(val);

        // atomically allocate some area. it may leak if the IO below fails or if we crash before updating the
        // offsets_and_size array, but we're okay with leaks
        let write_offset = self
            .header
            .write_offset
            .fetch_add(buf.len() as u64, Ordering::SeqCst) as u64;

        // now writing can be non-atomic (pwrite)
        self.file.write_all_at(&buf, HEADER_SIZE + write_offset)?;
        self.stats.add_entry(entry_size);

        Ok(((key.len() as u64) << 48) | ((val.len() as u64) << 32) | write_offset)
    }

    pub(crate) fn read_at(&self, row_idx: usize, entry_idx: usize) -> Result<Option<KVPair>> {
        let _guard = self.row_locks[row_idx].read();
        let row = &self.header.rows.0[row_idx];
        if row.signatures[entry_idx] != INVALID_SIG {
            Ok(Some(self.read_kv(row.offsets_and_sizes[entry_idx])?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn compact_into(&self, new_shard: &mut Shard) -> Result<()> {
        for res in self.unlocked_iter() {
            let (k, v) = res?;
            let ph = PartedHash::new(&self.config.hash_seed, &k);
            new_shard.insert(ph, &k, &v, InsertMode::MustCreate, false)?;
        }

        Ok(())
    }
    pub(crate) fn split_into(&self, bottom_shard: &Shard, top_shard: &Shard) -> Result<()> {
        for res in self.unlocked_iter() {
            let (k, v) = res?;

            let ph = PartedHash::new(&self.config.hash_seed, &k);
            if (ph.shard_selector() as u32) < bottom_shard.span.end {
                bottom_shard.insert(ph, &k, &v, InsertMode::MustCreate, false)?;
            } else {
                top_shard.insert(ph, &k, &v, InsertMode::MustCreate, false)?;
            }
        }
        Ok(())
    }

    fn unlocked_iter<'b>(&'b self) -> impl Iterator<Item = Result<KVPair>> + 'b {
        self.header.rows.0.iter().flat_map(|row| {
            row.signatures.iter().enumerate().filter_map(|(idx, &sig)| {
                if sig == INVALID_SIG {
                    None
                } else {
                    Some(self.read_kv(row.offsets_and_sizes[idx]))
                }
            })
        })
    }

    fn get_row(&self, ph: PartedHash) -> (RwLockReadGuard<()>, &ShardRow) {
        let row_idx = (ph.row_selector() as usize) % NUM_ROWS;
        let guard = self.row_locks[row_idx].read();
        let row = &self.header.rows.0[row_idx];
        (guard, row)
    }

    pub(crate) fn get_by_hash(&self, ph: PartedHash) -> Result<Vec<KVPair>> {
        let mut first_time = true;
        let (_guard, row) = self.get_row(ph);
        let mut kvs = Vec::with_capacity(1);
        let mut start = 0;
        while let Some(idx) = row.lookup(ph.signature(), &mut start) {
            kvs.push(self.read_kv(row.offsets_and_sizes[idx])?);
            if first_time {
                self.stats
                    .num_positive_lookups
                    .fetch_add(1, Ordering::Relaxed);
                first_time = false;
            }
        }
        if kvs.is_empty() {
            self.stats
                .num_negative_lookups
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(kvs)
    }

    pub(crate) fn get(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (_guard, row) = self.get_row(ph);
        let mut start = 0;
        while let Some(idx) = row.lookup(ph.signature(), &mut start) {
            let (k, v) = self.read_kv(row.offsets_and_sizes[idx])?;
            if key == k {
                self.stats
                    .num_positive_lookups
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Some(v));
            }
        }
        self.stats
            .num_negative_lookups
            .fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    #[cfg(feature = "flush_aggregation")]
    fn flush_aggregation(&self) -> Result<()> {
        let Some(delay) = self.config.flush_aggregation_delay else {
            return Ok(());
        };

        let do_sync = || {
            self.in_sync_agg_delay.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
            self.in_sync_agg_delay.store(false, Ordering::SeqCst);
            self.file.sync_data()
            /*self.mmap.flush()?;
            let rc = unsafe {
                libc::sync_file_range(
                    self.file.as_raw_fd(),
                    HEADER_SIZE as i64,
                    HEADER_SIZE as i64 + self.header.write_offset.load(Ordering::Relaxed) as i64,
                    libc::SYNC_FILE_RANGE_WAIT_BEFORE
                        | libc::SYNC_FILE_RANGE_WRITE
                        | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                )
            };
            ensure!(rc == 0);
            Ok(())
            */
        };

        if let Some(_guard) = self.sync_agg_mutex.try_lock() {
            // we're the first ones here. wait for the aggregation duration and sync the file
            do_sync()?;
        } else {
            // another thread is currently sync'ing, we're waiting in line. if the holder of the lock is in the
            // sleep (aggregation) phase, we can just wait for it to finish and return -- the other thread will
            // have sync'ed us by the time we got the lock. otherwise, we'll need to sync as well
            let was_in_delay = self.in_sync_agg_delay.load(Ordering::Relaxed);
            let _guard = self.sync_agg_mutex.lock();
            if !was_in_delay {
                do_sync()?;
            }
        }
        Ok(())
    }

    fn try_replace<'a>(
        &'a self,
        guard: RwLockWriteGuard<'a, ()>,
        row: &mut ShardRow,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
        mode: InsertMode,
        inc_stats: bool,
    ) -> Result<TryReplaceStatus> {
        let mut start = 0;
        while let Some(idx) = row.lookup(ph.signature(), &mut start) {
            let (k, existing_val) = self.read_kv(row.offsets_and_sizes[idx])?;
            if key != k {
                continue;
            }
            match mode {
                InsertMode::MustCreate => {
                    bail!(CandyError::KeyAlreadyExists(key.into(), ph.as_u64()))
                }
                InsertMode::GetOrCreate => {
                    // no-op, key already exists
                    if inc_stats {
                        self.stats
                            .num_positive_lookups
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(TryReplaceStatus::KeyExistsNotReplaced(existing_val));
                }
                InsertMode::Set => {
                    // fall through
                }
                InsertMode::Replace(expected_val) => {
                    if expected_val.is_some_and(|expected_val| expected_val != existing_val) {
                        return Ok(TryReplaceStatus::KeyExistsNotReplaced(existing_val));
                    }
                }
            }

            // optimization
            if val != existing_val {
                row.offsets_and_sizes[idx] = self.write_kv(key, val)?;
                self.header
                    .wasted_bytes
                    .fetch_add((k.len() + existing_val.len()) as u64, Ordering::Relaxed);
                if inc_stats {
                    self.stats.num_updates.fetch_add(1, Ordering::Relaxed);
                }
                #[cfg(feature = "flush_aggregation")]
                {
                    drop(guard);
                    self.flush_aggregation()?;
                }
            }
            return Ok(TryReplaceStatus::KeyExistsReplaced(existing_val));
        }

        Ok(TryReplaceStatus::KeyDoesNotExist(guard))
    }

    fn get_row_mut(&self, ph: PartedHash) -> (RwLockWriteGuard<()>, &mut ShardRow) {
        let row_idx = (ph.row_selector() as usize) % NUM_ROWS;
        let guard = self.row_locks[row_idx].write();
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
        full_key: &[u8],
        val: &[u8],
        mode: InsertMode,
        inc_stats: bool,
    ) -> Result<InsertStatus> {
        let (guard, row) = self.get_row_mut(ph);

        if self.header.write_offset.load(Ordering::Relaxed) as u64
            + (full_key.len() + val.len()) as u64
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

        match self.try_replace(guard, row, ph, &full_key, val, mode, inc_stats)? {
            TryReplaceStatus::KeyDoesNotExist(_guard) => {
                if matches!(mode, InsertMode::Replace(_)) {
                    return Ok(InsertStatus::KeyDoesNotExist);
                }

                // find an empty slot
                let mut start = 0;
                if let Some(idx) = row.lookup(INVALID_SIG, &mut start) {
                    let new_off = self.write_kv(&full_key, val)?;

                    // we don't want a reorder to happen here - first write the offset, then the signature
                    row.offsets_and_sizes[idx] = new_off;
                    std::sync::atomic::fence(Ordering::SeqCst);
                    row.signatures[idx] = ph.signature();
                    if inc_stats {
                        self.stats.num_inserts.fetch_add(1, Ordering::Relaxed);
                    }
                    self.header.num_entries.fetch_add(1, Ordering::Relaxed);
                    #[cfg(feature = "flush_aggregation")]
                    {
                        drop(_guard);
                        self.flush_aggregation()?;
                    }
                    Ok(InsertStatus::Added)
                } else {
                    // no room in this row, must split
                    Ok(InsertStatus::SplitNeeded)
                }
            }
            TryReplaceStatus::KeyExistsNotReplaced(existing) => {
                Ok(InsertStatus::AlreadyExists(existing))
            }
            TryReplaceStatus::KeyExistsReplaced(existing) => Ok(InsertStatus::Replaced(existing)),
        }
    }

    pub(crate) fn remove(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (_guard, row) = self.get_row_mut(ph);

        let mut start = 0;
        while let Some(idx) = row.lookup(ph.signature(), &mut start) {
            let (k, v) = self.read_kv(row.offsets_and_sizes[idx])?;
            if key == k {
                row.signatures[idx] = INVALID_SIG;
                // we managed to remove this key
                self.stats.num_removals.fetch_add(1, Ordering::Relaxed);
                self.header.num_entries.fetch_sub(1, Ordering::Relaxed);
                self.header
                    .wasted_bytes
                    .fetch_add((k.len() + v.len()) as u64, Ordering::Relaxed);
                #[cfg(feature = "flush_aggregation")]
                {
                    drop(_guard);
                    self.flush_aggregation()?;
                }
                return Ok(Some(v));
            }
        }

        Ok(None)
    }

    pub(crate) fn get_write_offset(&self) -> u64 {
        self.header.write_offset.load(Ordering::Relaxed)
    }
    pub(crate) fn get_wasted_bytes(&self) -> u64 {
        self.header.wasted_bytes.load(Ordering::Relaxed)
    }
}
