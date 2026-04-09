use memmap2::MmapMut;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use simd_itertools::PositionSimd;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use std::{
    fs::File,
    hash::Hasher,
    mem::{offset_of, size_of},
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::internal::{
    FILE_OFFSET_ALIGNMENT, HashCoord, INDEX_FILE_SIGNATURE, INDEX_FILE_VERSION, MAX_DATA_FILES,
    MIN_INITIAL_ROWS, MIN_SPLIT_LEVEL, PAGE_SIZE, ROW_WIDTH, SIZE_HINT_UNIT, index_file_path,
    index_rows_file_path, invalid_data_error, read_available_at, unexpected_eof_error,
};
use crate::types::{Config, Error, Result};

const CHECKPOINT_SLOT_COUNT: usize = 2;

#[derive(Clone, Copy)]
struct CheckpointCursor {
    generation: u64,
    file_ordinal: u64,
    offset: u64,
}

#[derive(FromBytes, IntoBytes, KnownLayout)]
#[repr(C)]
pub(crate) struct CheckpointSlot {
    generation: AtomicU64,
    file_ordinal: AtomicU64,
    offset: AtomicU64,
    checksum: AtomicU64,
}

const _: () = assert!(size_of::<CheckpointSlot>() == 32);

pub fn checkpoint_slot_checksum(generation: u64, file_ordinal: u64, offset: u64) -> u64 {
    let mut hasher = siphasher::sip::SipHasher13::new();
    hasher.write_u64(generation);
    hasher.write_u64(file_ordinal);
    hasher.write_u64(offset);
    hasher.finish()
}

#[derive(FromBytes, IntoBytes, KnownLayout)]
#[repr(C)]
pub(crate) struct IndexFileHeader {
    pub(crate) signature: [u8; 8],
    pub(crate) version: u32,
    _padding16: u32,
    pub(crate) hash_key_0: u64,
    pub(crate) hash_key_1: u64,
    _padding64: [u8; 64 - 4 * 8],

    ///////////////////////////////////
    // runtime state
    ///////////////////////////////////
    pub(crate) global_split_level: AtomicU64,
    _padding128: [u8; 64 - 8],

    ///////////////////////////////////
    // rebuild state
    ///////////////////////////////////
    /// Persisted replay cursor, double-buffered so recovery can pick the
    /// newest valid slot after crashes or torn writes.
    pub(crate) checkpoint_slots: [CheckpointSlot; CHECKPOINT_SLOT_COUNT],
    _padding1024: [u8; 896 - CHECKPOINT_SLOT_COUNT * 32],

    ///////////////////////////////////
    // stats
    ///////////////////////////////////
    pub(crate) committed_num_entries: AtomicU64,

    _trailer: [u8; PAGE_SIZE - 1024 - 8],
}

const _: () = assert!(offset_of!(IndexFileHeader, global_split_level) == 64);
const _: () = assert!(offset_of!(IndexFileHeader, checkpoint_slots) == 128);
const _: () = assert!(offset_of!(IndexFileHeader, committed_num_entries) == 1024);
const _: () = assert!(size_of::<IndexFileHeader>() == PAGE_SIZE);

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(transparent)]
pub struct EntryPointer(pub u64);

impl EntryPointer {
    pub const INVALID_POINTER: Self = Self(0);

    pub fn new(file_idx: u16, file_offset: u64, size: usize, masked_row_selector: u32) -> Self {
        debug_assert!(size > 0 && size <= u8::MAX as usize * SIZE_HINT_UNIT);

        let fi = (file_idx as u64) & ((1 << 12) - 1);
        let fo = ((file_offset / FILE_OFFSET_ALIGNMENT) & ((1 << 26) - 1)) << 12;
        let sh = (size.div_ceil(SIZE_HINT_UNIT) as u64) << (12 + 26);
        let rs = (masked_row_selector as u64) << (12 + 26 + 8);
        Self(fi | fo | sh | rs)
    }

    pub fn file_idx(&self) -> u16 {
        (self.0 & ((1 << 12) - 1)) as u16
    }

    pub fn file_offset(&self) -> u64 {
        ((self.0 >> 12) & ((1 << 26) - 1)) * FILE_OFFSET_ALIGNMENT
    }

    pub fn size_hint(&self) -> usize {
        ((self.0 >> (12 + 26)) & ((1 << 8) - 1)) as usize * SIZE_HINT_UNIT
    }

    pub fn masked_row_selector(&self) -> u32 {
        (self.0 >> (12 + 26 + 8)) as u32
    }

    pub fn is_valid(&self) -> bool {
        self.0 != Self::INVALID_POINTER.0
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout)]
#[repr(C)]
pub(crate) struct RowLayout {
    pub(crate) split_level: AtomicU64,
    _padding: [u8; 56],
    pub(crate) signatures: [u32; ROW_WIDTH],
    pub(crate) pointers: [EntryPointer; ROW_WIDTH],
}

const _: () = assert!(size_of::<RowLayout>() == PAGE_SIZE);
const _: () = assert!(offset_of!(RowLayout, signatures) % 8 == 0);
const _: () = assert!(offset_of!(RowLayout, pointers) % 8 == 0);

impl RowLayout {
    /// Returns `true` if the entry at `col` has selector bits consistent with
    /// being in the row at `row_idx` with the given `split_level`.  Phantom
    /// entries left by an interrupted split will fail this check.
    pub(crate) fn entry_belongs_to_row(
        &self,
        col: usize,
        row_idx: usize,
        split_level: u64,
    ) -> bool {
        if split_level <= MIN_SPLIT_LEVEL as u64 {
            return true;
        }
        let selector_bits = split_level - MIN_SPLIT_LEVEL as u64;
        let mask = (1u32 << selector_bits) - 1;
        let expected = (row_idx >> MIN_SPLIT_LEVEL) as u32 & mask;
        self.pointers[col].masked_row_selector() & mask == expected
    }

    pub(crate) fn iter_matches(&self, hash_coord: HashCoord) -> RowMatchIterator<'_> {
        RowMatchIterator {
            row: self,
            hash_coord,
            offset: 0,
        }
    }

    pub(crate) fn find_free_slot(&self) -> Option<usize> {
        self.signatures
            .iter()
            .position_simd(|&sig| sig == HashCoord::INVALID_SIG)
    }

    pub(crate) fn insert(&mut self, idx: usize, sig: u32, ptr: EntryPointer) {
        debug_assert!(self.signatures[idx] == HashCoord::INVALID_SIG);
        self.signatures[idx] = sig;
        crate::crash_point("insert_after_sig");
        self.pointers[idx] = ptr;
    }

    pub(crate) fn remove(&mut self, idx: usize) {
        self.signatures[idx] = HashCoord::INVALID_SIG;
        self.pointers[idx] = EntryPointer::INVALID_POINTER;
    }

    pub(crate) fn replace_pointer(&mut self, idx: usize, new_ptr: EntryPointer) {
        self.pointers[idx] = new_ptr;
    }

    pub(crate) fn set_split_level(&mut self, new_sl: u64) {
        self.split_level.store(new_sl, Ordering::Release);
    }
}

pub(crate) struct RowMatchIterator<'a> {
    row: &'a RowLayout,
    hash_coord: HashCoord,
    offset: usize,
}

impl Iterator for RowMatchIterator<'_> {
    type Item = (usize, EntryPointer);

    fn next(&mut self) -> Option<Self::Item> {
        while self.offset < ROW_WIDTH {
            if let Some(idx) = self.row.signatures[self.offset..]
                .iter()
                .position_simd(|&sig| sig == self.hash_coord.sig)
            {
                let real_idx = self.offset + idx;
                self.offset = real_idx + 1;
                let ptr = self.row.pointers[real_idx];
                if ptr.is_valid()
                    && ptr.masked_row_selector() == self.hash_coord.masked_row_selector()
                {
                    return Some((real_idx, ptr));
                }
            } else {
                self.offset = ROW_WIDTH;
            }
        }
        None
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout)]
#[repr(C)]
pub(crate) struct IndexFileLayout {
    pub(crate) header: IndexFileHeader,
    // note: we don't keep committed and uncommitted waste_levels for space efficiency and because
    // they only need be approximate
    pub(crate) waste_levels: [AtomicU32; MAX_DATA_FILES as usize],
}

const _: () = assert!(size_of::<IndexFileLayout>() == PAGE_SIZE * 5);

fn row_count_for_len(len: usize) -> usize {
    len / size_of::<RowLayout>()
}

fn row_offset(idx: usize) -> usize {
    idx * size_of::<RowLayout>()
}

fn row_bytes(bytes: &[u8], idx: usize) -> &[u8] {
    let start = row_offset(idx);
    let end = start + size_of::<RowLayout>();
    &bytes[start..end]
}

fn row_bytes_mut(bytes: &mut [u8], idx: usize) -> &mut [u8] {
    let start = row_offset(idx);
    let end = start + size_of::<RowLayout>();
    &mut bytes[start..end]
}

fn row_ref_bytes(bytes: &[u8], idx: usize) -> &RowLayout {
    unsafe { &*(row_bytes(bytes, idx).as_ptr() as *const RowLayout) }
}

fn row_mut_bytes(bytes: &mut [u8], idx: usize) -> &mut RowLayout {
    RowLayout::try_mut_from_bytes(row_bytes_mut(bytes, idx))
        .expect("row bytes should contain an aligned row")
}

unsafe fn row_mut_ptr(base_ptr: *const u8, idx: usize) -> *mut RowLayout {
    unsafe { base_ptr.add(row_offset(idx)) as *mut RowLayout }
}

pub(crate) struct RowsTableReadGuard<'a> {
    index_file: &'a IndexFile,
    pub(crate) row_guard: RwLockReadGuard<'a, MmapMut>,
}

impl<'a> RowsTableReadGuard<'a> {
    pub(crate) fn row(&self, idx: usize) -> RowReadGuard<'_> {
        let row_guard = self.index_file.row_locks[idx & self.index_file.row_locks_mask].read();
        let row_count = row_count_for_len(self.row_guard.len());
        assert!(
            idx < row_count,
            "row index out of bounds: {idx} >= {row_count}"
        );
        let row = row_ref_bytes(&self.row_guard[..], idx);
        RowReadGuard {
            _row_guard: row_guard,
            row,
        }
    }

    pub(crate) fn shard_id(&self, idx: usize) -> usize {
        idx & self.index_file.row_locks_mask
    }

    pub(crate) fn lock_shard(&self, shard_id: usize) -> RwLockWriteGuard<'_, ()> {
        self.index_file.row_locks[shard_id].write()
    }

    pub(crate) fn row_mut(&self, idx: usize) -> RowWriteGuard<'_> {
        let shard_idx = idx & self.index_file.row_locks_mask;
        let row_guard = self.index_file.row_locks[shard_idx].write();
        let row_count = row_count_for_len(self.row_guard.len());
        assert!(
            idx < row_count,
            "row index out of bounds: {idx} >= {row_count}"
        );
        let row = unsafe { &mut *row_mut_ptr(self.row_guard.as_ptr(), idx) };
        RowWriteGuard {
            _row_guard: row_guard,
            row,
            shard_idx,
        }
    }

    pub(crate) unsafe fn unlocked_row_ptr(&self, idx: usize) -> *mut RowLayout {
        let row_count = row_count_for_len(self.row_guard.len());
        assert!(
            idx < row_count,
            "row index out of bounds: {idx} >= {row_count}"
        );
        unsafe { row_mut_ptr(self.row_guard.as_ptr(), idx) }
    }
}

pub(crate) struct RowsTableWriteGuard<'a> {
    pub(crate) row_guard: RwLockWriteGuard<'a, MmapMut>,
}

impl RowsTableWriteGuard<'_> {
    fn row_mut(&mut self, idx: usize) -> &mut RowLayout {
        let row_count = row_count_for_len(self.row_guard.len());
        assert!(
            idx < row_count,
            "row index out of bounds: {idx} >= {row_count}"
        );
        row_mut_bytes(&mut self.row_guard[..], idx)
    }
}

pub(crate) struct RowReadGuard<'a> {
    _row_guard: RwLockReadGuard<'a, ()>,
    row: &'a RowLayout,
}

impl Deref for RowReadGuard<'_> {
    type Target = RowLayout;

    fn deref(&self) -> &Self::Target {
        self.row
    }
}

pub(crate) struct RowWriteGuard<'a> {
    _row_guard: RwLockWriteGuard<'a, ()>,
    row: &'a mut RowLayout,
    pub(crate) shard_idx: usize,
}

impl Deref for RowWriteGuard<'_> {
    type Target = RowLayout;

    fn deref(&self) -> &Self::Target {
        self.row
    }
}

impl DerefMut for RowWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.row
    }
}

pub(crate) struct IndexFile {
    /// Kept on Windows so `sync_all` can call `FlushFileBuffers`.
    /// On Linux the fd is closed after mmap; `msync` suffices for durability.
    #[cfg(windows)]
    header_file: File,
    rows_file: File,
    /// Fixed mapping covering the header + waste-level pages. Never remapped,
    /// so `header_ref()` / `layout_prefix_ref()` are always stable without a lock.
    header_mmap: MmapMut,
    /// Growable mapping covering only the row pages. Remapped on grow/shrink/reset.
    rows_mmap: RwLock<MmapMut>,
    row_locks: Vec<RwLock<()>>,
    row_locks_mask: usize,
    config: Arc<Config>,
    /// Cached checkpoint state so concurrent readers (e.g. compaction candidate
    /// selection) always see a consistent snapshot without going through the
    /// double-buffer slot protocol.
    cached_checkpoint_generation: AtomicU64,
    cached_checkpoint_ordinal: AtomicU64,
    cached_checkpoint_offset: AtomicU64,
}

impl IndexFile {
    fn read_checkpoint_slot(slot: &CheckpointSlot) -> Option<CheckpointCursor> {
        let generation = slot.generation.load(Ordering::Acquire);
        if generation == 0 {
            return None;
        }

        let file_ordinal = slot.file_ordinal.load(Ordering::Relaxed);
        let offset = slot.offset.load(Ordering::Relaxed);
        let checksum = slot.checksum.load(Ordering::Acquire);
        if checksum != checkpoint_slot_checksum(generation, file_ordinal, offset) {
            return None;
        }

        Some(CheckpointCursor {
            generation,
            file_ordinal,
            offset,
        })
    }

    fn durable_checkpoint(&self) -> Option<CheckpointCursor> {
        self.header_ref()
            .checkpoint_slots
            .iter()
            .filter_map(Self::read_checkpoint_slot)
            .max_by_key(|cursor| cursor.generation)
    }

    pub(crate) fn checkpoint_cursor(&self) -> (u64, u64) {
        let generation = self.cached_checkpoint_generation.load(Ordering::Acquire);
        if generation == 0 {
            return (0, 0);
        }
        let ordinal = self.cached_checkpoint_ordinal.load(Ordering::Relaxed);
        let offset = self.cached_checkpoint_offset.load(Ordering::Relaxed);
        (ordinal, offset)
    }

    pub(crate) fn checkpoint_generation(&self) -> u64 {
        self.cached_checkpoint_generation.load(Ordering::Acquire)
    }

    pub(crate) fn persist_checkpoint_cursor(&self, ordinal: u64, offset: u64) {
        let current_gen = self.cached_checkpoint_generation.load(Ordering::Relaxed);
        let next_generation = current_gen
            .checked_add(1)
            .expect("checkpoint generation overflow");
        let slot =
            &self.header_ref().checkpoint_slots[next_generation as usize % CHECKPOINT_SLOT_COUNT];

        slot.checksum.store(0, Ordering::Release);
        slot.generation.store(next_generation, Ordering::Relaxed);
        slot.file_ordinal.store(ordinal, Ordering::Relaxed);
        slot.offset.store(offset, Ordering::Relaxed);
        slot.checksum.store(
            checkpoint_slot_checksum(next_generation, ordinal, offset),
            Ordering::Release,
        );

        // Update the cache so concurrent readers see the new values immediately.
        self.cached_checkpoint_ordinal
            .store(ordinal, Ordering::Relaxed);
        self.cached_checkpoint_offset
            .store(offset, Ordering::Relaxed);
        self.cached_checkpoint_generation
            .store(next_generation, Ordering::Release);
    }

    #[cfg(target_os = "linux")]
    fn maybe_lock_mmap(config: &Config, mmap: &MmapMut) {
        if config.mlock_index {
            let _ = mmap.lock();
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn maybe_lock_mmap(_config: &Config, _mmap: &MmapMut) {}

    fn read_existing_header(header_file: &File, header_len: usize) -> Result<((u64, u64), u64)> {
        if header_len < size_of::<IndexFileLayout>() {
            return Err(unexpected_eof_error("index file header too short"));
        }
        if header_len != size_of::<IndexFileLayout>() {
            return Err(invalid_data_error("index header file has unexpected size"));
        }

        let header = read_available_at(header_file, size_of::<IndexFileHeader>(), 0)
            .map_err(Error::IOError)?;
        if header.len() < size_of::<IndexFileHeader>() {
            return Err(unexpected_eof_error("index file header too short"));
        }
        let header = IndexFileHeader::read_from_bytes(&header)
            .map_err(|_| invalid_data_error("invalid index file header size"))?;
        if &header.signature != INDEX_FILE_SIGNATURE || header.version != INDEX_FILE_VERSION {
            return Err(invalid_data_error("invalid index file header"));
        }

        Ok((
            (header.hash_key_0, header.hash_key_1),
            header.global_split_level.load(Ordering::Relaxed),
        ))
    }

    pub(crate) fn existing_hash_key(base_path: &Path) -> Result<Option<(u64, u64)>> {
        let header_path = index_file_path(base_path);
        let header_file = match File::options().read(true).open(header_path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::IOError(err)),
        };
        let header_len = header_file.metadata().map_err(Error::IOError)?.len() as usize;
        if header_len == 0 {
            return Ok(None);
        }

        let (hash_key, _) = Self::read_existing_header(&header_file, header_len)?;
        Ok(Some(hash_key))
    }

    fn validate_existing(
        header_file: &File,
        header_len: usize,
        rows_len: usize,
        hash_key: (u64, u64),
    ) -> Result<()> {
        if !rows_len.is_multiple_of(PAGE_SIZE) {
            return Err(invalid_data_error(
                "index rows file size is not page aligned",
            ));
        }

        let (stored_hash_key, gsl) = Self::read_existing_header(header_file, header_len)?;
        if stored_hash_key != hash_key {
            return Err(invalid_data_error("index hash key mismatch"));
        }

        let row_count = row_count_for_len(rows_len);
        if row_count < MIN_INITIAL_ROWS || !row_count.is_power_of_two() {
            return Err(invalid_data_error("invalid index file row count"));
        }

        if gsl < MIN_SPLIT_LEVEL as u64 {
            return Err(invalid_data_error("invalid index global split level"));
        }

        let uncommitted_rows = 1usize
            .checked_shl(gsl as u32)
            .ok_or_else(|| invalid_data_error("index global split level overflow"))?;
        if uncommitted_rows > row_count {
            return Err(invalid_data_error(
                "index global split level exceeds file size",
            ));
        }

        Ok(())
    }

    pub(crate) fn flush_header(&self) -> Result<()> {
        self.header_mmap.flush().map_err(Error::IOError)
    }

    pub(crate) fn open(base_path: &Path, config: Arc<Config>) -> Result<Self> {
        let hash_key = config.hash_key;
        let num_rows = (config.initial_capacity / ROW_WIDTH)
            .max(MIN_INITIAL_ROWS)
            .next_power_of_two();
        let num_locks = config.max_concurrency.min(num_rows).next_power_of_two();
        let row_locks = (0..num_locks).map(|_| RwLock::new(())).collect::<Vec<_>>();
        let row_locks_mask = num_locks - 1;

        let header_path = index_file_path(base_path);
        let rows_path = index_rows_file_path(base_path);

        let header_file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(header_path)
            .map_err(Error::IOError)?;
        let rows_file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(rows_path)
            .map_err(Error::IOError)?;

        let header_size = size_of::<IndexFileLayout>();
        let header_len = header_file.metadata().map_err(Error::IOError)?.len() as usize;
        let rows_len = rows_file.metadata().map_err(Error::IOError)?.len() as usize;
        let new_file = header_len == 0 && rows_len == 0;
        let rows_size = num_rows * size_of::<RowLayout>();

        if new_file {
            header_file
                .set_len(header_size as u64)
                .map_err(Error::IOError)?;
            rows_file
                .set_len(rows_size as u64)
                .map_err(Error::IOError)?;
        } else {
            Self::validate_existing(&header_file, header_len, rows_len, config.hash_key)?;
        }

        let actual_rows_size = if new_file { rows_size } else { rows_len };

        let header_mmap = unsafe {
            memmap2::MmapOptions::new()
                .len(header_size)
                .map_mut(&header_file)
        }
        .map_err(Error::IOError)?;
        Self::maybe_lock_mmap(config.as_ref(), &header_mmap);

        let rows_mmap = unsafe {
            memmap2::MmapOptions::new()
                .len(actual_rows_size)
                .map_mut(&rows_file)
        }
        .map_err(Error::IOError)?;
        Self::maybe_lock_mmap(config.as_ref(), &rows_mmap);

        if new_file {
            header_file.sync_all().map_err(Error::IOError)?;
        }

        let inst = Self {
            #[cfg(windows)]
            header_file,
            rows_file,
            header_mmap,
            rows_mmap: RwLock::new(rows_mmap),
            row_locks,
            row_locks_mask,
            config,
            cached_checkpoint_generation: AtomicU64::new(0),
            cached_checkpoint_ordinal: AtomicU64::new(0),
            cached_checkpoint_offset: AtomicU64::new(0),
        };

        if new_file {
            let rows_table = inst.rows_table_mut();
            inst.init_header_and_rows(rows_table, hash_key)?;
        } else if let Some(cursor) = inst.durable_checkpoint() {
            inst.cached_checkpoint_generation
                .store(cursor.generation, Ordering::Relaxed);
            inst.cached_checkpoint_ordinal
                .store(cursor.file_ordinal, Ordering::Relaxed);
            inst.cached_checkpoint_offset
                .store(cursor.offset, Ordering::Relaxed);
        }

        Ok(inst)
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        // Persist row updates before any header state that claims those rows are durable.
        self.rows_mmap.write().flush().map_err(Error::IOError)?;
        self.rows_file.sync_all().map_err(Error::IOError)?;
        self.header_mmap.flush().map_err(Error::IOError)?;
        #[cfg(windows)]
        self.header_file.sync_all().map_err(Error::IOError)?;
        Ok(())
    }

    pub(crate) fn file_size_bytes(&self) -> u64 {
        let header = size_of::<IndexFileLayout>() as u64;
        let rows = self.rows_file.metadata().map(|m| m.len()).unwrap_or(0);
        header + rows
    }

    pub(crate) fn rows_table(&self) -> RowsTableReadGuard<'_> {
        RowsTableReadGuard {
            index_file: self,
            row_guard: self.rows_mmap.read(),
        }
    }

    pub(crate) fn rows_table_mut(&self) -> RowsTableWriteGuard<'_> {
        RowsTableWriteGuard {
            row_guard: self.rows_mmap.write(),
        }
    }

    /// Returns a direct reference to the header without acquiring any lock.
    ///
    /// Safe because the header mmap is never remapped and the header fields
    /// used for stats are all atomics.
    fn full_header_ref(&self) -> &IndexFileLayout {
        unsafe { &*(self.header_mmap.as_ptr() as *const IndexFileLayout) }
    }

    pub(crate) fn header_ref(&self) -> &IndexFileHeader {
        &self.full_header_ref().header
    }

    pub(crate) fn add_file_waste(&self, file_idx: u16, waste: u32) -> u32 {
        self.full_header_ref().waste_levels[file_idx as usize].fetch_add(waste, Ordering::Relaxed)
            + waste
    }

    pub(crate) fn file_waste(&self, file_idx: u16) -> u32 {
        self.full_header_ref().waste_levels[file_idx as usize].load(Ordering::Relaxed)
    }

    /// Returns the combined waste across all file slots.
    pub(crate) fn total_waste(&self) -> u64 {
        let ref_full = self.full_header_ref();
        let mut total = 0u64;
        for waste in ref_full.waste_levels.iter() {
            total += waste.load(Ordering::Relaxed) as u64;
        }
        total
    }

    /// Takes the combined waste and resets it
    pub(crate) fn take_file_waste(&self, file_idx: u16) -> u32 {
        self.full_header_ref().waste_levels[file_idx as usize].swap(0, Ordering::Relaxed)
    }

    pub(crate) fn grow(&self, nsl: u64) -> Result<Option<Duration>> {
        let mut layout_mut = self.rows_table_mut();
        let gsl = self.header_ref().global_split_level.load(Ordering::Acquire);
        if nsl <= gsl {
            return Ok(None);
        }

        let mut remap_dur = None;
        let required_rows_size = (1usize << nsl) * size_of::<RowLayout>();
        if layout_mut.row_guard.len() < required_rows_size {
            let remap_start = Instant::now();
            let alloc_split = nsl + self.config.remap_scaler as u64;
            let new_rows_size = (1usize << alloc_split) * size_of::<RowLayout>();

            self.rows_file
                .set_len(new_rows_size as u64)
                .map_err(Error::IOError)?;

            #[cfg(target_os = "linux")]
            unsafe {
                layout_mut
                    .row_guard
                    .remap(new_rows_size, memmap2::RemapOptions::new().may_move(true))
            }
            .map_err(Error::IOError)?;

            #[cfg(not(target_os = "linux"))]
            {
                *layout_mut.row_guard = unsafe {
                    memmap2::MmapOptions::new()
                        .len(new_rows_size)
                        .map_mut(&self.rows_file)
                }
                .map_err(Error::IOError)?;
            }

            Self::maybe_lock_mmap(self.config.as_ref(), &layout_mut.row_guard);
            remap_dur = Some(remap_start.elapsed());
        }

        self.header_ref()
            .global_split_level
            .store(nsl, Ordering::Release);
        Ok(remap_dur)
    }

    pub(crate) fn num_rows(&self) -> usize {
        let gsl = self.header_ref().global_split_level.load(Ordering::Acquire) as usize;
        1usize << gsl
    }

    pub(crate) fn num_shards(&self) -> usize {
        self.row_locks.len()
    }

    pub(crate) fn shrink_with_rows_guard(
        &self,
        min_rows: usize,
        mut row_table: RowsTableWriteGuard<'_>,
    ) -> Result<usize> {
        loop {
            let global_split_level = self.header_ref().global_split_level.load(Ordering::Acquire);
            let current_rows = 1usize << global_split_level;
            if current_rows <= min_rows {
                break;
            }

            let next_level = global_split_level - 1;
            let half_count = 1usize << next_level;

            let mut can_merge = true;
            for idx in 0..half_count {
                let row1 = row_ref_bytes(&row_table.row_guard[..], idx);
                let row1_split = row1.split_level.load(Ordering::Acquire);
                if row1_split != global_split_level {
                    continue;
                }

                let row2 = row_ref_bytes(&row_table.row_guard[..], idx + half_count);
                let count1 = row1
                    .signatures
                    .iter()
                    .filter(|&&sig| sig != HashCoord::INVALID_SIG)
                    .count();
                let count2 = row2
                    .signatures
                    .iter()
                    .filter(|&&sig| sig != HashCoord::INVALID_SIG)
                    .count();
                if count1 + count2 > ROW_WIDTH {
                    can_merge = false;
                    break;
                }
            }

            if !can_merge {
                break;
            }

            for idx in 0..half_count {
                let row1 = unsafe { &mut *row_mut_ptr(row_table.row_guard.as_ptr(), idx) };
                let row2 =
                    unsafe { &mut *row_mut_ptr(row_table.row_guard.as_ptr(), idx + half_count) };

                if row1.split_level.load(Ordering::Acquire) != global_split_level {
                    continue;
                }

                let mut dest_idx = 0usize;
                for src_idx in 0..ROW_WIDTH {
                    if row2.signatures[src_idx] == HashCoord::INVALID_SIG {
                        continue;
                    }

                    while dest_idx < ROW_WIDTH
                        && row1.signatures[dest_idx] != HashCoord::INVALID_SIG
                    {
                        dest_idx += 1;
                    }

                    if dest_idx >= ROW_WIDTH {
                        break;
                    }

                    row1.insert(dest_idx, row2.signatures[src_idx], row2.pointers[src_idx]);
                    row2.remove(src_idx);
                }

                row2.set_split_level(0);
                row1.set_split_level(next_level);
            }

            self.header_ref()
                .global_split_level
                .store(next_level, Ordering::Release);
        }

        let final_level = self.header_ref().global_split_level.load(Ordering::Acquire);
        let new_rows_size = (1usize << final_level) * size_of::<RowLayout>();

        if new_rows_size < row_table.row_guard.len() {
            #[cfg(target_os = "linux")]
            {
                unsafe {
                    row_table
                        .row_guard
                        .remap(new_rows_size, memmap2::RemapOptions::new().may_move(true))
                }
                .map_err(Error::IOError)?;
                self.rows_file
                    .set_len(new_rows_size as u64)
                    .map_err(Error::IOError)?;
            }

            #[cfg(not(target_os = "linux"))]
            {
                row_table.row_guard.flush().map_err(Error::IOError)?;

                #[cfg(windows)]
                {
                    // On Windows we must unmap before truncating.
                    *row_table.row_guard = memmap2::MmapOptions::new()
                        .len(1)
                        .map_anon()
                        .map_err(Error::IOError)?;
                }

                self.rows_file
                    .set_len(new_rows_size as u64)
                    .map_err(Error::IOError)?;
                *row_table.row_guard = unsafe {
                    memmap2::MmapOptions::new()
                        .len(new_rows_size)
                        .map_mut(&self.rows_file)
                }
                .map_err(Error::IOError)?;
            }

            Self::maybe_lock_mmap(self.config.as_ref(), &row_table.row_guard);
        }

        Ok(1usize << final_level)
    }

    fn init_header_and_rows(
        &self,
        mut rows_table: RowsTableWriteGuard,
        hash_key: (u64, u64),
    ) -> Result<()> {
        // Zero both mmaps first, then populate.
        rows_table.row_guard.fill(0);
        // Safety: header_mmap is a contiguous MmapMut that we own; no other &mut exists yet.
        unsafe {
            std::ptr::write_bytes(
                self.header_mmap.as_ptr() as *mut u8,
                0,
                self.header_mmap.len(),
            );
        }

        // Now create the mutable reference after zeroing is complete.
        // Safety: only called during init (open) or reset, both single-threaded
        // w.r.t. this store instance.
        let layout = unsafe { &mut *(self.header_mmap.as_ptr() as *mut IndexFileLayout) };

        layout.header.signature = *INDEX_FILE_SIGNATURE;
        layout.header.version = INDEX_FILE_VERSION;
        layout.header.hash_key_0 = hash_key.0;
        layout.header.hash_key_1 = hash_key.1;
        layout
            .header
            .global_split_level
            .store(MIN_SPLIT_LEVEL as u64, Ordering::Release);

        for row_idx in 0..MIN_INITIAL_ROWS {
            rows_table
                .row_mut(row_idx)
                .set_split_level(MIN_SPLIT_LEVEL as u64);
        }

        self.flush_header()?;
        rows_table.row_guard.flush().map_err(Error::IOError)?;
        self.rows_file.sync_all().map_err(Error::IOError)?;
        Ok(())
    }

    pub(crate) fn reset(&self, mut row_table: RowsTableWriteGuard<'_>) -> Result<()> {
        let min_rows_size = MIN_INITIAL_ROWS * size_of::<RowLayout>();

        #[cfg(target_os = "linux")]
        unsafe {
            self.rows_file
                .set_len(min_rows_size as u64)
                .map_err(Error::IOError)?;
            row_table
                .row_guard
                .remap(min_rows_size, memmap2::RemapOptions::new().may_move(true))
        }
        .map_err(Error::IOError)?;

        #[cfg(not(target_os = "linux"))]
        {
            row_table.row_guard.flush().map_err(Error::IOError)?;

            #[cfg(windows)]
            {
                *row_table.row_guard = memmap2::MmapOptions::new()
                    .len(1)
                    .map_anon()
                    .map_err(Error::IOError)?;
            }

            self.rows_file
                .set_len(min_rows_size as u64)
                .map_err(Error::IOError)?;
            *row_table.row_guard = unsafe {
                memmap2::MmapOptions::new()
                    .len(min_rows_size)
                    .map_mut(&self.rows_file)
            }
            .map_err(Error::IOError)?;
        }

        Self::maybe_lock_mmap(self.config.as_ref(), &row_table.row_guard);

        self.init_header_and_rows(row_table, self.config.hash_key)
    }
}
