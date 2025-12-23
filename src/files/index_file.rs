use crate::types::{
    CandyError, Config, EntryPointer, HashCoordinates, INDEX_FILE_MAGIC, INDEX_FILE_VERSION,
    PAGE_SIZE, ROW_WIDTH, Result,
};
use memmap2::MmapMut;
use parking_lot::RwLock;
use simd_itertools::PositionSimd;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
pub(crate) struct RowLayout {
    pub split_level: AtomicU64,
    pub padding: [u8; 120],
    pub signatures: [u32; ROW_WIDTH],
    pub pointers: [EntryPointer; ROW_WIDTH],
}

const _: () = assert!(std::mem::size_of::<RowLayout>() == 2 * PAGE_SIZE);

impl RowLayout {
    pub fn iter_matches(&self, hash_coord: HashCoordinates) -> RowMatchIterator<'_> {
        RowMatchIterator {
            row: self,
            hash_coord,
            offset: 0,
        }
    }
}

pub(crate) struct RowMatchIterator<'a> {
    row: &'a RowLayout,
    hash_coord: HashCoordinates,
    offset: usize,
}

impl<'a> Iterator for RowMatchIterator<'a> {
    type Item = (usize, EntryPointer);

    fn next(&mut self) -> Option<Self::Item> {
        while self.offset < ROW_WIDTH {
            if let Some(idx) = self.row.signatures[self.offset..]
                .iter()
                .position_simd(|&sig| sig == self.hash_coord.signature)
            {
                let real_idx = self.offset + idx;
                self.offset = real_idx + 1;
                let ptr = self.row.pointers[real_idx];
                if ptr.is_valid() && ptr.row_selector() == self.hash_coord.row_selector {
                    return Some((real_idx, ptr));
                }
            } else {
                self.offset = ROW_WIDTH;
                break;
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct IndexFileStats {
    pub num_inserts: u64,
    pub num_updates: u64,
    pub num_deletes: u64,
    pub num_compacted_files: u64,
    pub entries_under_128: u64,
    pub entries_under_1k: u64,
    pub entries_under_8k: u64,
    pub entries_under_32k: u64,
    pub entries_over_32k: u64,
}

#[repr(C)]
pub(crate) struct IndexFileHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub _padding1: [u8; 52],
    // 64
    pub global_split_level: AtomicU64,
    _padding2: [u8; 56],
    // 128
    pub num_inserts: AtomicU64,
    pub num_deletes: AtomicU64,
    pub num_updates: AtomicU64,
    pub index_checksum: AtomicU64,
    pub _last_write_pos: AtomicU64,
    _padding3: [u8; 24],
    // 192
    pub num_compacted_files: AtomicU64,
    pub entries_under_128: AtomicU64,
    pub entries_under_1k: AtomicU64,
    pub entries_under_8k: AtomicU64,
    pub entries_under_32k: AtomicU64,
    pub entries_over_32k: AtomicU64,
    _padding4: [u8; 3854],
}

const _: () = assert!(std::mem::size_of::<IndexFileHeader>() == PAGE_SIZE);

#[repr(C)]
struct IndexFileLayout {
    header: IndexFileHeader,
    rows: [RowLayout; 0],
}

const _: () = assert!(std::mem::size_of::<IndexFileLayout>() == PAGE_SIZE);

impl IndexFileHeader {
    #[inline]
    pub fn record_entry_size(&self, entry_size: usize) {
        let counter = if entry_size < 128 {
            &self.entries_under_128
        } else if entry_size < 1024 {
            &self.entries_under_1k
        } else if entry_size < 8 * 1024 {
            &self.entries_under_8k
        } else if entry_size < 32 * 1024 {
            &self.entries_under_32k
        } else {
            &self.entries_over_32k
        };

        counter.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReadGuardLayout<'a>(parking_lot::RwLockReadGuard<'a, MmapMut>);

impl<'a> ReadGuardLayout<'a> {
    #[inline]
    fn layout(&self) -> &IndexFileLayout {
        unsafe { &*(self.0.as_ptr() as *const IndexFileLayout) }
    }
    #[inline]
    fn header(&self) -> &IndexFileHeader {
        &self.layout().header
    }
    #[inline]
    fn row(&self, row_index: usize) -> &RowLayout {
        debug_assert!(
            std::mem::size_of::<IndexFileHeader>() + row_index * std::mem::size_of::<RowLayout>()
                < self.0.len(),
            "{row_index} out of bounds",
        );

        unsafe { &*self.layout().rows.as_ptr().add(row_index) }
    }
    /// # Safety
    /// Caller must ensure that no other mutable reference to this row exists.
    /// This usually means holding the corresponding row lock.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn row_mut(&self, row_index: usize) -> &mut RowLayout {
        debug_assert!(
            std::mem::size_of::<IndexFileHeader>() + row_index * std::mem::size_of::<RowLayout>()
                < self.0.len(),
            "{row_index} out of bounds",
        );

        unsafe { &mut *(self.layout().rows.as_ptr().add(row_index) as *mut RowLayout) }
    }
}

struct WriteGuardLayout<'a>(parking_lot::RwLockWriteGuard<'a, MmapMut>);

impl<'a> WriteGuardLayout<'a> {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn layout(&self) -> &mut IndexFileLayout {
        unsafe { &mut *(self.0.as_ptr() as *mut IndexFileLayout) }
    }
    #[inline]
    fn header(&self) -> &mut IndexFileHeader {
        &mut self.layout().header
    }
    #[inline]
    fn row(&self, row_index: usize) -> &RowLayout {
        debug_assert!(
            std::mem::size_of::<IndexFileHeader>() + row_index * std::mem::size_of::<RowLayout>()
                < self.0.len(),
            "{row_index} out of bounds",
        );
        unsafe { &*self.layout().rows.as_ptr().add(row_index) }
    }
    /// # Safety
    /// Caller must ensure that the returned reference does not alias with any other
    /// active references to the same row.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn row_mut(&self, row_index: usize) -> &mut RowLayout {
        debug_assert!(
            std::mem::size_of::<IndexFileHeader>() + row_index * std::mem::size_of::<RowLayout>()
                < self.0.len(),
            "{row_index} out of bounds",
        );
        unsafe { &mut *self.layout().rows.as_mut_ptr().add(row_index) }
    }
}

pub(crate) struct IndexFile {
    pub file: File,
    pub mmap: RwLock<MmapMut>,
    pub remapping_scaler: u8,
    pub row_locks: Vec<RwLock<()>>,
}

fn initial_split_level_for_config(config: &Config) -> u64 {
    let target_rows = config.initial_capacity.div_ceil(ROW_WIDTH);
    let mut split_level = target_rows.next_power_of_two().ilog2() as u64;

    while (1 << split_level) < 16 || (1 << split_level) < config.max_concurrency {
        split_level += 1;
    }

    split_level
}

impl IndexFile {
    pub fn flush(&self) -> Result<()> {
        self.mmap.read().flush().map_err(CandyError::IOError)
    }

    unsafe fn mmap_file<'a>(file: &File) -> Result<(MmapMut, &'a mut IndexFileLayout)> {
        let mut mmap = unsafe { MmapMut::map_mut(file).map_err(CandyError::IOError)? };
        let layout = unsafe { &mut *(mmap.as_mut_ptr() as *mut IndexFileLayout) };

        Ok((mmap, layout))
    }

    pub fn open(path: &Path, config: &Config) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(CandyError::IOError)?;

        let len = file.metadata().map_err(CandyError::IOError)?.len();

        if len == 0 {
            let mmap = Self::initialize_index_file(&file, config)?;

            return Ok(Self {
                file,
                mmap: RwLock::new(mmap),
                remapping_scaler: config.remapping_scaler,
                row_locks: (0..config.max_concurrency)
                    .map(|_| RwLock::new(()))
                    .collect(),
            });
        }

        let (mmap, layout) = unsafe { Self::mmap_file(&file)? };

        if layout.header.magic != *INDEX_FILE_MAGIC {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid index file magic",
            )));
        }
        if layout.header.version != INDEX_FILE_VERSION {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid index file version",
            )));
        }

        let global_split_level = layout.header.global_split_level.load(Ordering::Relaxed);
        let expected_len = (std::mem::size_of::<IndexFileHeader>()
            + (1 << global_split_level) * std::mem::size_of::<RowLayout>())
            as u64;

        if (mmap.len() as u64) < expected_len {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "index file too short for split level {}: got {} expected {}",
                    global_split_level,
                    mmap.len(),
                    expected_len
                ),
            )));
        }

        // Verify checksum
        let mut computed_checksum = 0u64;
        let num_rows = 1 << global_split_level;
        for i in 0..num_rows {
            let row = unsafe { &*layout.rows.as_ptr().add(i) };
            for j in 0..ROW_WIDTH {
                let sig = row.signatures[j];
                let ptr = row.pointers[j];
                if sig != HashCoordinates::INVALID_SIG && ptr.is_data_pointer() {
                    computed_checksum ^= ptr.calc_checksum(sig);
                }
            }
        }

        let stored_checksum = layout.header.index_checksum.load(Ordering::Relaxed);
        if computed_checksum != stored_checksum {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "index checksum mismatch, expected 0x{:x} got 0x{:x}",
                    stored_checksum, computed_checksum
                ),
            )));
        }

        Ok(Self {
            file,
            mmap: RwLock::new(mmap),
            remapping_scaler: config.remapping_scaler,
            row_locks: (0..config.max_concurrency)
                .map(|_| RwLock::new(()))
                .collect(),
        })
    }

    pub fn file_size_bytes(&self) -> u64 {
        self.file.metadata().map_or(0, |m| m.len())
    }

    fn initialize_index_file(file: &File, config: &Config) -> Result<MmapMut> {
        let initial_split_level = initial_split_level_for_config(config);
        let initial_rows = (1u64 << initial_split_level) as usize;
        assert!(initial_rows.is_power_of_two());

        let expected_len = (std::mem::size_of::<IndexFileHeader>()
            + initial_rows * std::mem::size_of::<RowLayout>()) as u64;

        file.set_len(expected_len).map_err(CandyError::IOError)?;
        let (mmap, layout) = unsafe { Self::mmap_file(file)? };

        layout.header.magic = *INDEX_FILE_MAGIC;
        layout.header.version = INDEX_FILE_VERSION;
        layout
            .header
            .global_split_level
            .store(initial_split_level, Ordering::Relaxed);
        layout.header.index_checksum.store(0, Ordering::Relaxed);
        layout.header._last_write_pos.store(0, Ordering::Relaxed);
        layout
            .header
            .num_compacted_files
            .store(0, Ordering::Relaxed);
        layout.header.num_inserts.store(0, Ordering::Relaxed);
        layout.header.num_deletes.store(0, Ordering::Relaxed);
        layout.header.num_updates.store(0, Ordering::Relaxed);
        layout.header.entries_under_128.store(0, Ordering::Relaxed);
        layout.header.entries_under_1k.store(0, Ordering::Relaxed);
        layout.header.entries_under_8k.store(0, Ordering::Relaxed);
        layout.header.entries_under_32k.store(0, Ordering::Relaxed);
        layout.header.entries_over_32k.store(0, Ordering::Relaxed);

        for i in 0..initial_rows {
            let row = unsafe { &mut *layout.rows.as_mut_ptr().add(i) };
            // Zero out the row
            unsafe { std::ptr::write_bytes(row, 0, 1) };
            // Set split level
            row.split_level
                .store(initial_split_level, Ordering::Relaxed);
        }

        mmap.flush().map_err(CandyError::IOError)?;

        #[cfg(target_os = "linux")]
        {
            if config.mlock_index {
                _ = mmap.lock();
            }
        }

        Ok(mmap)
    }

    pub(crate) fn reset(&self, config: &Config) -> Result<()> {
        let mmap = Self::initialize_index_file(&self.file, config)?;
        *self.mmap.write() = mmap;
        Ok(())
    }

    #[inline]
    fn read_guard_layout(&self) -> ReadGuardLayout<'_> {
        ReadGuardLayout(self.mmap.read())
    }

    pub fn operate_on_row<R>(
        &self,
        hash_coord: HashCoordinates,
        mut f: impl FnMut(&RowLayout) -> Result<R>,
    ) -> Result<R> {
        let layout = self.read_guard_layout();
        let global_split_level = layout.header().global_split_level.load(Ordering::Relaxed) as u32;
        let mut curr_split_level = global_split_level;

        loop {
            let row_index = hash_coord.row_selector & ((1 << curr_split_level) - 1);

            // Acquire row lock
            let lock_index = (row_index as usize) & (self.row_locks.len() - 1);
            let row_guard = self.row_locks[lock_index].read();

            let row = layout.row(row_index as usize);
            let row_split = row.split_level.load(Ordering::Relaxed);

            if row_split == 0 {
                assert!(
                    curr_split_level > 0,
                    "global_split_level={global_split_level} row_index={row_index} curr_split_level={curr_split_level}"
                );
                drop(row_guard);
                curr_split_level -= 1;
                continue;
            }

            if row_split > curr_split_level as u64 {
                drop(row_guard);
                curr_split_level = row_split as u32;
                continue;
            }

            return f(row);
        }
    }

    fn grow_file(&self, new_row_split: u64) -> Result<()> {
        let required_len = (std::mem::size_of::<IndexFileHeader>()
            + (1 << new_row_split) * std::mem::size_of::<RowLayout>())
            as u64;

        let mut write_guard = self.mmap.write();
        if (write_guard.len() as u64) < required_len {
            let alloc_split = new_row_split + self.remapping_scaler as u64;
            let new_len = (std::mem::size_of::<IndexFileHeader>()
                + (1 << alloc_split) * std::mem::size_of::<RowLayout>())
                as u64;

            self.file.set_len(new_len).map_err(CandyError::IOError)?;

            #[cfg(target_os = "linux")]
            unsafe {
                write_guard.remap(
                    new_len as usize,
                    memmap2::RemapOptions::new().may_move(true),
                )
            }
            .map_err(CandyError::IOError)?;

            #[cfg(not(target_os = "linux"))]
            unsafe {
                *write_guard = memmap2::MmapOptions::new()
                    .len(new_len as usize)
                    .map_mut(&self.file)
                    .map_err(CandyError::IOError)?;
            }
        }
        Ok(())
    }

    fn split_row_data(
        &self,
        layout: &ReadGuardLayout,
        row1_index: usize,
        row1_split: u64,
        new_row_split: u64,
    ) {
        layout
            .header()
            .global_split_level
            .fetch_max(new_row_split, Ordering::Relaxed);

        let row2_index = row1_index | (1 << row1_split);

        // this is safe because we have the write lock on the row
        let row1 = unsafe { layout.row_mut(row1_index) };
        let row2 = unsafe { layout.row_mut(row2_index) };

        debug_assert_eq!(row2.split_level.load(Ordering::Relaxed), 0);

        let mut j = 0;
        for i in 0..ROW_WIDTH {
            let selector = row1.pointers[i].row_selector();
            if (selector & (1 << row1_split)) != 0 {
                row2.signatures[j] = row1.signatures[i];
                row2.pointers[j] = row1.pointers[i];
                row1.signatures[i] = HashCoordinates::INVALID_SIG;
                row1.pointers[i] = EntryPointer::INVALID_PTR;
                j += 1;
            }
        }

        row1.split_level.store(new_row_split, Ordering::Relaxed);
        row2.split_level.store(new_row_split, Ordering::Relaxed);
    }

    pub fn operate_on_row_mut<R>(
        &self,
        hash_coord: HashCoordinates,
        mut f: impl FnMut(&mut RowLayout, &IndexFileHeader) -> Result<R>,
    ) -> Result<R> {
        // Start with read lock on mmap
        let mut layout = self.read_guard_layout();
        let mut global_split_level = layout
            .header()
            .global_split_level
            .load(std::sync::atomic::Ordering::Relaxed) as u32;
        let mut curr_split_level = global_split_level;

        loop {
            let row1_index = (hash_coord.row_selector & ((1 << curr_split_level) - 1)) as usize;

            // Acquire row lock
            let lock_index = row1_index & (self.row_locks.len() - 1);
            let row_guard = self.row_locks[lock_index].write();

            // this is safe because we have the row lock
            let row = unsafe { layout.row_mut(row1_index) };

            let row1_split = row.split_level.load(Ordering::Relaxed);

            if row1_split == 0 {
                // This row is not initialized yet, meaning it's covered by a parent row.
                // We must look at the parent.
                assert!(
                    curr_split_level > 0,
                    "global_split_level={global_split_level} row_index={row1_index} curr_split_level={curr_split_level}"
                );
                drop(row_guard);
                curr_split_level -= 1;
                continue;
            }

            if row1_split > curr_split_level as u64 {
                drop(row_guard);
                curr_split_level = row1_split as u32;
                continue;
            }

            let res = f(row, layout.header());

            match res {
                Err(CandyError::SplitRow) => {
                    let new_row_split = row1_split + 1;

                    // Check if we need to grow the file
                    let required_len = (std::mem::size_of::<IndexFileHeader>()
                        + (1 << new_row_split) * std::mem::size_of::<RowLayout>())
                        as u64;

                    if (layout.0.len() as u64) < required_len {
                        // We need to grow. Drop locks and acquire write lock on mmap.
                        drop(row_guard);
                        drop(layout);

                        self.grow_file(new_row_split)?;

                        // Restart loop
                        layout = self.read_guard_layout();
                        global_split_level = layout
                            .header()
                            .global_split_level
                            .load(std::sync::atomic::Ordering::Relaxed)
                            as u32;
                        curr_split_level = global_split_level;
                        continue;
                    }

                    self.split_row_data(&layout, row1_index, row1_split, new_row_split);

                    drop(row_guard);
                    curr_split_level = new_row_split as u32;
                }
                res => return res,
            }
        }
    }

    pub fn stats(&self) -> IndexFileStats {
        let layout = self.read_guard_layout();
        let header = layout.header();
        IndexFileStats {
            num_inserts: header.num_inserts.load(Ordering::Relaxed),
            num_updates: header.num_updates.load(Ordering::Relaxed),
            num_deletes: header.num_deletes.load(Ordering::Relaxed),
            num_compacted_files: header.num_compacted_files.load(Ordering::Relaxed),
            entries_under_128: header.entries_under_128.load(Ordering::Relaxed),
            entries_under_1k: header.entries_under_1k.load(Ordering::Relaxed),
            entries_under_8k: header.entries_under_8k.load(Ordering::Relaxed),
            entries_under_32k: header.entries_under_32k.load(Ordering::Relaxed),
            entries_over_32k: header.entries_over_32k.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn record_compacted_file(&self) {
        self.read_guard_layout()
            .header()
            .num_compacted_files
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn num_rows(&self) -> usize {
        let global_split_level = self
            .read_guard_layout()
            .header()
            .global_split_level
            .load(Ordering::Relaxed);
        1 << global_split_level
    }

    pub fn get_entry(&self, row_idx: usize, col_idx: usize) -> Option<EntryPointer> {
        let layout = self.read_guard_layout();

        let global_split_level = layout.header().global_split_level.load(Ordering::Relaxed);
        if row_idx >= (1 << global_split_level) {
            return None;
        }

        let lock_index = row_idx & (self.row_locks.len() - 1);
        let _guard = self.row_locks[lock_index].read();

        let row = layout.row(row_idx);
        if row.signatures[col_idx] != HashCoordinates::INVALID_SIG {
            Some(row.pointers[col_idx])
        } else {
            None
        }
    }

    pub fn shrink(&self, min_rows: usize) -> Result<usize> {
        let mut layout = WriteGuardLayout(self.mmap.write());

        loop {
            let global_split_level = layout.header().global_split_level.load(Ordering::Relaxed);
            if (1 << global_split_level) <= min_rows {
                break;
            }

            let current_level = global_split_level;
            let next_level = current_level - 1;
            let half_count = 1 << next_level;

            // Check if we can merge
            let mut can_merge = true;
            for i in 0..half_count {
                let row1 = layout.row(i);
                let row1_split = row1.split_level.load(Ordering::Relaxed);

                if row1_split == current_level {
                    let row2 = layout.row(i + half_count);
                    // Count items
                    let c1 = row1
                        .signatures
                        .iter()
                        .filter(|&&s| s != HashCoordinates::INVALID_SIG)
                        .count();
                    let c2 = row2
                        .signatures
                        .iter()
                        .filter(|&&s| s != HashCoordinates::INVALID_SIG)
                        .count();
                    if c1 + c2 > ROW_WIDTH {
                        can_merge = false;
                        break;
                    }
                }
            }

            if !can_merge {
                break;
            }

            // Perform merge
            for i in 0..half_count {
                let row1 = unsafe { layout.row_mut(i) };
                let row2 = unsafe { layout.row_mut(i + half_count) };

                let row1_split = row1.split_level.load(Ordering::Relaxed);
                if row1_split == current_level {
                    // Merge row2 into row1
                    let mut dest_idx = 0;

                    for src_idx in 0..ROW_WIDTH {
                        if row2.signatures[src_idx] != HashCoordinates::INVALID_SIG {
                            // Find next empty slot
                            while dest_idx < ROW_WIDTH
                                && row1.signatures[dest_idx] != HashCoordinates::INVALID_SIG
                            {
                                dest_idx += 1;
                            }

                            if dest_idx < ROW_WIDTH {
                                row1.signatures[dest_idx] = row2.signatures[src_idx];
                                row1.pointers[dest_idx] = row2.pointers[src_idx];
                            }
                        }
                    }

                    // Clear row2
                    row2.split_level.store(0, Ordering::Relaxed);
                    row2.signatures.fill(HashCoordinates::INVALID_SIG);
                    row2.pointers.fill(EntryPointer::INVALID_PTR);

                    row1.split_level.store(next_level, Ordering::Relaxed);
                }
            }

            layout
                .header()
                .global_split_level
                .store(next_level, Ordering::Relaxed);
        }

        let final_level = layout.header().global_split_level.load(Ordering::Relaxed);
        let new_len = (std::mem::size_of::<IndexFileHeader>()
            + (1 << final_level) * std::mem::size_of::<RowLayout>()) as u64;

        if new_len < layout.0.len() as u64 {
            #[cfg(target_os = "linux")]
            {
                unsafe {
                    layout.0.remap(
                        new_len as usize,
                        memmap2::RemapOptions::new().may_move(true),
                    )
                }
                .map_err(CandyError::IOError)?;
                self.file.set_len(new_len).map_err(CandyError::IOError)?;
            }

            #[cfg(not(target_os = "linux"))]
            {
                layout.0.flush().map_err(CandyError::IOError)?;

                // On Windows, we cannot truncate the file while it is mapped.
                // We replace the mapping with a dummy anonymous mapping to drop the file mapping.
                #[cfg(windows)]
                {
                    let dummy = memmap2::MmapOptions::new()
                        .len(1)
                        .map_anon()
                        .map_err(CandyError::IOError)?;
                    *layout.0 = dummy;
                }

                self.file.set_len(new_len).map_err(CandyError::IOError)?;

                unsafe {
                    *layout.0 = memmap2::MmapOptions::new()
                        .len(new_len as usize)
                        .map_mut(&self.file)
                        .map_err(CandyError::IOError)?;
                }
            }
        }

        Ok(1 << final_level)
    }
}

#[doc(hidden)]
pub mod test_offsets {
    use super::*;

    pub const LAST_WRITE_POS: usize = std::mem::offset_of!(IndexFileHeader, _last_write_pos);
    pub const INDEX_CHECKSUM: usize = std::mem::offset_of!(IndexFileHeader, index_checksum);
}
