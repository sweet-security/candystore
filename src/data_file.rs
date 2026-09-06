use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use std::{
    collections::VecDeque,
    fs::File,
    mem::size_of,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::internal::{
    DATA_ENTRY_OFFSET_MASK, DATA_FILE_SIGNATURE, DATA_FILE_VERSION, EntryType,
    FILE_OFFSET_ALIGNMENT, KEY_NAMESPACE_BITS, KVBuf, KVRef, KeyNamespace, MAX_KEY_NAMESPACE,
    PAGE_SIZE, READ_BUFFER_SIZE, SIZE_HINT_UNIT, data_file_path, entry_magic_offset,
    invalid_data_error, read_available_at, read_into_at, sync_dir, write_all_at,
};
use crate::types::{Config, Error, MAX_USER_KEY_SIZE, MAX_USER_VALUE_SIZE, Result};

const INLINE_SCRATCH_BUFFER_SIZE: usize = 1024;

struct ParsedDataEntry {
    data_len: usize,
    vlen: u16,
    ns: u8,
}

#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct DataFileHeader {
    magic: [u8; 8],
    version: u32,
    _padding0: u32,
    ordinal: u64,
    _trailer: [u8; 4096 - 24],
}

const _: () = assert!(size_of::<DataFileHeader>() == PAGE_SIZE);

struct InflightSlot {
    seq: AtomicU64,
    ordinal: AtomicU64,
    offset: AtomicU64,
}

pub(crate) struct InflightTracker {
    snapshot_barrier: RwLock<()>,
    next_seq: AtomicU64,
    slots: Vec<InflightSlot>,
    completed_deltas: Vec<Mutex<VecDeque<(u64, i64)>>>,
}

impl InflightTracker {
    pub(crate) fn new(num_shards: usize) -> Self {
        Self {
            snapshot_barrier: RwLock::new(()),
            next_seq: AtomicU64::new(1),
            slots: (0..num_shards)
                .map(|_| InflightSlot {
                    seq: AtomicU64::new(0),
                    ordinal: AtomicU64::new(0),
                    offset: AtomicU64::new(0),
                })
                .collect(),
            completed_deltas: (0..num_shards)
                .map(|_| Mutex::new(VecDeque::new()))
                .collect(),
        }
    }

    fn reserve<'a>(
        &'a self,
        data_file: &DataFile,
        shard_idx: usize,
        len: u64,
        delta: i8,
    ) -> Result<(u64, InflightGuard<'a>)> {
        let _barrier = self.snapshot_barrier.read();
        let offset = data_file.allocate(len)?;
        let ordinal = data_file.file_ordinal;
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let slot = &self.slots[shard_idx];
        slot.ordinal.store(ordinal, Ordering::Relaxed);
        slot.offset.store(offset, Ordering::Relaxed);
        slot.seq.store(seq, Ordering::Release);

        Ok((
            offset,
            InflightGuard {
                tracker: self,
                shard_idx,
                seq,
                delta,
                armed: true,
            },
        ))
    }

    pub(crate) fn checkpoint_progress(&self, active_file: &DataFile) -> (u64, u64, i64, u64) {
        let _barrier = self.snapshot_barrier.write();

        let mut checkpoint = None::<(u64, u64)>;
        let mut earliest_active_seq = None::<u64>;
        for slot in &self.slots {
            let seq = slot.seq.load(Ordering::Acquire);
            if seq == 0 {
                continue;
            }
            let ordinal = slot.ordinal.load(Ordering::Relaxed);
            let offset = slot.offset.load(Ordering::Relaxed);
            let position = (ordinal, offset);
            checkpoint = Some(checkpoint.map_or(position, |current| current.min(position)));
            earliest_active_seq = Some(earliest_active_seq.map_or(seq, |current| current.min(seq)));
        }

        let checkpoint =
            checkpoint.unwrap_or_else(|| (active_file.file_ordinal, active_file.used_bytes()));
        let completed_before_seq = earliest_active_seq.unwrap_or(u64::MAX);
        let mut committed_delta = 0i64;
        for queue in &self.completed_deltas {
            let queue = queue.lock();
            for &(seq, delta) in queue.iter() {
                if seq >= completed_before_seq {
                    break;
                }
                committed_delta += delta;
            }
        }

        (
            checkpoint.0,
            checkpoint.1,
            committed_delta,
            completed_before_seq,
        )
    }

    pub(crate) fn drain_completed_before(&self, completed_before_seq: u64) -> i64 {
        let _barrier = self.snapshot_barrier.write();
        let mut committed_delta = 0i64;
        for queue in &self.completed_deltas {
            let mut queue = queue.lock();
            while let Some(&(seq, delta)) = queue.front() {
                if seq >= completed_before_seq {
                    break;
                }
                queue.pop_front();
                committed_delta += delta;
            }
        }
        committed_delta
    }

    pub(crate) fn clear_all(&self) {
        let _barrier = self.snapshot_barrier.write();
        for slot in &self.slots {
            slot.seq.store(0, Ordering::Release);
            slot.ordinal.store(0, Ordering::Relaxed);
            slot.offset.store(0, Ordering::Relaxed);
        }
        for queue in &self.completed_deltas {
            queue.lock().clear();
        }
    }

    fn clear_matching(&self, shard_idx: usize, expected_seq: u64) {
        let _barrier = self.snapshot_barrier.read();
        let slot = &self.slots[shard_idx];
        if slot.seq.load(Ordering::Acquire) == expected_seq {
            slot.seq.store(0, Ordering::Release);
        }
    }

    fn complete_matching(&self, shard_idx: usize, expected_seq: u64, delta: i8) {
        let _barrier = self.snapshot_barrier.read();
        let slot = &self.slots[shard_idx];
        if slot.seq.load(Ordering::Acquire) == expected_seq {
            slot.seq.store(0, Ordering::Release);
            if delta != 0 {
                self.completed_deltas[shard_idx]
                    .lock()
                    .push_back((expected_seq, i64::from(delta)));
            }
        }
    }
}

pub(crate) struct InflightGuard<'a> {
    tracker: &'a InflightTracker,
    shard_idx: usize,
    seq: u64,
    delta: i8,
    armed: bool,
}

impl InflightGuard<'_> {
    pub(crate) fn complete(mut self) {
        if self.armed {
            self.tracker
                .complete_matching(self.shard_idx, self.seq, self.delta);
            self.armed = false;
        }
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.tracker.clear_matching(self.shard_idx, self.seq);
        }
    }
}

pub(crate) struct DataFile {
    pub(crate) file: File,
    file_offset: AtomicU64,
    dirty: AtomicBool,
    sync_lock: Mutex<()>,
    sealed_for_rotation: AtomicBool,
    config: Arc<Config>,
    pub(crate) file_idx: u16,
    pub(crate) file_ordinal: u64,
    preallocated: bool,
    recovery_tail_upper_bound: u64,
}

impl DataFile {
    fn read_header(file: &File) -> Result<DataFileHeader> {
        let header =
            read_available_at(file, size_of::<DataFileHeader>(), 0).map_err(Error::IOError)?;
        if header.len() < size_of::<DataFileHeader>() {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "data file header too short",
            )));
        }

        let header = DataFileHeader::read_from_bytes(&header).map_err(|_| {
            Error::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid data file header size",
            ))
        })?;
        if &header.magic != DATA_FILE_SIGNATURE || header.version != DATA_FILE_VERSION {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid data file header",
            )));
        }

        Ok(header)
    }

    pub(crate) fn read_ordinal(base_path: &Path, file_idx: u16) -> Result<u64> {
        let file = File::options()
            .read(true)
            .open(data_file_path(base_path, file_idx))
            .map_err(Error::IOError)?;
        Ok(Self::read_header(&file)?.ordinal)
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.file_offset.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub(crate) fn recovery_tail_upper_bound(&self) -> u64 {
        self.recovery_tail_upper_bound
    }

    /// Syncs if any write completed since the last successful sync. The flag is
    /// cleared before the fdatasync so a write landing concurrently re-dirties it.
    pub(crate) fn sync_to_current(&self) -> Result<()> {
        self.sync_to_current_with(|file| file.sync_data())
    }

    fn sync_to_current_with(&self, sync: impl FnOnce(&File) -> std::io::Result<()>) -> Result<()> {
        let _sync_guard = self.sync_lock.lock();
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        if let Err(err) = sync(&self.file) {
            self.dirty.store(true, Ordering::Release);
            return Err(Error::IOError(err));
        }
        Ok(())
    }

    pub(crate) fn truncate_to_offset(&self, file_offset: u64) -> Result<()> {
        debug_assert_eq!(file_offset % FILE_OFFSET_ALIGNMENT, 0);
        if self.preallocated {
            // A crash between the two set_len calls would leave the file
            // non-preallocated.  That is harmless: the next open will
            // detect it as non-preallocated and fall back to sync_all
            // until rotation creates a fresh preallocated file.
            self.file
                .set_len(size_of::<DataFileHeader>() as u64 + file_offset)
                .map_err(Error::IOError)?;
            self.file
                .set_len(size_of::<DataFileHeader>() as u64 + self.config.max_data_file_size as u64)
                .map_err(Error::IOError)?;
        } else {
            self.file
                .set_len(size_of::<DataFileHeader>() as u64 + file_offset)
                .map_err(Error::IOError)?;
        }
        self.file_offset.store(file_offset, Ordering::Release);
        self.file.sync_all().map_err(Error::IOError)?;
        self.dirty.store(false, Ordering::Release);
        Ok(())
    }

    fn used_data_upper_bound(file: &File, physical_data_len: u64) -> Result<u64> {
        if physical_data_len == 0 {
            return Ok(0);
        }

        let mut end = physical_data_len;
        while end > 0 {
            let start = end.saturating_sub(READ_BUFFER_SIZE as u64);
            let chunk = read_available_at(
                file,
                (end - start) as usize,
                size_of::<DataFileHeader>() as u64 + start,
            )
            .map_err(Error::IOError)?;
            if let Some(rel) = chunk.iter().rposition(|byte| *byte != 0) {
                let aligned = (start + rel as u64 + 1).next_multiple_of(FILE_OFFSET_ALIGNMENT);
                return Ok(aligned.min(physical_data_len));
            }
            end = start;
        }

        Ok(0)
    }

    /// Scans forward from offset 0, parsing each entry, and returns the
    /// aligned end of the last valid entry.  We temporarily set `file_offset`
    /// to `tail_upper_bound` so that `read_next_entry_ref` won't short-circuit
    /// before reaching it.  This is safe because `open` is single-threaded;
    /// the real value is overwritten by the caller immediately after.
    fn detect_used_bytes(&self, tail_upper_bound: u64) -> Result<u64> {
        if tail_upper_bound == 0 {
            return Ok(0);
        }

        self.file_offset.store(tail_upper_bound, Ordering::Release);

        let mut offset = 0u64;
        let mut read_buf = Vec::new();
        let mut buf_file_offset = 0u64;
        let mut last_durable_offset = 0u64;
        while let Some((_, _, next_offset)) =
            self.read_next_entry_ref(offset, &mut read_buf, &mut buf_file_offset)?
        {
            offset = next_offset;
            last_durable_offset = next_offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);
        }

        Ok(last_durable_offset)
    }

    fn parse_data_entry(buf: &[u8], offset: u64) -> Result<ParsedDataEntry> {
        if buf.len() < 8 {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "entry too short",
            )));
        }

        let header = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let magic_offset = entry_magic_offset(offset);

        if header & DATA_ENTRY_OFFSET_MASK != magic_offset {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "corrupt entry magic",
            )));
        }

        let klen = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let vlen = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let entry_len = 4 + 4 + klen as usize + vlen as usize + 2;
        if buf.len() < entry_len {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "entry too short",
            )));
        }

        let checksum = u16::from_le_bytes(buf[entry_len - 2..entry_len].try_into().unwrap());
        if checksum != crc16_ibm3740_fast::hash(&buf[..entry_len - 2]) as u16 {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksum mismatch",
            )));
        }

        let ns = ((header >> 24) & ((1 << KEY_NAMESPACE_BITS) - 1)) as u8;
        let entry_type = (header >> 30) & 0b11;
        if entry_type != EntryType::Insert as u32 && entry_type != EntryType::Update as u32 {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid entry type",
            )));
        }

        Ok(ParsedDataEntry {
            data_len: 8 + vlen as usize + klen as usize,
            vlen,
            ns,
        })
    }

    pub(crate) fn open(
        base_path: &Path,
        config: Arc<Config>,
        file_idx: u16,
        validate_tail: bool,
    ) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .open(data_file_path(base_path, file_idx))
            .map_err(Error::IOError)?;
        let header = Self::read_header(&file)?;
        let physical_data_len = file
            .metadata()
            .map_err(Error::IOError)?
            .len()
            .saturating_sub(size_of::<DataFileHeader>() as u64);
        let preallocated = physical_data_len == config.max_data_file_size as u64;
        let recovery_tail_upper_bound = Self::used_data_upper_bound(&file, physical_data_len)?;

        let inst = Self {
            file,
            file_offset: AtomicU64::new(physical_data_len),
            dirty: AtomicBool::new(false),
            sync_lock: Mutex::new(()),
            sealed_for_rotation: AtomicBool::new(false),
            config,
            file_idx,
            file_ordinal: header.ordinal,
            preallocated,
            recovery_tail_upper_bound,
        };
        let used_bytes = if validate_tail {
            inst.detect_used_bytes(recovery_tail_upper_bound)?
        } else {
            recovery_tail_upper_bound
        };
        inst.file_offset.store(used_bytes, Ordering::Release);

        Ok(inst)
    }

    pub(crate) fn create(
        base_path: &Path,
        config: Arc<Config>,
        file_idx: u16,
        ordinal: u64,
    ) -> Result<Self> {
        let file = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(data_file_path(base_path, file_idx))
            .map_err(Error::IOError)?;
        file.set_len(size_of::<DataFileHeader>() as u64 + config.max_data_file_size as u64)
            .map_err(Error::IOError)?;
        let header = DataFileHeader {
            magic: *DATA_FILE_SIGNATURE,
            version: DATA_FILE_VERSION,
            _padding0: 0,
            ordinal,
            _trailer: [0; 4096 - 24],
        };
        write_all_at(&file, header.as_bytes(), 0).map_err(Error::IOError)?;
        file.sync_all().map_err(Error::IOError)?;
        sync_dir(base_path)?;
        Ok(Self {
            file,
            file_offset: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            sync_lock: Mutex::new(()),
            sealed_for_rotation: AtomicBool::new(false),
            config,
            file_idx,
            file_ordinal: ordinal,
            preallocated: true,
            recovery_tail_upper_bound: 0,
        })
    }

    pub(crate) fn seal_for_rotation(&self) {
        self.sealed_for_rotation.store(true, Ordering::SeqCst);
    }

    fn allocate(&self, len: u64) -> Result<u64> {
        if self.sealed_for_rotation.load(Ordering::SeqCst) {
            return Err(Error::RotateDataFile(self.file_idx));
        }
        let mut file_offset = self.file_offset.load(Ordering::Relaxed);
        loop {
            if file_offset + len > self.config.max_data_file_size as u64 {
                return Err(Error::RotateDataFile(self.file_idx));
            }
            match self.file_offset.compare_exchange(
                file_offset,
                file_offset + len,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(file_offset),
                Err(current) => file_offset = current,
            }
        }
    }

    fn append_entry<'a>(
        &self,
        entry_type: EntryType,
        ns: KeyNamespace,
        key: &[u8],
        val: Option<&[u8]>,
        shard_idx: usize,
        inflight_tracker: &'a InflightTracker,
    ) -> Result<(u64, usize, InflightGuard<'a>)> {
        debug_assert!(key.len() <= MAX_USER_KEY_SIZE);
        debug_assert!(ns as u8 <= MAX_KEY_NAMESPACE);

        let val_len = val.map_or(0, |v| v.len());
        if let Some(v) = val {
            debug_assert!(v.len() <= MAX_USER_VALUE_SIZE);
        }

        let entry_len = 4 + if val.is_some() { 4 } else { 2 } + val_len + key.len() + 2;
        let aligned_len = entry_len.next_multiple_of(FILE_OFFSET_ALIGNMENT as usize);
        let delta = match entry_type {
            EntryType::Insert => 1,
            EntryType::Tombstone => -1,
            _ => 0,
        };
        let (file_offset, inflight_guard) =
            inflight_tracker.reserve(self, shard_idx, aligned_len as u64, delta)?;
        debug_assert!(file_offset % FILE_OFFSET_ALIGNMENT == 0);

        let mut buf = SmallVec::<[u8; INLINE_SCRATCH_BUFFER_SIZE]>::with_capacity(aligned_len);
        // We overwrite the entry bytes below and only zero the alignment padding.
        unsafe { buf.set_len(aligned_len) };
        let buf = &mut buf[..];

        let magic_offset = entry_magic_offset(file_offset);
        let header = magic_offset | ((entry_type as u32) << 30) | ((ns as u32) << 24);

        buf[0..4].copy_from_slice(&header.to_le_bytes());
        buf[4..6].copy_from_slice(&(key.len() as u16).to_le_bytes());

        if let Some(v) = val {
            buf[6..8].copy_from_slice(&(v.len() as u16).to_le_bytes());
            buf[8..8 + v.len()].copy_from_slice(v);
            buf[8 + v.len()..8 + v.len() + key.len()].copy_from_slice(key);
        } else {
            buf[6..6 + key.len()].copy_from_slice(key);
        }

        // use a non-zero padding byte
        buf[entry_len..aligned_len].fill(0xff);
        let checksum = crc16_ibm3740_fast::hash(&buf[..entry_len - 2]) as u16;
        buf[entry_len - 2..entry_len].copy_from_slice(&checksum.to_le_bytes());

        let res = write_all_at(
            &self.file,
            buf,
            size_of::<DataFileHeader>() as u64 + file_offset,
        )
        .map_err(Error::IOError);
        // Set after the pwrite (even a failed one) so no sync can miss these pages.
        self.dirty.store(true, Ordering::Release);
        res?;

        Ok((file_offset, aligned_len, inflight_guard))
    }

    pub(crate) fn append_kv<'a>(
        &self,
        entry_type: EntryType,
        ns: KeyNamespace,
        key: &[u8],
        val: &[u8],
        shard_idx: usize,
        inflight_tracker: &'a InflightTracker,
    ) -> Result<(u64, usize, InflightGuard<'a>)> {
        debug_assert!(matches!(entry_type, EntryType::Insert | EntryType::Update));
        self.append_entry(entry_type, ns, key, Some(val), shard_idx, inflight_tracker)
    }

    pub(crate) fn append_tombstone<'a>(
        &self,
        ns: KeyNamespace,
        key: &[u8],
        shard_idx: usize,
        inflight_tracker: &'a InflightTracker,
    ) -> Result<(u64, usize, InflightGuard<'a>)> {
        self.append_entry(
            EntryType::Tombstone,
            ns,
            key,
            None,
            shard_idx,
            inflight_tracker,
        )
    }

    pub(crate) fn read_kv_into<'a>(
        &self,
        offset: u64,
        size_hint: usize,
        buf: &'a mut Vec<u8>,
    ) -> Result<KVRef<'a>> {
        debug_assert!(size_hint >= SIZE_HINT_UNIT);
        read_into_at(
            &self.file,
            buf,
            size_hint,
            size_of::<DataFileHeader>() as u64 + offset,
        )
        .map_err(Error::IOError)?;
        let parsed = Self::parse_data_entry(buf, offset)?;
        buf.truncate(parsed.data_len);
        Ok(KVRef {
            buf,
            vlen: parsed.vlen,
            header_len: 8,
            ns: parsed.ns,
            entry_type: EntryType::Insert,
        })
    }

    pub(crate) fn read_kv(&self, offset: u64, size_hint: usize) -> Result<KVBuf> {
        debug_assert!(size_hint >= SIZE_HINT_UNIT);
        let mut buf = read_available_at(
            &self.file,
            size_hint,
            size_of::<DataFileHeader>() as u64 + offset,
        )
        .map_err(Error::IOError)?;
        let parsed = Self::parse_data_entry(&buf, offset)?;
        buf.truncate(parsed.data_len);
        Ok(KVBuf {
            buf,
            vlen: parsed.vlen,
            header_len: 8,
            ns: parsed.ns,
            entry_type: EntryType::Insert,
        })
    }

    fn ensure_verified_entry(
        &self,
        read_buf: &mut Vec<u8>,
        buf_file_offset: &mut u64,
        rel: usize,
        entry_len: usize,
        offset: u64,
    ) -> Result<Option<usize>> {
        let start = if read_buf.len() - rel >= entry_len {
            rel
        } else {
            read_into_at(
                &self.file,
                read_buf,
                entry_len,
                size_of::<DataFileHeader>() as u64 + offset,
            )
            .map_err(Error::IOError)?;
            *buf_file_offset = offset;
            if read_buf.len() < entry_len {
                return Ok(None);
            }
            0
        };

        let entry_bytes = &read_buf[start..start + entry_len];
        let checksum =
            u16::from_le_bytes(entry_bytes[entry_len - 2..entry_len].try_into().unwrap());
        if checksum != crc16_ibm3740_fast::hash(&entry_bytes[..entry_len - 2]) as u16 {
            return Ok(None);
        }

        Ok(Some(start))
    }

    pub(crate) fn read_next_entry_ref<'a>(
        &self,
        mut offset: u64,
        read_buf: &'a mut Vec<u8>,
        buf_file_offset: &mut u64,
    ) -> Result<Option<(KVRef<'a>, u64, u64)>> {
        let used_bytes = self.used_bytes();
        if offset >= used_bytes {
            return Ok(None);
        }
        offset = offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);

        loop {
            if offset >= used_bytes {
                return Ok(None);
            }
            let buf_start = if offset >= *buf_file_offset {
                (offset - *buf_file_offset) as usize
            } else {
                read_buf.clear();
                0
            };

            if buf_start >= read_buf.len() || read_buf.len() - buf_start < 8 {
                read_into_at(
                    &self.file,
                    read_buf,
                    READ_BUFFER_SIZE,
                    size_of::<DataFileHeader>() as u64 + offset,
                )
                .map_err(Error::IOError)?;
                *buf_file_offset = offset;
                if read_buf.len() < 8 {
                    return Ok(None);
                }
            }

            let rel = (offset - *buf_file_offset) as usize;
            let avail = &read_buf[rel..];

            let header = u32::from_le_bytes(avail[0..4].try_into().unwrap());
            let magic_offset = entry_magic_offset(offset);
            if header & DATA_ENTRY_OFFSET_MASK != magic_offset {
                offset += FILE_OFFSET_ALIGNMENT;
                continue;
            }

            let ns = ((header >> 24) & ((1 << KEY_NAMESPACE_BITS) - 1)) as u8;
            let entry_type = (header >> 30) & 0b11;

            match entry_type {
                x if x == EntryType::Insert as u32 || x == EntryType::Update as u32 => {
                    let resolved_type = if x == EntryType::Insert as u32 {
                        EntryType::Insert
                    } else {
                        EntryType::Update
                    };
                    let klen = u16::from_le_bytes(avail[4..6].try_into().unwrap());
                    let vlen = u16::from_le_bytes(avail[6..8].try_into().unwrap());
                    let entry_len = 4 + 4 + klen as usize + vlen as usize + 2;

                    let Some(start) = self.ensure_verified_entry(
                        read_buf,
                        buf_file_offset,
                        rel,
                        entry_len,
                        offset,
                    )?
                    else {
                        offset += FILE_OFFSET_ALIGNMENT;
                        continue;
                    };
                    let buf = &read_buf[start..start + 8 + vlen as usize + klen as usize];

                    return Ok(Some((
                        KVRef {
                            buf,
                            vlen,
                            header_len: 8,
                            ns,
                            entry_type: resolved_type,
                        },
                        offset,
                        offset + entry_len as u64,
                    )));
                }
                x if x == EntryType::Tombstone as u32 => {
                    let klen = u16::from_le_bytes(avail[4..6].try_into().unwrap());
                    let entry_len = 4 + 2 + klen as usize + 2;

                    let Some(start) = self.ensure_verified_entry(
                        read_buf,
                        buf_file_offset,
                        rel,
                        entry_len,
                        offset,
                    )?
                    else {
                        offset += FILE_OFFSET_ALIGNMENT;
                        continue;
                    };
                    let buf = &read_buf[start..start + 6 + klen as usize];

                    return Ok(Some((
                        KVRef {
                            buf,
                            vlen: 0,
                            header_len: 6,
                            ns,
                            entry_type: EntryType::Tombstone,
                        },
                        offset,
                        offset + entry_len as u64,
                    )));
                }
                _ => {
                    return Err(invalid_data_error("unknown data entry type"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::mpsc, thread, time::Duration};

    use crate::types::INITIAL_DATA_FILE_ORDINAL;

    #[test]
    fn concurrent_sync_waits_for_in_progress_sync() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let data_file = Arc::new(DataFile::create(
            dir.path(),
            Arc::new(Config::default()),
            0,
            INITIAL_DATA_FILE_ORDINAL,
        )?);
        data_file.dirty.store(true, Ordering::Release);

        let (sync_started_tx, sync_started_rx) = mpsc::channel();
        let (release_sync_tx, release_sync_rx) = mpsc::channel();
        let first_file = Arc::clone(&data_file);
        let first = thread::spawn(move || {
            first_file.sync_to_current_with(|_| {
                sync_started_tx.send(()).unwrap();
                release_sync_rx.recv().unwrap();
                Ok(())
            })
        });
        sync_started_rx.recv().unwrap();

        let (second_started_tx, second_started_rx) = mpsc::channel();
        let (second_done_tx, second_done_rx) = mpsc::channel();
        let second_file = Arc::clone(&data_file);
        let second = thread::spawn(move || {
            second_started_tx.send(()).unwrap();
            let result = second_file
                .sync_to_current_with(|_| panic!("clean file should not be synced twice"));
            second_done_tx.send(()).unwrap();
            result
        });

        second_started_rx.recv().unwrap();
        assert!(
            second_done_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "a concurrent caller returned before the active sync completed"
        );
        release_sync_tx.send(()).unwrap();
        first.join().unwrap()?;
        second.join().unwrap()?;
        second_done_rx.recv().unwrap();
        Ok(())
    }

    #[test]
    fn write_after_sync_redirties_file() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let data_file = DataFile::create(
            dir.path(),
            Arc::new(Config::default()),
            0,
            INITIAL_DATA_FILE_ORDINAL,
        )?;
        let tracker = InflightTracker::new(1);
        assert!(!data_file.is_dirty());

        let (_, _, guard) = data_file.append_kv(
            EntryType::Insert,
            KeyNamespace::User,
            b"k",
            b"v",
            0,
            &tracker,
        )?;
        guard.complete();
        assert!(data_file.is_dirty());

        data_file.sync_to_current()?;
        assert!(!data_file.is_dirty());

        // A write that lands after a sync must not be masked by any offset watermark.
        let (_, _, guard) = data_file.append_kv(
            EntryType::Insert,
            KeyNamespace::User,
            b"k2",
            b"v",
            0,
            &tracker,
        )?;
        guard.complete();
        assert!(data_file.is_dirty());
        data_file.sync_to_current()?;
        assert!(!data_file.is_dirty());
        Ok(())
    }

    #[test]
    fn checkpoint_progress_uses_earliest_file_position() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let data_file = DataFile::create(
            dir.path(),
            Arc::new(Config::default()),
            0,
            INITIAL_DATA_FILE_ORDINAL,
        )?;
        let tracker = InflightTracker::new(2);

        tracker.slots[0]
            .ordinal
            .store(INITIAL_DATA_FILE_ORDINAL, Ordering::Relaxed);
        tracker.slots[0].offset.store(128, Ordering::Relaxed);
        tracker.slots[0].seq.store(1, Ordering::Release);

        tracker.slots[1]
            .ordinal
            .store(INITIAL_DATA_FILE_ORDINAL, Ordering::Relaxed);
        tracker.slots[1].offset.store(64, Ordering::Relaxed);
        tracker.slots[1].seq.store(2, Ordering::Release);

        assert_eq!(
            tracker.checkpoint_progress(&data_file),
            (INITIAL_DATA_FILE_ORDINAL, 64, 0, 1)
        );
        Ok(())
    }

    #[test]
    fn checkpoint_progress_does_not_drain_completed_deltas() -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::IOError)?;
        let data_file = DataFile::create(
            dir.path(),
            Arc::new(Config::default()),
            0,
            INITIAL_DATA_FILE_ORDINAL,
        )?;
        let tracker = InflightTracker::new(1);
        tracker.completed_deltas[0].lock().push_back((1, 1));
        tracker.completed_deltas[0].lock().push_back((2, -1));

        assert_eq!(
            tracker.checkpoint_progress(&data_file),
            (INITIAL_DATA_FILE_ORDINAL, 0, 0, u64::MAX)
        );
        assert_eq!(tracker.completed_deltas[0].lock().len(), 2);
        assert_eq!(tracker.drain_completed_before(u64::MAX), 0);
        assert!(tracker.completed_deltas[0].lock().is_empty());
        Ok(())
    }
}
