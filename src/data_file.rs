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
    DATA_ENTRY_OFFSET_MAGIC, DATA_ENTRY_OFFSET_MASK, DATA_FILE_SIGNATURE, DATA_FILE_VERSION,
    EntryType, FILE_OFFSET_ALIGNMENT, KEY_NAMESPACE_BITS, KVBuf, KVRef, KeyNamespace,
    MAX_KEY_NAMESPACE, PAGE_SIZE, READ_BUFFER_SIZE, SIZE_HINT_UNIT, data_file_path,
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

    pub(crate) fn checkpoint_progress(&self, active_file: &DataFile) -> (u64, u64, i64) {
        let _barrier = self.snapshot_barrier.write();

        let mut min_slot: Option<(u64, u64, u64)> = None;
        for slot in &self.slots {
            let seq = slot.seq.load(Ordering::Acquire);
            if seq == 0 {
                continue;
            }
            let ordinal = slot.ordinal.load(Ordering::Relaxed);
            let offset = slot.offset.load(Ordering::Relaxed);
            let current = (seq, ordinal, offset);
            min_slot = Some(min_slot.map_or(current, |min_current| min_current.min(current)));
        }

        let checkpoint = min_slot
            .map(|(_, ordinal, offset)| (ordinal, offset))
            .unwrap_or_else(|| (active_file.file_ordinal, active_file.used_bytes()));
        let completed_before_seq = min_slot.map_or(u64::MAX, |(seq, _, _)| seq);
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

        (checkpoint.0, checkpoint.1, committed_delta)
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
    sealed_for_rotation: AtomicBool,
    config: Arc<Config>,
    pub(crate) file_idx: u16,
    pub(crate) file_ordinal: u64,
}

impl DataFile {
    pub(crate) fn used_bytes(&self) -> u64 {
        self.file_offset.load(Ordering::Acquire)
    }

    pub(crate) fn truncate_to_offset(&self, file_offset: u64) -> Result<()> {
        debug_assert_eq!(file_offset % FILE_OFFSET_ALIGNMENT, 0);
        self.file
            .set_len(size_of::<DataFileHeader>() as u64 + file_offset)
            .map_err(Error::IOError)?;
        self.file_offset.store(file_offset, Ordering::Release);
        self.file.sync_all().map_err(Error::IOError)
    }

    fn parse_data_entry(buf: &[u8], offset: u64) -> Result<ParsedDataEntry> {
        if buf.len() < 8 {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "entry too short",
            )));
        }

        let header = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let magic_offset = (((offset / FILE_OFFSET_ALIGNMENT) as u32) ^ DATA_ENTRY_OFFSET_MAGIC)
            & DATA_ENTRY_OFFSET_MASK;

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

    pub(crate) fn open(base_path: &Path, config: Arc<Config>, file_idx: u16) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .open(data_file_path(base_path, file_idx))
            .map_err(Error::IOError)?;
        let header =
            read_available_at(&file, size_of::<DataFileHeader>(), 0).map_err(Error::IOError)?;
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
        let mut file_offset = file
            .metadata()
            .map_err(Error::IOError)?
            .len()
            .saturating_sub(size_of::<DataFileHeader>() as u64);
        file_offset -= file_offset % FILE_OFFSET_ALIGNMENT;
        file.set_len(size_of::<DataFileHeader>() as u64 + file_offset)
            .map_err(Error::IOError)?;

        Ok(Self {
            file,
            file_offset: AtomicU64::new(file_offset),
            sealed_for_rotation: AtomicBool::new(false),
            config,
            file_idx,
            file_ordinal: header.ordinal,
        })
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
        file.set_len(size_of::<DataFileHeader>() as u64)
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
            sealed_for_rotation: AtomicBool::new(false),
            config,
            file_idx,
            file_ordinal: ordinal,
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

        let magic_offset = (((file_offset / FILE_OFFSET_ALIGNMENT) as u32)
            ^ DATA_ENTRY_OFFSET_MAGIC)
            & DATA_ENTRY_OFFSET_MASK;
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

        buf[entry_len..aligned_len].fill(0);
        let checksum = crc16_ibm3740_fast::hash(&buf[..entry_len - 2]) as u16;
        buf[entry_len - 2..entry_len].copy_from_slice(&checksum.to_le_bytes());

        let res = write_all_at(
            &self.file,
            buf,
            size_of::<DataFileHeader>() as u64 + file_offset,
        )
        .map_err(Error::IOError);
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
        offset = offset.next_multiple_of(FILE_OFFSET_ALIGNMENT);

        loop {
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
            let magic_offset = (((offset / FILE_OFFSET_ALIGNMENT) as u32)
                ^ DATA_ENTRY_OFFSET_MAGIC)
                & DATA_ENTRY_OFFSET_MASK;
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
