use siphasher::sip128::{Hasher128, SipHasher13};

use std::{
    fs::File,
    hash::Hasher,
    path::{Path, PathBuf},
};

use crate::types::{Error, Result};

pub(crate) const PAGE_SIZE: usize = 4096;
pub(crate) const ROW_WIDTH: usize = 16 * 21;
pub(crate) const MIN_SPLIT_LEVEL: usize = 3;
pub(crate) const MASKED_ROW_SELECTOR_BITS: u32 = 18;
pub(crate) const MIN_INITIAL_ROWS: usize = 1 << MIN_SPLIT_LEVEL;
pub(crate) const MAX_REPRESENTABLE_FILE_SIZE: u32 =
    ((1u32 << 26) - 1) * FILE_OFFSET_ALIGNMENT as u32;
pub(crate) const ENTRY_TYPE_SHIFT: u32 = 14;
pub(crate) const MAX_INTERNAL_KEY_SIZE: usize = (1 << ENTRY_TYPE_SHIFT) - 1;
pub(crate) const MAX_INTERNAL_VALUE_SIZE: usize = (1 << 16) - 1;
pub(crate) const MAX_DATA_FILES: u16 = 1 << 12;
pub(crate) const MAX_DATA_FILE_IDX: u16 = MAX_DATA_FILES - 1;

pub(crate) const INDEX_FILE_SIGNATURE: &[u8; 8] = b"CandyIdx";
pub(crate) const INDEX_FILE_VERSION: u32 = 0x0002_0009;
pub(crate) const DATA_FILE_SIGNATURE: &[u8; 8] = b"CandyDat";
pub(crate) const DATA_FILE_VERSION: u32 = 0x0002_0003;
pub(crate) const FILE_OFFSET_ALIGNMENT: u64 = 16;
pub(crate) const SIZE_HINT_UNIT: usize = 512;
pub(crate) const DATA_ENTRY_OFFSET_MAGIC: u32 = 0x91c8_d7cd;
pub(crate) const DATA_ENTRY_OFFSET_BITS: u8 = 24;
pub(crate) const DATA_ENTRY_OFFSET_MASK: u32 = (1 << DATA_ENTRY_OFFSET_BITS) - 1;
pub(crate) const KEY_NAMESPACE_BITS: u8 = 6;

/// Computes the magic offset field for a data entry at the given file offset.
pub fn entry_magic_offset(file_offset: u64) -> u32 {
    let magic = (((file_offset / FILE_OFFSET_ALIGNMENT) as u32) ^ DATA_ENTRY_OFFSET_MAGIC)
        & DATA_ENTRY_OFFSET_MASK;
    // ensure magic is never 0 so a valid entry cannot be all zeros
    if magic == 0 {
        DATA_ENTRY_OFFSET_MAGIC & DATA_ENTRY_OFFSET_MASK
    } else {
        magic
    }
}
pub(crate) const MAX_KEY_NAMESPACE: u8 = (1 << KEY_NAMESPACE_BITS) - 1;
pub(crate) const READ_BUFFER_SIZE: usize = 128 * 1024;

pub(crate) fn aligned_data_entry_waste(klen: usize, vlen: usize) -> u32 {
    (10 + klen as u32 + vlen as u32).next_multiple_of(FILE_OFFSET_ALIGNMENT as u32)
}

pub(crate) fn aligned_tombstone_entry_waste(klen: usize) -> u32 {
    (8 + klen as u32).next_multiple_of(FILE_OFFSET_ALIGNMENT as u32)
}

pub(crate) fn aligned_data_entry_size(klen: usize, vlen: usize) -> u64 {
    (10 + klen as u64 + vlen as u64).next_multiple_of(FILE_OFFSET_ALIGNMENT)
}

pub(crate) fn index_file_path(base_path: &Path) -> PathBuf {
    base_path.join("index")
}

pub(crate) fn index_rows_file_path(base_path: &Path) -> PathBuf {
    base_path.join("rows")
}

pub(crate) fn data_file_path(base_path: &Path, file_idx: u16) -> PathBuf {
    base_path.join(format!("data_{file_idx:04}"))
}

#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(Error::IOError)?
        .sync_all()
        .map_err(Error::IOError)
}

#[cfg(not(unix))]
pub(crate) fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn sync_file_range(file: &File, offset: u64, len: u64) -> Result<()> {
    use std::os::fd::AsRawFd;

    if len == 0 {
        return Ok(());
    }

    let sync_offset = i64::try_from(offset)
        .map_err(|_| Error::IOError(std::io::Error::other("sync offset overflow")))?;
    let sync_len = i64::try_from(len)
        .map_err(|_| Error::IOError(std::io::Error::other("sync length overflow")))?;

    let rc = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            sync_offset,
            sync_len,
            libc::SYNC_FILE_RANGE_WAIT_BEFORE
                | libc::SYNC_FILE_RANGE_WRITE
                | libc::SYNC_FILE_RANGE_WAIT_AFTER,
        )
    };
    if rc == 0 {
        return Ok(());
    }

    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP) => {
            file.sync_data().map_err(Error::IOError)
        }
        _ => Err(Error::IOError(err)),
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn sync_file_range(file: &File, _offset: u64, len: u64) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    file.sync_data().map_err(Error::IOError)
}

pub(crate) fn parse_data_file_idx(path: &Path) -> Option<u16> {
    let name = path.file_name()?.to_str()?;
    let suffix = name.strip_prefix("data_")?;
    if suffix.len() != 4 {
        return None;
    }
    suffix.parse().ok()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RangeMetadata {
    pub(crate) head: u64,
    pub(crate) tail: u64,
    pub(crate) count: u64,
}

impl RangeMetadata {
    pub(crate) fn new() -> Self {
        Self {
            head: 1u64 << 63,
            tail: (1u64 << 63) - 1,
            count: 0,
        }
    }

    pub(crate) fn to_bytes(self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.head.to_le_bytes());
        buf[8..16].copy_from_slice(&self.tail.to_le_bytes());
        buf[16..24].copy_from_slice(&self.count.to_le_bytes());
        buf
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 24 {
            return None;
        }
        Some(Self {
            head: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
            tail: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
            count: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
        })
    }
}

#[repr(u16)]
pub(crate) enum EntryType {
    Insert = 0,
    Update = 1,
    Tombstone = 2,
    // for future use: extended entries
    #[allow(unused)]
    Extended = 3,
}

pub(crate) fn invalid_data_error(message: &'static str) -> Error {
    Error::IOError(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

pub(crate) fn unexpected_eof_error(message: &'static str) -> Error {
    Error::IOError(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        message,
    ))
}

pub(crate) fn is_resettable_open_error(err: &Error) -> bool {
    matches!(
        err,
        Error::IOError(io_err)
            if matches!(
                io_err.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
            )
    )
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum KeyNamespace {
    #[allow(dead_code)]
    Invalid = 0, // reserves 0, must NOT be written to the file
    User = 1,
    QueueMeta = 2,
    QueueData = 3,
    BigMeta = 4,
    BigData = 5,
    ListMeta = 6,
    ListIndex = 7,
    ListData = 8,
    Typed = 9,
    TypedQueueMeta = 10,
    TypedQueueData = 11,
    TypedBigMeta = 12,
    TypedBigData = 13,
    TypedListMeta = 14,
    TypedListIndex = 15,
    TypedListData = 16,
}

const _: () = assert!((KeyNamespace::TypedListData as u8) < (1 << KEY_NAMESPACE_BITS));

impl KeyNamespace {
    pub(crate) fn from_u8(ns: u8) -> Option<Self> {
        match ns {
            x if x == Self::User as u8 => Some(Self::User),
            x if x == Self::QueueMeta as u8 => Some(Self::QueueMeta),
            x if x == Self::QueueData as u8 => Some(Self::QueueData),
            x if x == Self::BigMeta as u8 => Some(Self::BigMeta),
            x if x == Self::BigData as u8 => Some(Self::BigData),
            x if x == Self::ListMeta as u8 => Some(Self::ListMeta),
            x if x == Self::ListIndex as u8 => Some(Self::ListIndex),
            x if x == Self::ListData as u8 => Some(Self::ListData),
            x if x == Self::Typed as u8 => Some(Self::Typed),
            x if x == Self::TypedQueueMeta as u8 => Some(Self::TypedQueueMeta),
            x if x == Self::TypedQueueData as u8 => Some(Self::TypedQueueData),
            x if x == Self::TypedBigMeta as u8 => Some(Self::TypedBigMeta),
            x if x == Self::TypedBigData as u8 => Some(Self::TypedBigData),
            x if x == Self::TypedListMeta as u8 => Some(Self::TypedListMeta),
            x if x == Self::TypedListIndex as u8 => Some(Self::TypedListIndex),
            x if x == Self::TypedListData as u8 => Some(Self::TypedListData),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct HashCoord {
    pub(crate) sig: u32,
    pub(crate) row_selector: u32,
}

impl HashCoord {
    pub(crate) const INVALID_SIG: u32 = 0;

    pub(crate) fn new(ns: KeyNamespace, key: &[u8], hash_key: (u64, u64)) -> Self {
        let mut hasher = SipHasher13::new_with_keys(hash_key.0, hash_key.1);
        hasher.write_u8(ns as u8);
        hasher.write(key);
        let h = hasher.finish128();
        let row_selector = h.h1 as u32;
        let mut sig = (h.h1 >> 32) as u32;
        if sig == Self::INVALID_SIG {
            sig = h.h2 as u32;
            if sig == Self::INVALID_SIG {
                sig = (h.h2 >> 32) as u32;
                if sig == Self::INVALID_SIG {
                    sig = 0x6419_9a93;
                }
            }
        }

        Self { sig, row_selector }
    }

    pub(crate) fn masked_row_selector(&self) -> u32 {
        (self.row_selector >> MIN_SPLIT_LEVEL) & ((1 << MASKED_ROW_SELECTOR_BITS) - 1)
    }

    pub(crate) fn row_index(&self, split_level: u64) -> usize {
        debug_assert!(split_level >= MIN_SPLIT_LEVEL as u64, "sl={split_level}");
        ((self.row_selector as u64) & ((1 << split_level) - 1)) as usize
    }
}

pub(crate) struct KVBuf {
    pub(crate) buf: Vec<u8>,
    pub(crate) vlen: u16,
    pub(crate) header_len: u16,
    #[allow(dead_code)]
    pub(crate) ns: u8,
    #[allow(dead_code)]
    pub(crate) entry_type: EntryType,
}

impl KVBuf {
    pub(crate) fn value(&self) -> &[u8] {
        let start = self.header_len as usize;
        &self.buf[start..start + self.vlen as usize]
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.buf[self.header_len as usize + self.vlen as usize..]
    }

    pub(crate) fn into_value(mut self) -> Vec<u8> {
        let start = self.header_len as usize;
        let vlen = self.vlen as usize;
        if start > 0 {
            self.buf.copy_within(start..start + vlen, 0);
        }
        self.buf.truncate(vlen);
        self.buf
    }
}

pub(crate) struct KVRef<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) vlen: u16,
    pub(crate) header_len: u16,
    pub(crate) ns: u8,
    pub(crate) entry_type: EntryType,
}

impl KVRef<'_> {
    pub(crate) fn value(&self) -> &[u8] {
        let start = self.header_len as usize;
        &self.buf[start..start + self.vlen as usize]
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.buf[self.header_len as usize + self.vlen as usize..]
    }
}

#[cfg(unix)]
pub(crate) fn read_into_at(
    f: &File,
    buf: &mut Vec<u8>,
    count: usize,
    file_offset: u64,
) -> std::io::Result<()> {
    buf.resize(count, 0);
    let mut offset = 0;
    while offset < count {
        let n = std::os::unix::fs::FileExt::read_at(
            f,
            &mut buf[offset..],
            file_offset + offset as u64,
        )?;
        if n == 0 {
            break;
        } else {
            offset += n;
        }
    }
    buf.truncate(offset);
    Ok(())
}

#[cfg(windows)]
pub(crate) fn read_into_at(
    f: &File,
    buf: &mut Vec<u8>,
    count: usize,
    file_offset: u64,
) -> std::io::Result<()> {
    buf.resize(count, 0);
    let mut offset = 0;
    while offset < count {
        let n = std::os::windows::fs::FileExt::seek_read(
            f,
            &mut buf[offset..],
            file_offset + offset as u64,
        )?;
        if n == 0 {
            break;
        } else {
            offset += n;
        }
    }
    buf.truncate(offset);
    Ok(())
}

pub(crate) fn read_available_at(
    f: &File,
    count: usize,
    file_offset: u64,
) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    read_into_at(f, &mut buf, count, file_offset)?;
    Ok(buf)
}

#[cfg(unix)]
pub(crate) fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
}

#[cfg(windows)]
pub(crate) fn write_all_at(f: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let written = std::os::windows::fs::FileExt::seek_write(f, buf, offset)?;
        if written == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        buf = &buf[written..];
        offset += written as u64;
    }
    Ok(())
}
