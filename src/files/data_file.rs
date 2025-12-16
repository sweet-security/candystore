use crate::types::{
    CandyError, DATA_FILE_MAGIC, DATA_FILE_VERSION, KEY_LEN_BITS, KEY_LEN_MASK, KeyNamespace,
    PAGE_SIZE, Result,
};
use crate::{MAX_KEY_LEN, MAX_VALUE_LEN};
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C, packed)]
struct DataFileHeader {
    magic: [u8; 8],
    version: u32,
    serial: u64,
    _padding1: [u8; 48],
    // 64
    checkpoint_offset: u64,
    aggregated_checksum: u64,
    _padding2: [u8; 4012],
}

const _: () = assert!(std::mem::size_of::<DataFileHeader>() == PAGE_SIZE);

#[doc(hidden)]
pub mod test_offsets {
    use super::*;
    pub const DATA_MAGIC: usize = std::mem::offset_of!(DataFileHeader, magic);
    pub const DATA_VERSION: usize = std::mem::offset_of!(DataFileHeader, version);
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataEntryType {
    KV = 0x00,
    Tombstone = 0x01,
    _Reserved1 = 0x02,
    _Reserved2 = 0x03,
}

pub(crate) struct KVBuf {
    pub buf: Vec<u8>,
    pub key_len: u16,
    pub entry_type: DataEntryType,
}

impl KVBuf {
    pub fn ns(&self) -> u8 {
        self.buf[DataFile::ENTRY_HEADER_LEN]
    }

    pub fn key(&self) -> &[u8] {
        &self.buf
            [DataFile::ENTRY_HEADER_LEN + 1..DataFile::ENTRY_HEADER_LEN + 1 + self.key_len as usize]
    }

    pub fn value(&self) -> &[u8] {
        &self.buf[(DataFile::ENTRY_HEADER_LEN + 1 + self.key_len as usize)..]
    }
}

#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(f, buf, offset)
}

#[cfg(unix)]
fn read_exact_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
}

#[cfg(unix)]
fn write_all_at(f: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(f, buf, offset)
}

#[cfg(windows)]
fn read_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(f, buf, offset)
}

#[cfg(windows)]
fn read_exact_at(f: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        match std::os::windows::fs::FileExt::seek_read(f, buf, offset) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                offset += n as u64;
            }
            Err(e) => return Err(e),
        }
    }
    if !buf.is_empty() {
        Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn write_all_at(f: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        match std::os::windows::fs::FileExt::seek_write(f, buf, offset) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) struct DataFile {
    pub file: File,
    pub serial: u64,
    pub write_offset: AtomicU64,
    pub flush_lock: Mutex<()>,
}

impl DataFile {
    pub const ENTRY_HEADER_LEN: usize = 4; // type|key_len (2) + value_len (2)
    pub const ENTRY_CHECKSUM_LEN: usize = 4; // checksum (4)

    pub fn open(path: &Path, serial: Option<u64>) -> Result<Self> {
        Self::_open(path, serial, false)
    }
    pub fn open_trunc(path: &Path, serial: Option<u64>) -> Result<Self> {
        Self::_open(path, serial, true)
    }

    fn _open(path: &Path, serial: Option<u64>, truncate: bool) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(truncate)
            .open(path)
            .map_err(CandyError::IOError)?;

        let len = file.metadata().map_err(CandyError::IOError)?.len();
        let header_size = std::mem::size_of::<DataFileHeader>() as u64;

        if len == 0 {
            let serial = serial.ok_or(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "serial required for new file",
            )))?;
            file.set_len(header_size).map_err(CandyError::IOError)?;
            let mut buf = vec![0u8; header_size as usize];
            let header = unsafe { &mut *(buf.as_mut_ptr() as *mut DataFileHeader) };
            header.magic = *DATA_FILE_MAGIC;
            header.version = DATA_FILE_VERSION;
            header.serial = serial;
            header.checkpoint_offset = 0;
            header.aggregated_checksum = 0;
            write_all_at(&file, &buf, 0).map_err(CandyError::IOError)?;

            Ok(Self {
                file,
                serial,
                write_offset: AtomicU64::new(0),
                flush_lock: Mutex::new(()),
            })
        } else {
            if len < header_size {
                return Err(CandyError::IOError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "data file too short",
                )));
            }
            let mut buf = vec![0u8; header_size as usize];
            read_exact_at(&file, &mut buf, 0).map_err(CandyError::IOError)?;
            let header = unsafe { &*(buf.as_ptr() as *const DataFileHeader) };
            if header.magic != *DATA_FILE_MAGIC {
                return Err(CandyError::IOError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid data file magic",
                )));
            }
            if header.version != DATA_FILE_VERSION {
                return Err(CandyError::IOError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid data file version",
                )));
            }

            // write_offset is relative to data start, so it's len - header_size
            let write_offset = len - header_size;

            Ok(Self {
                file,
                serial: header.serial,
                write_offset: AtomicU64::new(write_offset),
                flush_lock: Mutex::new(()),
            })
        }
    }

    pub(crate) fn read_entry(&self, offset: u32, size_hint: Option<u32>) -> Result<KVBuf> {
        let start_file_offset = offset as u64 + std::mem::size_of::<DataFileHeader>() as u64;

        let mut buf: Vec<u8> = if let Some(hint) = size_hint {
            let mut buf = vec![0u8; hint as usize];

            let mut read = 0;
            while read < buf.len() {
                match read_at(
                    &self.file,
                    &mut buf[read..],
                    start_file_offset + read as u64,
                ) {
                    Ok(0) => break,
                    Ok(n) => read += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(CandyError::IOError(e)),
                }
            }
            buf.truncate(read);
            buf
        } else {
            let header_len = Self::ENTRY_HEADER_LEN;
            let mut buf = vec![0u8; header_len];
            read_exact_at(&self.file, &mut buf, start_file_offset).map_err(CandyError::IOError)?;

            let key_len_raw = u16::from_le_bytes([buf[0], buf[1]]);
            let value_len = u16::from_le_bytes([buf[2], buf[3]]);
            let key_len = key_len_raw & KEY_LEN_MASK;

            let total_len = Self::ENTRY_HEADER_LEN
                + 1
                + key_len as usize
                + value_len as usize
                + Self::ENTRY_CHECKSUM_LEN;
            buf.resize(total_len, 0);
            read_exact_at(
                &self.file,
                &mut buf[header_len..],
                start_file_offset + header_len as u64,
            )
            .map_err(CandyError::IOError)?;
            buf
        };

        if buf.len() < Self::ENTRY_HEADER_LEN + 1 + Self::ENTRY_CHECKSUM_LEN {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "entry too short",
            )));
        }

        let key_len_raw = u16::from_le_bytes([buf[0], buf[1]]);
        let key_len = key_len_raw & KEY_LEN_MASK;
        let entry_type_id = key_len_raw >> KEY_LEN_BITS;
        let value_len = u16::from_le_bytes([buf[2], buf[3]]);

        let data_len = Self::ENTRY_HEADER_LEN + 1 + key_len as usize + value_len as usize;
        if buf.len() < data_len + Self::ENTRY_CHECKSUM_LEN {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "entry incomplete",
            )));
        }

        let checksum = u32::from_le_bytes([
            buf[data_len],
            buf[data_len + 1],
            buf[data_len + 2],
            buf[data_len + 3],
        ]);
        let expected = crc32fast::hash(&buf[..data_len]);
        if checksum != expected {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "checksum mismatch, got 0x{:x} expected 0x{:x}",
                    checksum, expected
                ),
            )));
        }

        let entry_type = match entry_type_id {
            0 => DataEntryType::KV,
            1 => DataEntryType::Tombstone,
            _ => {
                return Err(CandyError::IOError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid entry type {entry_type_id}"),
                )));
            }
        };

        buf.truncate(data_len);

        Ok(KVBuf {
            buf,
            key_len: key_len as u16,
            entry_type,
        })
    }

    pub fn read_kv(&self, offset: u32, size_hint: u32) -> Result<KVBuf> {
        let kv = self.read_entry(offset, Some(size_hint))?;
        if kv.entry_type != DataEntryType::KV {
            return Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid entry type {:?}", kv.entry_type),
            )));
        }
        Ok(kv)
    }

    pub fn append_kv<K: AsRef<[u8]> + ?Sized, V: AsRef<[u8]> + ?Sized>(
        &self,
        ns: KeyNamespace,
        key: &K,
        value: &V,
    ) -> Result<(u32, u32)> {
        let key = key.as_ref();
        let value = value.as_ref();
        debug_assert!(key.len() <= MAX_KEY_LEN);
        debug_assert!(value.len() <= MAX_VALUE_LEN);

        let data_len = Self::ENTRY_HEADER_LEN + 1 + key.len() + value.len();
        let mut buf: SmallVec<[u8; 1024]> =
            SmallVec::with_capacity(data_len + Self::ENTRY_CHECKSUM_LEN);
        buf.resize(data_len + Self::ENTRY_CHECKSUM_LEN, 0);

        let key_len_and_type = (key.len() as u16) | ((DataEntryType::KV as u16) << KEY_LEN_BITS);
        buf[0..2].copy_from_slice(&key_len_and_type.to_le_bytes());
        buf[2..4].copy_from_slice(&(value.len() as u16).to_le_bytes());
        buf[Self::ENTRY_HEADER_LEN] = ns as u8;
        buf[Self::ENTRY_HEADER_LEN + 1..Self::ENTRY_HEADER_LEN + 1 + key.len()]
            .copy_from_slice(key);
        buf[Self::ENTRY_HEADER_LEN + 1 + key.len()..data_len].copy_from_slice(value);
        let checksum = crc32fast::hash(&buf[..data_len]);
        buf[data_len..].copy_from_slice(&checksum.to_le_bytes());

        let offset = self
            .write_offset
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        debug_assert!(offset + buf.len() as u64 <= u32::MAX as u64);

        write_all_at(
            &self.file,
            &buf,
            offset + std::mem::size_of::<DataFileHeader>() as u64,
        )
        .map_err(CandyError::IOError)?;

        Ok((offset as u32, buf.len() as u32))
    }

    pub fn append_tombstone<K: AsRef<[u8]> + ?Sized>(
        &self,
        ns: KeyNamespace,
        key: &K,
    ) -> Result<(u32, u32)> {
        let key = key.as_ref();
        debug_assert!(key.len() <= MAX_KEY_LEN);

        let data_len = Self::ENTRY_HEADER_LEN + 1 + key.len();
        let mut buf: SmallVec<[u8; 512]> =
            SmallVec::with_capacity(data_len + Self::ENTRY_CHECKSUM_LEN);
        buf.resize(data_len + Self::ENTRY_CHECKSUM_LEN, 0);

        let key_len_and_type =
            (key.len() as u16) | ((DataEntryType::Tombstone as u16) << KEY_LEN_BITS);
        buf[0..2].copy_from_slice(&key_len_and_type.to_le_bytes());
        buf[Self::ENTRY_HEADER_LEN] = ns as u8;
        buf[Self::ENTRY_HEADER_LEN + 1..Self::ENTRY_HEADER_LEN + 1 + key.len()]
            .copy_from_slice(key);
        let checksum = crc32fast::hash(&buf[..data_len]);
        buf[data_len..].copy_from_slice(&checksum.to_le_bytes());

        let offset = self
            .write_offset
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        debug_assert!(offset + buf.len() as u64 <= u32::MAX as u64);

        write_all_at(
            &self.file,
            &buf,
            offset + std::mem::size_of::<DataFileHeader>() as u64,
        )
        .map_err(CandyError::IOError)?;

        Ok((offset as u32, buf.len() as u32))
    }

    pub fn flush_checkpoint(&self, offset: u64, checksum: u64) -> Result<()> {
        let _lock = self.flush_lock.lock();

        let mut buf = vec![0u8; std::mem::size_of::<DataFileHeader>()];

        read_exact_at(&self.file, &mut buf, 0).map_err(CandyError::IOError)?;

        let header = unsafe { &mut *(buf.as_mut_ptr() as *mut DataFileHeader) };
        header.checkpoint_offset = offset;
        header.aggregated_checksum = checksum;

        write_all_at(&self.file, &buf, 0).map_err(CandyError::IOError)?;
        self.file.sync_all().map_err(CandyError::IOError)?;
        Ok(())
    }

    pub fn iter_entries(&self) -> DataFileIterator<'_> {
        DataFileIterator::new(self, 0)
    }

    pub fn iter_entries_from(&self, offset: u32) -> DataFileIterator<'_> {
        DataFileIterator::new(self, offset)
    }

    pub fn reset(&self) -> Result<()> {
        let header_size = std::mem::size_of::<DataFileHeader>() as u64;
        self.file
            .set_len(header_size)
            .map_err(CandyError::IOError)?;
        self.file.sync_all().map_err(CandyError::IOError)?;
        self.write_offset.store(0, Ordering::Relaxed);
        self.flush_checkpoint(0, 0)?;
        Ok(())
    }
}

pub(crate) struct DataFileIterator<'a> {
    data_file: &'a DataFile,
    buffer: Vec<u8>,
    buffer_offset: usize,
    buffer_valid: usize,
    file_offset: u64,
    current_offset: u32,
}

impl<'a> DataFileIterator<'a> {
    const BUF_SIZE: usize = 128 * 1024;

    fn new(data_file: &'a DataFile, start_offset: u32) -> Self {
        let file_offset = if start_offset == 0 {
            std::mem::size_of::<DataFileHeader>() as u64
        } else {
            start_offset as u64 + std::mem::size_of::<DataFileHeader>() as u64
        };

        Self {
            data_file,
            buffer: vec![0u8; Self::BUF_SIZE],
            buffer_offset: 0,
            buffer_valid: 0,
            file_offset,
            current_offset: start_offset,
        }
    }

    fn fill_buffer(&mut self) -> Result<()> {
        // Update file_offset to point to the start of the remaining data
        self.file_offset += self.buffer_offset as u64;

        // Move remaining data to start
        if self.buffer_offset < self.buffer_valid {
            let remaining = self.buffer_valid - self.buffer_offset;
            self.buffer
                .copy_within(self.buffer_offset..self.buffer_valid, 0);
            self.buffer_valid = remaining;
        } else {
            self.buffer_valid = 0;
        }
        self.buffer_offset = 0;

        // Read more data
        let read_start = self.file_offset + self.buffer_valid as u64;
        let to_read = self.buffer.len() - self.buffer_valid;
        if to_read == 0 {
            return Ok(());
        }

        let n = read_at(
            &self.data_file.file,
            &mut self.buffer[self.buffer_valid..],
            read_start,
        )
        .map_err(CandyError::IOError)?;
        self.buffer_valid += n;
        Ok(())
    }
}

impl<'a> Iterator for DataFileIterator<'a> {
    type Item = Result<(u32, u32, KVBuf)>; // offset, size, kvbuf

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert!(self.buffer_offset <= self.buffer_valid);
        debug_assert!(self.buffer_valid <= self.buffer.len());

        // We don't check against write_offset here because we might be iterating
        // to find the valid tail (recovery), so we trust the file content until EOF/Error.

        // Ensure we have at least header size
        if self.buffer_valid - self.buffer_offset < DataFile::ENTRY_HEADER_LEN {
            if let Err(e) = self.fill_buffer() {
                return Some(Err(e));
            }
            if self.buffer_valid - self.buffer_offset < DataFile::ENTRY_HEADER_LEN {
                // EOF
                return None;
            }
        }

        let buf = &self.buffer[self.buffer_offset..];
        let key_len = u16::from_le_bytes([buf[0], buf[1]]);
        let value_len = u16::from_le_bytes([buf[2], buf[3]]);
        let key_len_masked = key_len & KEY_LEN_MASK;
        let data_len =
            DataFile::ENTRY_HEADER_LEN + 1 + key_len_masked as usize + value_len as usize;
        let total_len = data_len + DataFile::ENTRY_CHECKSUM_LEN;

        // Ensure we have full entry
        if self.buffer_valid - self.buffer_offset < total_len {
            // If entry is larger than buffer, we need to handle it.
            if total_len > self.buffer.len() {
                self.buffer.resize(total_len, 0);
            }

            if let Err(e) = self.fill_buffer() {
                return Some(Err(e));
            }

            if self.buffer_valid - self.buffer_offset < total_len {
                return Some(Err(CandyError::IOError(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "incomplete entry",
                ))));
            }
        }

        let buf = &self.buffer[self.buffer_offset..self.buffer_offset + total_len];

        // Verify checksum
        let checksum = u32::from_le_bytes([
            buf[data_len],
            buf[data_len + 1],
            buf[data_len + 2],
            buf[data_len + 3],
        ]);
        let expected = crc32fast::hash(&buf[..data_len]);
        if checksum != expected {
            return Some(Err(CandyError::IOError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksum mismatch",
            ))));
        }

        let entry_type = key_len >> KEY_LEN_BITS;
        let entry_type = match entry_type {
            x if x == DataEntryType::KV as u16 => DataEntryType::KV,
            x if x == DataEntryType::Tombstone as u16 => DataEntryType::Tombstone,
            _ => {
                return Some(Err(CandyError::IOError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid entry type",
                ))));
            }
        };

        let kvbuf = KVBuf {
            buf: buf[..data_len].to_vec(),
            key_len: key_len_masked,
            entry_type,
        };

        let offset = self.current_offset;
        self.current_offset += total_len as u32;
        self.buffer_offset += total_len;

        Some(Ok((offset, total_len as u32, kvbuf)))
    }
}
