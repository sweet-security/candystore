use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use candystore::internal::{
    DATA_FILE_HEADER_LEN, DATA_FILE_ORDINAL_OFFSET, FILE_OFFSET_ALIGNMENT, parse_data_file_idx,
};
use candystore::{Config, Error};

#[allow(dead_code)]
pub fn small_file_config() -> Config {
    Config {
        max_data_file_size: 16 * 1024,
        ..Config::default()
    }
}

#[allow(dead_code)]
pub fn read_data_file_ordinal(path: &Path) -> Result<u64, Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(Error::IOError)?;
    file.seek(SeekFrom::Start(DATA_FILE_ORDINAL_OFFSET))
        .map_err(Error::IOError)?;
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).map_err(Error::IOError)?;
    Ok(u64::from_le_bytes(buf))
}

#[allow(dead_code)]
pub fn active_file_ordinal(dir: &Path) -> Result<u64, Error> {
    let mut max_ordinal: Option<u64> = None;

    for entry in std::fs::read_dir(dir).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        let path = entry.path();
        if parse_data_file_idx(&path).is_none() {
            continue;
        }
        let ordinal = read_data_file_ordinal(&path)?;
        max_ordinal = Some(max_ordinal.map_or(ordinal, |current| current.max(ordinal)));
    }

    max_ordinal.ok_or_else(|| {
        Error::IOError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no data files found",
        ))
    })
}

#[allow(dead_code)]
pub fn logical_data_len(path: &std::path::Path) -> u64 {
    const CHUNK_LEN: usize = 64 * 1024;
    let header_len = DATA_FILE_HEADER_LEN;

    let mut file = std::fs::File::open(path).unwrap();
    let total_len = file.metadata().unwrap().len().saturating_sub(header_len);
    if total_len == 0 {
        return 0;
    }

    let mut end = total_len;
    let mut buf = vec![0u8; CHUNK_LEN];
    while end > 0 {
        let start = end.saturating_sub(CHUNK_LEN as u64);
        let chunk_len = (end - start) as usize;
        file.seek(SeekFrom::Start(header_len + start)).unwrap();
        file.read_exact(&mut buf[..chunk_len]).unwrap();
        if let Some(rel) = buf[..chunk_len].iter().rposition(|byte| *byte != 0) {
            return (start + rel as u64 + 1).next_multiple_of(FILE_OFFSET_ALIGNMENT);
        }
        end = start;
    }

    0
}
