use std::hash::Hasher;
use std::io::{Read, Seek, SeekFrom};

use candystore::Config;

#[allow(dead_code)]
pub fn small_file_config() -> Config {
    Config {
        max_data_file_size: 16 * 1024,
        ..Config::default()
    }
}

#[allow(dead_code)]
pub fn checkpoint_slot_checksum(generation: u64, ordinal: u64, offset: u64) -> u64 {
    let mut hasher = siphasher::sip::SipHasher13::new();
    hasher.write_u64(generation);
    hasher.write_u64(ordinal);
    hasher.write_u64(offset);
    hasher.finish()
}

#[allow(dead_code)]
pub fn logical_data_len(path: &std::path::Path) -> u64 {
    const HEADER_LEN: u64 = 4096;
    const ALIGNMENT: u64 = 16;
    const CHUNK_LEN: usize = 64 * 1024;

    let mut file = std::fs::File::open(path).unwrap();
    let total_len = file.metadata().unwrap().len().saturating_sub(HEADER_LEN);
    if total_len == 0 {
        return 0;
    }

    let mut end = total_len;
    let mut buf = vec![0u8; CHUNK_LEN];
    while end > 0 {
        let start = end.saturating_sub(CHUNK_LEN as u64);
        let chunk_len = (end - start) as usize;
        file.seek(SeekFrom::Start(HEADER_LEN + start)).unwrap();
        file.read_exact(&mut buf[..chunk_len]).unwrap();
        if let Some(rel) = buf[..chunk_len].iter().rposition(|byte| *byte != 0) {
            return (start + rel as u64 + 1).next_multiple_of(ALIGNMENT);
        }
        end = start;
    }

    0
}
