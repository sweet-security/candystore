use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use candystore::{Config, RebuildStrategy};

#[allow(dead_code)]
pub fn small_file_config() -> Config {
    Config {
        max_data_file_size: 16 * 1024,
        ..Config::default()
    }
}

#[allow(dead_code)]
pub fn rebuild_if_dirty_config() -> Config {
    Config {
        rebuild_strategy: RebuildStrategy::RebuildIfDirty,
        ..Config::default()
    }
}

#[allow(dead_code)]
pub fn corrupt_first_row_checksum(path: &Path) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("rows"))
        .unwrap();
    let checksum_offset = 8;
    file.seek(SeekFrom::Start(checksum_offset)).unwrap();
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).unwrap();
    let checksum = u64::from_le_bytes(buf) ^ 1;
    file.seek(SeekFrom::Start(checksum_offset)).unwrap();
    file.write_all(&checksum.to_le_bytes()).unwrap();
    file.sync_all().unwrap();
}
