use crate::files::index_file::IndexFileHeader;
use crate::{
    CandyStore, Config, RecoveryMode,
    internal::{read_exact_at, write_all_at},
};
use std::fs::OpenOptions;

#[test]
fn test_happy_path_loading() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default();

    // 1. Create and populate DB
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..100 {
            store
                .set(
                    format!("key:{}", i).as_bytes(),
                    format!("val:{}", i).as_bytes(),
                )
                .unwrap();
        }
    } // store dropped here

    // 2. Reopen and verify
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..100 {
            let val = store.get(format!("key:{}", i).as_bytes()).unwrap();
            assert_eq!(val, Some(format!("val:{}", i).as_bytes().to_vec()));
        }
    }
}

#[test]
fn test_validation_failures() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::FailIfCorrupted,
        ..Config::default()
    };

    // Create a valid DB first
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key", b"value").unwrap();
    }

    let index_path = dir.path().join("index.db");

    // Case 1: Corrupt Magic
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        write_all_at(&file, &[0, 0, 0, 0], 0).unwrap();
    }
    assert!(
        CandyStore::open(dir.path(), config.clone()).is_err(),
        "Should fail with invalid magic"
    );

    // Restore DB for next test
    let _ = std::fs::remove_file(&index_path);
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key", b"value").unwrap();
    }

    // Case 2: Corrupt Version
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        write_all_at(&file, &[0xFF, 0, 0, 0], 4).unwrap();
    }
    assert!(
        CandyStore::open(dir.path(), config.clone()).is_err(),
        "Should fail with invalid version"
    );

    // Restore DB
    let _ = std::fs::remove_file(&index_path);
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key", b"value").unwrap();
    }

    // Case 3: Truncated File
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        file.set_len(100).unwrap(); // Too small for header
    }
    assert!(
        CandyStore::open(dir.path(), config.clone()).is_err(),
        "Should fail with truncated file"
    );

    // Restore DB
    let _ = std::fs::remove_file(&index_path);
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key", b"value").unwrap();
    }

    // Case 4: Inconsistent Split Level
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        // Offset 8 is global_split_level (after magic u32 + version u32)
        // Write a huge split level (e.g. 20) which requires a large file
        let huge_level = 20u64;
        write_all_at(&file, &huge_level.to_le_bytes(), 8).unwrap();
    }
    assert!(
        CandyStore::open(dir.path(), config.clone()).is_err(),
        "Should fail with inconsistent split level"
    );
}

#[test]
fn test_checksum_header_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::FailIfCorrupted,
        ..Config::default()
    };

    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key", b"value").unwrap();
    }

    // Corrupt the checksum in the header
    let index_path = dir.path().join("index.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_path)
        .unwrap();

    const CSUM_OFFSET: usize = std::mem::offset_of!(IndexFileHeader, index_checksum);

    let mut buf = [0u8; 8];
    read_exact_at(&file, &mut buf, CSUM_OFFSET as u64).unwrap();
    let old_checksum = u64::from_le_bytes(buf);
    println!("Old checksum: {:x}", old_checksum);

    buf[0] ^= 0xFF;
    write_all_at(&file, &buf, CSUM_OFFSET as u64).unwrap();

    let new_checksum = u64::from_le_bytes(buf);
    println!("New checksum: {:x}", new_checksum);

    // Now try to open
    let res = CandyStore::open(dir.path(), config);
    assert!(res.is_err());
}
