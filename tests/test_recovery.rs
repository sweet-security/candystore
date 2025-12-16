use candystore::{CandyStore, Config, RecoveryMode};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

#[test]
fn test_naive_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        ..Default::default()
    };

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
        // Ensure data is flushed
    }

    let index_path = dir.path().join("index.db");

    // 2. Corrupt the index file (checksum mismatch)
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        // Corrupt the checksum
        file.write_all_at(&[0xFF; 8], candystore::test_offsets::INDEX_CHECKSUM as u64)
            .unwrap();
    }

    // 3. Open DB - should trigger recovery
    // Note: Currently it returns error. We want it to recover.
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    // 4. Verify data
    for i in 0..100 {
        let val = store.get(format!("key:{}", i).as_bytes()).unwrap();
        assert_eq!(val, Some(format!("val:{}", i).as_bytes().to_vec()));
    }
}

#[test]
fn test_data_corruption_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        ..Default::default()
    };

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
    }

    let index_path = dir.path().join("index.db");
    let data_path = dir.path().join("data_00000.db");

    // 2. Corrupt the index file to force recovery
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        file.write_all_at(&[0xFF; 8], candystore::test_offsets::INDEX_CHECKSUM as u64)
            .unwrap();
    }

    // 3. Corrupt the data file (truncate the last entry)
    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        let len = file.metadata().unwrap().len();
        // Truncate by 5 bytes, which should damage the last entry (checksum or value)
        file.set_len(len - 5).unwrap();
    }

    // 4. Open DB - should trigger recovery and handle data corruption
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    // 5. Verify data
    // The last key (99) should be missing because its entry was corrupted/truncated.
    // Keys 0..98 should be present.
    for i in 0..99 {
        let val = store.get(format!("key:{}", i).as_bytes()).unwrap();
        assert_eq!(
            val,
            Some(format!("val:{}", i).as_bytes().to_vec()),
            "Key {} should be present",
            i
        );
    }

    let val_99 = store.get(b"key:99").unwrap();
    assert_eq!(val_99, None, "Key 99 should be missing due to corruption");
}

#[test]
fn test_recovery_multiple_files() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        max_data_file_size: 4096, // Small file size to force multiple files
        ..Default::default()
    };

    let num_items = 1000;

    // 1. Create and populate DB
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..num_items {
            store
                .set(
                    format!("key:{:04}", i).as_bytes(),
                    format!("val:{:04}", i).as_bytes(),
                )
                .unwrap();
        }
    }

    // Verify we have multiple data files
    let entries = std::fs::read_dir(dir.path()).unwrap();
    let data_files_count = entries
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .starts_with("data_")
        })
        .count();
    assert!(data_files_count > 1, "Should have multiple data files");

    let index_path = dir.path().join("index.db");

    // 2. Corrupt the index file
    {
        let file = OpenOptions::new().write(true).open(&index_path).unwrap();
        file.write_all_at(&[0xFF; 8], candystore::test_offsets::INDEX_CHECKSUM as u64)
            .unwrap();
    }

    // 3. Open DB - should trigger recovery across all files
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    // 4. Verify data
    for i in 0..num_items {
        let val = store.get(format!("key:{:04}", i).as_bytes()).unwrap();
        assert_eq!(
            val,
            Some(format!("val:{:04}", i).as_bytes().to_vec()),
            "Key {} missing after recovery",
            i
        );
    }
}
