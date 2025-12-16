use candystore::{CandyStore, Config, RecoveryMode};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

#[test]
fn test_reset_on_data_file_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::ClearAllIfCorrupted,
        ..Config::default()
    };

    // 1. Create a valid DB with some data
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key1", b"value1").unwrap();
        store.set(b"key2", b"value2").unwrap();
    }

    // 2. Corrupt the data file (magic)
    // Find the data file
    let data_path = dir.path().join("data_00000.db");
    assert!(data_path.exists());

    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        // Corrupt magic
        file.write_all_at(&[0; 8], candystore::test_offsets::DATA_MAGIC as u64)
            .unwrap();
    }

    // 3. Open with RecoveryMode::ClearAllIfCorrupted
    // Should succeed and be empty
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        assert!(store.get(b"key1").unwrap().is_none());
        assert!(store.get(b"key2").unwrap().is_none());

        // Should be able to write new data
        store.set(b"key3", b"value3").unwrap();
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    }
}

#[test]
fn test_reset_on_data_file_version_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::ClearAllIfCorrupted,
        ..Config::default()
    };

    // 1. Create a valid DB with some data
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        store.set(b"key1", b"value1").unwrap();
    }

    // 2. Corrupt the data file (version)
    let data_path = dir.path().join("data_00000.db");
    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        // Version is at offset 8 (after 8 bytes magic)
        file.write_all_at(
            &[0xFF, 0xFF, 0xFF, 0xFF],
            candystore::test_offsets::DATA_VERSION as u64,
        )
        .unwrap();
    }

    // 3. Open with RecoveryMode::ClearAllIfCorrupted
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        assert!(store.get(b"key1").unwrap().is_none());

        store.set(b"key2", b"value2").unwrap();
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }
}
