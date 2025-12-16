use candystore::{CandyStore, Config};
use std::thread;
use std::time::Duration;

#[test]
fn test_compaction_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        max_data_file_size: 4096, // Small file size to force rotation
        compaction_interval: Duration::from_millis(100),
        compaction_min_file_size: 1024, // Allow compacting small files
        compaction_min_waste_threshold: 0.1, // Low threshold
        ..Default::default()
    };
    let store = CandyStore::open(dir.path(), config).unwrap();

    // Insert items to fill multiple files
    let val = vec![0u8; 100];
    // 100 bytes val + overhead ~ 16 bytes = 116 bytes.
    // 4096 / 116 = ~35 items per file.

    // Insert 100 items. Should create ~3 files.
    for i in 0u64..100 {
        store.set(&i.to_le_bytes(), &val).unwrap();
    }

    let stats = store.stats();
    assert!(stats.num_data_files >= 3);

    // Update 50 items. This creates waste in old files.
    for i in 0u64..50 {
        store.set(&i.to_le_bytes(), &val).unwrap();
    }

    // Wait for compaction
    let start_compactions = store.stats().num_compactions;
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(100));
        if store.stats().num_compactions > start_compactions {
            break;
        }
    }

    let stats = store.stats();
    assert!(
        stats.num_compactions > start_compactions,
        "Compaction should have triggered"
    );

    // Verify data is still there
    for i in 0u64..100 {
        let res = store.get(&i.to_le_bytes()).unwrap();
        assert!(res.is_some(), "Key {} missing", i);
    }
}

#[test]
fn test_compaction_min_file_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        max_data_file_size: 4096,
        compaction_interval: Duration::from_millis(100),
        compaction_min_file_size: 100_000, // Much larger than max_data_file_size
        compaction_min_waste_threshold: 0.1,
        ..Default::default()
    };
    let store = CandyStore::open(dir.path(), config).unwrap();

    let val = vec![0u8; 100];
    for i in 0u64..100 {
        store.set(&i.to_le_bytes(), &val).unwrap();
    }
    for i in 0u64..50 {
        store.set(&i.to_le_bytes(), &val).unwrap();
    }

    thread::sleep(Duration::from_secs(1));

    let stats = store.stats();
    assert_eq!(
        stats.num_compactions, 0,
        "Compaction should NOT have triggered due to file size limit"
    );
}

#[test]
fn test_compaction_waste_threshold_limit() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        max_data_file_size: 4096,
        compaction_interval: Duration::from_millis(100),
        compaction_min_file_size: 1024,
        compaction_min_waste_threshold: 0.9, // Very high threshold (90% waste required)
        ..Default::default()
    };
    let store = CandyStore::open(dir.path(), config).unwrap();

    let val = vec![0u8; 100];
    for i in 0u64..100 {
        store.set(&i.to_le_bytes(), &val).unwrap();
    }
    // Update only 1 item, creating very little waste
    store.set(&0u64.to_le_bytes(), &val).unwrap();

    thread::sleep(Duration::from_secs(1));

    let stats = store.stats();
    assert_eq!(
        stats.num_compactions, 0,
        "Compaction should NOT have triggered due to waste threshold"
    );
}
