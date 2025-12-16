use crate::store::CandyStore;
use crate::types::{Config, KeyNamespace, SpecialEntryType};

#[test]
fn test_special_keys_stats() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        max_data_file_size: 4096, // Small
        ..Config::default()
    };
    let store = CandyStore::open(dir.path(), config).unwrap();

    // Insert enough to rotate
    let val = vec![0u8; 100];
    // 100 bytes val + ~8 bytes key + header ~4 + checksum 4 = ~116 bytes per entry.
    // 4096 / 116 ~= 35 entries per file.
    for i in 0u64..100 {
        store.set(&i.to_le_bytes(), &val).unwrap();
    }

    let stats = store.stats();
    assert!(stats.num_data_files > 1);

    // Remove all
    for i in 0u64..100 {
        store.remove(&i.to_le_bytes()).unwrap();
    }

    // Check file 0 stats
    // File 0 should be full and then rotated.
    // Removing keys from it should update special keys.

    let wasted = store
        ._get_special_key(
            KeyNamespace::StatsWastedBytes,
            SpecialEntryType::WastedBytes,
            0,
        )
        .unwrap();
    // It might be None if we didn't remove anything from file 0 (unlikely)
    // or if file 0 was active when we removed (unlikely if we rotated).

    if let Some(wasted_bytes) = wasted {
        println!("File 0 wasted: {}", wasted_bytes);
        assert!(wasted_bytes > 0);
    } else {
        // If file 0 is still active (e.g. if we didn't rotate), then it's None.
        // But we asserted num_data_files > 1.
        // However, active_file_id might be 1.
        // If we remove from file 0, it is NOT active.
        // So we should have stats.
        panic!("Expected wasted bytes for file 0");
    }
}
