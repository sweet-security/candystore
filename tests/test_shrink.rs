use candystore::{CandyStore, Config};
use std::sync::Arc;

#[test]
fn test_shrink_index() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        initial_capacity: 4, // Start very small
        ..Config::default()
    };
    let store = Arc::new(CandyStore::open(dir.path(), config).unwrap());

    // Insert enough keys to force split
    // ROW_WIDTH is 576.
    // We need to fill a row to force split.
    // With 4 rows, we need ~600 keys that hash to the same row, or just many keys.
    // Let's insert 10,000 keys.
    for i in 0..10000 {
        let key = format!("key_{}", i);
        store.set(key.as_bytes(), b"value").unwrap();
    }

    // We can't easily check rows directly via public API, but shrink returns new size.

    // Remove most keys
    for i in 0..9000 {
        let key = format!("key_{}", i);
        store.remove(key.as_bytes()).unwrap();
    }

    // Shrink
    let new_rows = store.shrink_index_blocking(0.2).unwrap();
    println!("Shrunk to {} rows", new_rows);

    // It should be smaller than if we didn't shrink, but hard to assert exact number without knowing distribution.
    // But it should definitely be valid.

    // Verify remaining keys
    for i in 9000..10000 {
        let key = format!("key_{}", i);
        let val = store.get(key.as_bytes()).unwrap();
        assert_eq!(val, Some(b"value".to_vec()));
    }

    // Verify removed keys
    for i in 0..9000 {
        let key = format!("key_{}", i);
        let val = store.get(key.as_bytes()).unwrap();
        assert_eq!(val, None);
    }
}
