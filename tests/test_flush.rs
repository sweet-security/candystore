use candystore::{CandyStore, Config};
use std::thread;
use std::time::Duration;

#[test]
fn test_flush() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        flush_interval: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    let store = CandyStore::open(dir.path(), config).unwrap();

    // Write some data
    store.set(b"key1", b"value1").unwrap();

    // Wait for flush
    thread::sleep(Duration::from_millis(500));

    // We can't easily verify flush happened without inspecting the file or mocking.
    // But we can check if it crashes or hangs.

    store.set(b"key2", b"value2").unwrap();
    thread::sleep(Duration::from_millis(500));
}

#[test]
fn test_explicit_flush() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let store = CandyStore::open(dir.path(), config).unwrap();

    store.set(b"key1", b"value1").unwrap();
    store.flush().unwrap();

    // Verify we can read it back immediately
    assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
}
