use candystore::{CandyStore, Config, MAX_VALUE_LEN};
use std::time::Instant;

#[test]
fn test_basic_ops() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        max_data_file_size: 10 * 1024 * 1024,
        ..Default::default()
    };

    #[cfg(debug_assertions)]
    const ITERATIONS: usize = 10_000;

    #[cfg(not(debug_assertions))]
    const ITERATIONS: usize = 1_000_000;

    for i in 1..=1 {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();

        let n = ITERATIONS;
        let items = (0..n)
            .map(|i| {
                let key = format!("key:{:016}", i);
                let value = format!("val:{:016}", i);
                (key, value)
            })
            .collect::<Vec<_>>();

        println!("Inserting {} items...", n);
        let start = Instant::now();
        for (key, value) in &items {
            println!("{key}");
            store.set(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let duration = start.elapsed();
        println!(
            "Inserted {} items in {:?}. Avg: {:.2} us/op",
            n,
            duration,
            duration.as_micros() as f64 / n as f64
        );

        println!("Verifying {} items...", n);
        let start = Instant::now();
        for (key, expected_value) in &items {
            let val = store.get(key.as_bytes()).unwrap();
            assert!(val.is_some(), "Key not found: {}", key);
            assert_eq!(
                val.unwrap(),
                expected_value.as_bytes(),
                "Value mismatch for key: {}",
                key
            );
        }
        let duration = start.elapsed();
        println!(
            "Verified {} items in {:?}. Avg: {:.2} us/op",
            n,
            duration,
            duration.as_micros() as f64 / n as f64
        );

        println!("Updating {} items...", n);
        let start = Instant::now();
        for (key, value) in &items {
            store.set(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let duration = start.elapsed();
        println!(
            "Updated {} items in {:?}. Avg: {:.2} us/op",
            n,
            duration,
            duration.as_micros() as f64 / n as f64
        );

        println!("Listing {} items...", n);
        let start = Instant::now();
        let count = store.iter().count();
        let duration = start.elapsed();
        assert_eq!(count, n);
        println!(
            "Listed {} items in {:?}. Avg: {:.2} us/op",
            count,
            duration,
            duration.as_micros() as f64 / n as f64
        );

        println!("Removing {} items...", n);
        let start = Instant::now();
        for (key, _) in &items {
            let val = store.remove(key.as_bytes()).unwrap();
            assert!(val.is_some(), "Key not found for removal: {}", key);
        }
        let duration = start.elapsed();
        println!(
            "Removed {} items in {:?}. Avg: {:.2} us/op",
            n,
            duration,
            duration.as_micros() as f64 / n as f64
        );

        println!("Verifying removal...");
        for (key, _) in &items {
            let val = store.get(key.as_bytes()).unwrap();
            assert!(val.is_none(), "Key should be removed: {}", key);
        }

        let stats = store.stats();
        println!("Stats: {:?}", stats);
        assert_eq!(stats.num_inserts, i * n);
        assert_eq!(stats.num_removals, i * n);
    }
}

#[test]
fn test_clear_and_contains() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    assert!(!store.contains(b"foo").unwrap());
    store.set(b"foo", b"bar").unwrap();
    assert!(store.contains(b"foo").unwrap());
    assert_eq!(store.get(b"foo").unwrap(), Some(b"bar".to_vec()));

    store.clear().unwrap();
    assert!(!store.contains(b"foo").unwrap());
    assert_eq!(store.iter().count(), 0);

    store.set(b"foo", b"baz").unwrap();
    assert_eq!(store.get(b"foo").unwrap(), Some(b"baz".to_vec()));
}

#[test]
fn test_big_value_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let big = vec![42u8; MAX_VALUE_LEN + 17];

    assert!(!store.remove_big(b"big").unwrap());
    assert!(!store.contains(b"big").unwrap());

    assert!(!store.set_big(b"big", &big).unwrap());
    // set_big stores chunks in the queue namespace; KV contains remains false
    assert!(!store.contains(b"big").unwrap());

    let fetched = store.get_big(b"big").unwrap();
    assert_eq!(fetched.as_deref(), Some(big.as_slice()));

    assert!(store.remove_big(b"big").unwrap());
    assert!(store.get_big(b"big").unwrap().is_none());
    assert!(!store.contains(b"big").unwrap());
}
