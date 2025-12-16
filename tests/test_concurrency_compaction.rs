use candystore::{CandyStore, Config};
use rand::prelude::*;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_concurrency_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap().to_string();

    let config = Config {
        max_data_file_size: 64 * 1024,       // 64KB files - very small
        compaction_min_file_size: 1024,      // Compact even small files
        compaction_min_waste_threshold: 0.2, // 20% waste triggers compaction
        compaction_interval: Duration::from_millis(10), // Check often
        max_concurrency: 8,
        ..Default::default()
    };

    let store = Arc::new(CandyStore::open(&dir_path, config).unwrap());
    let num_threads = 8;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    // We want to generate a lot of waste.
    // Each thread will pick a small set of keys and update them repeatedly.
    for i in 0..num_threads {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let mut rng = rand::rng();
            barrier.wait();

            // 100 keys per thread
            let keys: Vec<String> = (0..100).map(|k| format!("key:{}:{}", i, k)).collect();
            let val = vec![0u8; 100]; // 100 bytes value

            // 1000 iterations of updates
            for _ in 0..1000 {
                let key = keys.choose(&mut rng).unwrap();
                store.set(key.as_bytes(), &val).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Wait a bit for any pending compactions (though they run in background)
    thread::sleep(Duration::from_secs(1));

    let stats = store.stats();
    println!("Stats: {:?}", stats);

    // We expect compactions to have happened because we overwrote keys 1000 times
    // but only have 100 keys per thread. That's 90% waste potential.
    assert!(stats.num_compactions > 0, "Expected compactions to occur");

    // Verify data integrity
    for i in 0..num_threads {
        for k in 0..100 {
            let key = format!("key:{}:{}", i, k);
            let val = store.get(key.as_bytes()).unwrap();
            assert!(val.is_some(), "Key {} missing", key);
            assert_eq!(val.unwrap().len(), 100);
        }
    }
}
