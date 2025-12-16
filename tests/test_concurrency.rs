use candystore::{CandyStore, Config};
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

#[cfg(debug_assertions)]
const NUM_THREADS: usize = 20;

#[cfg(not(debug_assertions))]
const NUM_THREADS: usize = 100;

#[cfg(debug_assertions)]
const NUM_KEYS: usize = 10_000;

#[cfg(not(debug_assertions))]
const NUM_KEYS: usize = 50_000;

const NUM_SHARED_KEYS: usize = NUM_KEYS / 10; // 10%
const NUM_PRIVATE_KEYS: usize = NUM_KEYS - NUM_SHARED_KEYS;
const ITERATIONS: usize = 3; // Number of passes over the keys

#[test]
fn test_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap().to_string();

    let config = Config {
        max_data_file_size: 1024 * 1024, // Small size to force rotations
        initial_capacity: 1024,          // Initial capacity
        max_concurrency: 16,
        ..Default::default()
    };

    let store = Arc::new(CandyStore::open(&dir_path, config).unwrap());
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let store = store.clone();
        let barrier = barrier.clone();

        handles.push(thread::spawn(move || {
            let mut rng = rand::rng();
            let mut expected_state: HashMap<String, Option<String>> = HashMap::new();

            // Generate keys
            let mut keys = Vec::new();

            // Shared keys
            for i in 0..NUM_SHARED_KEYS {
                keys.push((format!("shared:{:06}", i), true));
            }

            // Private keys
            for i in 0..NUM_PRIVATE_KEYS {
                let key = format!("private:{:02}:{:06}", thread_id, i);
                keys.push((key.clone(), false));
                expected_state.insert(key, None);
            }

            barrier.wait();

            for _ in 0..ITERATIONS {
                keys.shuffle(&mut rng);

                for (key, is_shared) in &keys {
                    let op = rng.random_range(0..3); // 0: Get, 1: Set, 2: Remove

                    match op {
                        0 => {
                            // GET
                            let result = store.get(key.as_bytes()).unwrap();
                            if !is_shared {
                                let expected = expected_state.get(key).unwrap();
                                match (result, expected) {
                                    (Some(val), Some(exp_val)) => {
                                        assert_eq!(
                                            val,
                                            exp_val.as_bytes(),
                                            "Value mismatch for key {}",
                                            key
                                        );
                                    }
                                    (None, None) => {}
                                    (Some(val), None) => {
                                        panic!(
                                            "Found value {:?} for key {} but expected None",
                                            String::from_utf8_lossy(&val),
                                            key
                                        );
                                    }
                                    (None, Some(exp_val)) => {
                                        panic!(
                                            "Found None for key {} but expected {:?}",
                                            key, exp_val
                                        );
                                    }
                                }
                            }
                        }
                        1 => {
                            // SET
                            let val = format!("val:{}:{}:{}", thread_id, key, rng.random::<u32>());
                            store.set(key.as_bytes(), val.as_bytes()).unwrap();
                            if !is_shared {
                                expected_state.insert(key.clone(), Some(val));
                            }
                        }
                        2 => {
                            // REMOVE
                            store.remove(key.as_bytes()).unwrap();
                            if !is_shared {
                                expected_state.insert(key.clone(), None);
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Final verification of private keys
    // We can't easily verify shared keys because we don't know who won the last write.
    // But we can verify the store is consistent (e.g. stats).

    let stats = store.stats();
    println!("Final Stats: {:?}", stats);
}
