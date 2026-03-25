mod common;

use std::sync::{Arc, Barrier};
use std::thread;

use candystore::{CandyStore, Config, Error, SetStatus};
use tempfile::tempdir;

#[test]
fn test_multi_threaded_disjoint_writes() -> Result<(), Error> {
    const THREADS: usize = 30;
    const KEYS_PER_THREAD: usize = 10_000;

    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default())?);
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for key_idx in 0..KEYS_PER_THREAD {
                    let key = format!("mt_key_{thread_idx:02}_{key_idx:04}");
                    let value = format!("mt_val_{thread_idx:02}_{key_idx:04}");
                    assert!(matches!(
                        db.set(&key, &value).unwrap(),
                        SetStatus::CreatedNew
                    ));
                    assert_eq!(db.get(&key).unwrap(), Some(value.into()));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    for thread_idx in 0..THREADS {
        for key_idx in 0..KEYS_PER_THREAD {
            let key = format!("mt_key_{thread_idx:02}_{key_idx:04}");
            let value = format!("mt_val_{thread_idx:02}_{key_idx:04}");
            assert_eq!(db.get(&key)?, Some(value.into()));
        }
    }

    Ok(())
}

#[test]
fn test_multi_threaded_reads() -> Result<(), Error> {
    const THREADS: usize = 30;
    const KEYS: usize = 10_000;

    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default())?);

    for key_idx in 0..KEYS {
        let key = format!("read_key_{key_idx:04}");
        let value = format!("read_val_{key_idx:04}");
        assert!(matches!(db.set(&key, &value)?, SetStatus::CreatedNew));
    }

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for key_idx in 0..KEYS {
                    let key = format!("read_key_{key_idx:04}");
                    let value = format!("read_val_{key_idx:04}");
                    assert_eq!(db.get(&key).unwrap(), Some(value.into()));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

#[test]
fn test_multi_threaded_same_key_writes() -> Result<(), Error> {
    const THREADS: usize = 30;

    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default())?);
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let value = format!("same_value_{thread_idx:02}");
                barrier.wait();
                db.set("shared-key", &value).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let final_value = db.get("shared-key")?.expect("value should exist");
    assert!(
        std::str::from_utf8(&final_value)
            .unwrap()
            .starts_with("same_value_")
    );

    Ok(())
}

#[test]
fn test_multi_threaded_same_key_read_write() -> Result<(), Error> {
    const THREADS: usize = 30;
    const WRITES_PER_THREAD: usize = 10_000;

    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default())?);
    assert!(matches!(
        db.set("shared-key", "seed")?,
        SetStatus::CreatedNew
    ));

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for write_idx in 0..WRITES_PER_THREAD {
                    if thread_idx % 2 == 0 {
                        let value = format!("rw_{thread_idx:02}_{write_idx:02}");
                        db.set("shared-key", &value).unwrap();
                    } else {
                        let value = db.get("shared-key").unwrap();
                        assert!(value.is_some());
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(db.get("shared-key")?.is_some());

    Ok(())
}

#[test]
fn test_multi_threaded_writes_with_splits_and_rotation() -> Result<(), Error> {
    const THREADS: usize = 30;
    const KEYS_PER_THREAD: usize = 2_000; // to avoid too many open files in small config

    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), common::small_file_config())?);
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for key_idx in 0..KEYS_PER_THREAD {
                    let key = format!("mt_split_rotate_key_{thread_idx:02}_{key_idx:04}");
                    let value = format!(
                        "mt_split_rotate_val_{thread_idx:02}_{key_idx:04}_{}",
                        "x".repeat(48)
                    );
                    assert!(matches!(
                        db.set(&key, &value).unwrap(),
                        SetStatus::CreatedNew
                    ));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    for thread_idx in 0..THREADS {
        for key_idx in 0..KEYS_PER_THREAD {
            let key = format!("mt_split_rotate_key_{thread_idx:02}_{key_idx:04}");
            let value = format!(
                "mt_split_rotate_val_{thread_idx:02}_{key_idx:04}_{}",
                "x".repeat(48)
            );
            assert_eq!(db.get(&key)?, Some(value.into()));
        }
    }

    let data_file_count = std::fs::read_dir(dir.path())
        .map_err(Error::IOError)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("data_"))
        })
        .count();
    assert!(
        data_file_count > 1,
        "expected concurrent writes to trigger rotation with small files"
    );

    Ok(())
}

fn expected_key(is_shared: bool, thread_idx: usize, key_idx: usize) -> Vec<u8> {
    let mut key = String::new();
    if is_shared {
        key.push_str(&format!("shared_key_{key_idx}"));
    } else {
        key.push_str(&format!("distinct_key_{thread_idx}_{key_idx}"));
    }
    // Mix in some large keys
    if key_idx.is_multiple_of(7) {
        key.push_str(&"K".repeat(150));
    }
    key.into_bytes()
}

fn expected_value(key: &[u8]) -> Vec<u8> {
    let mut val = String::from_utf8_lossy(key).into_owned();
    let length_marker = key.iter().map(|&b| b as usize).sum::<usize>();
    if length_marker % 3 == 0 {
        val.push_str(&"V".repeat(5000));
    } else if length_marker % 5 == 0 {
        val.push_str(&"V".repeat(100)); // Medium
    }
    val.into_bytes()
}

fn pseudo_rand(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    *seed
}

#[test]
fn test_concurrent_mixed_workload() -> Result<(), Error> {
    const THREADS: usize = 30;
    const OPERATIONS_PER_THREAD: usize = 10_000;
    const SHARED_KEYS_TOTAL: usize = 2_000;
    const DISTINCT_KEYS_PER_THREAD: usize = 300; // 30 * 300 = 9000 distinct total

    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default())?);
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|thread_idx| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut seed = (thread_idx as u64 + 1) * 123456789;

                // Track our own distinct keys so we strictly assert them
                let mut distinct_state = vec![false; DISTINCT_KEYS_PER_THREAD];

                barrier.wait();

                for _ in 0..OPERATIONS_PER_THREAD {
                    let r = pseudo_rand(&mut seed);
                    let is_shared = (r % 100) < 50; // 50% operations on shared pool, 50% on distinct

                    let key_idx = if is_shared {
                        (pseudo_rand(&mut seed) as usize) % SHARED_KEYS_TOTAL
                    } else {
                        (pseudo_rand(&mut seed) as usize) % DISTINCT_KEYS_PER_THREAD
                    };

                    let key = expected_key(is_shared, thread_idx, key_idx);
                    let val = expected_value(&key);

                    let op = pseudo_rand(&mut seed) % 100;
                    if op < 40 {
                        // 40% Set
                        db.set(&key, &val).unwrap();
                        if !is_shared {
                            distinct_state[key_idx] = true;
                        }
                    } else if op < 80 {
                        // 40% Get
                        let actual = db.get(&key).unwrap();
                        if is_shared {
                            // Validation: Either exactly the expected value or None!
                            if let Some(v) = actual {
                                assert_eq!(v, val, "Shared key data corrupted!");
                            }
                        } else {
                            if distinct_state[key_idx] {
                                assert_eq!(actual, Some(val), "Distinct key missing!");
                            } else {
                                assert_eq!(actual, None, "Distinct key found but not set!");
                            }
                        }
                    } else {
                        // 20% Remove
                        let _ = db.remove(&key).unwrap();
                        if !is_shared {
                            distinct_state[key_idx] = false;
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}
