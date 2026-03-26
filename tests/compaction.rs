mod common;

use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};
use std::thread;

use candystore::{CandyStore, Config, Error};
use tempfile::tempdir;

#[test]
fn test_background_compaction() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    let config = Config {
        max_data_file_size: 1024,
        compaction_min_threshold: 256,
        ..Config::default()
    };

    let db = CandyStore::open(dir.path(), config)?;

    let data_files = || -> BTreeSet<String> {
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| {
                entry
                    .ok()
                    .and_then(|entry| entry.file_name().into_string().ok())
                    .filter(|name| name.starts_with("data_"))
            })
            .collect()
    };

    for i in 0..100 {
        db.set(format!("key{i:04}"), format!("value{i:04}"))?;
    }

    let initial_files = data_files();
    assert!(initial_files.len() > 1, "should have multiple data files");

    for i in 0..100 {
        db.set(format!("key{i:04}"), format!("updated{i:04}"))?;
    }

    let mut files_after = data_files();
    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        files_after = data_files();
        if initial_files.iter().any(|file| !files_after.contains(file)) {
            break;
        }
    }

    assert!(
        initial_files.iter().any(|file| !files_after.contains(file)),
        "compaction should have removed at least one initial data file: initial={initial_files:?}, current={files_after:?}"
    );

    for i in 0..100 {
        let key = format!("key{i:04}");
        let expected = format!("updated{i:04}");
        assert_eq!(
            db.get(&key)?,
            Some(expected.into_bytes()),
            "key {key} should have updated value after compaction"
        );
    }

    Ok(())
}

#[test]
fn test_background_compaction_after_reopen_without_writes() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    let config = Config {
        max_data_file_size: 1024,
        compaction_min_threshold: 256,
        ..Config::default()
    };
    let write_config = Config {
        compaction_throughput_bytes_per_sec: 0,
        ..config
    };

    let files_before;
    {
        let db = CandyStore::open(dir.path(), write_config)?;

        for i in 0..200 {
            db.set(format!("key{i:04}"), vec![b'a'; 64])?;
        }

        for i in 0..200 {
            assert_eq!(db.remove(format!("key{i:04}"))?, Some(vec![b'a'; 64]));
        }

        files_before = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("data_"))
            })
            .count();
        assert!(
            files_before > 1,
            "expected multiple data files before close"
        );
    }

    let count_data_files = || -> usize {
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|e| e.file_name().to_str().map(|s| s.starts_with("data_")))
                    .unwrap_or(false)
            })
            .count()
    };

    let db = CandyStore::open(dir.path(), config)?;

    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        if count_data_files() < files_before {
            break;
        }
    }

    let files_after = count_data_files();
    assert!(
        files_after < files_before,
        "reopened store should compact without new writes: before={files_before}, after={files_after}"
    );

    for i in 0..200 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            None,
            "key{i:04} should remain deleted"
        );
    }

    Ok(())
}

#[test]
fn test_background_compaction_drains_large_backlog() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    let write_config = Config {
        // Keep one value per file while leaving each stale file below the setup-time
        // compaction threshold so backlog creation does not race the background worker.
        max_data_file_size: 200,
        compaction_min_threshold: 160,
        ..Config::default()
    };

    let compact_config = Config {
        max_data_file_size: write_config.max_data_file_size,
        compaction_min_threshold: 64,
        ..Config::default()
    };

    const NUM_KEYS: usize = 64;

    {
        let db = CandyStore::open(dir.path(), write_config)?;

        for i in 0..NUM_KEYS {
            db.set(format!("key{i:04}"), vec![b'a'; 96])?;
        }

        for i in 0..NUM_KEYS {
            assert_eq!(db.remove(format!("key{i:04}"))?, Some(vec![b'a'; 96]));
        }
    }

    let count_data_files = || -> usize {
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|e| e.file_name().to_str().map(|s| s.starts_with("data_")))
                    .unwrap_or(false)
            })
            .count()
    };

    let setup_files = count_data_files();
    assert!(
        setup_files >= NUM_KEYS,
        "expected backlog setup to create many stale files: {setup_files}"
    );

    let db = CandyStore::open(dir.path(), compact_config)?;

    let files_before = count_data_files();
    assert!(
        files_before >= setup_files.saturating_sub(2),
        "expected reopen to begin with nearly the full stale-file backlog: setup={setup_files}, before={files_before}"
    );

    let min_expected_drained = (setup_files / 2).max(8);

    for _ in 0..300 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        if count_data_files() + min_expected_drained <= files_before {
            break;
        }
    }

    let files_after = count_data_files();
    assert!(
        files_after + min_expected_drained <= files_before,
        "compaction worker should drain a large backlog after being woken: before={files_before}, after={files_after}, expected_drain={min_expected_drained}"
    );

    for i in 0..NUM_KEYS {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            None,
            "key{i:04} should remain deleted"
        );
    }

    Ok(())
}

#[test]
fn test_compaction_updates_reclaimed_bytes() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    let config = Config {
        max_data_file_size: 1024,
        compaction_min_threshold: 256,
        ..Config::default()
    };

    let db = CandyStore::open(dir.path(), config)?;

    for i in 0..100 {
        db.set(format!("key{i:04}"), format!("value{i:04}"))?;
    }

    // Update all keys to generate waste
    for i in 0..100 {
        db.set(format!("key{i:04}"), format!("updated{i:04}"))?;
    }

    // Wait for compaction to run
    for _ in 0..200 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        if db.stats().num_compactions > 0 {
            break;
        }
    }

    let stats = db.stats();
    assert!(
        stats.num_compactions > 0,
        "compaction should have run at least once"
    );
    assert!(
        stats.reclaimed_bytes > 0,
        "reclaimed_bytes should be positive after compaction"
    );
    assert!(
        stats.waste_bytes > 0,
        "waste_bytes must be positive (total waste ever generated)"
    );

    for i in 0..100 {
        let key = format!("key{i:04}");
        let expected = format!("updated{i:04}");
        assert_eq!(db.get(&key)?, Some(expected.into_bytes()));
    }

    Ok(())
}

#[test]
fn test_concurrent_updates_with_compaction() -> Result<(), Error> {
    const THREADS: usize = 8;
    const KEYS: usize = 200;
    const ROUNDS: usize = 20;

    let dir = tempdir().unwrap();

    let config = Config {
        max_data_file_size: 2048,
        compaction_min_threshold: 512,
        ..Config::default()
    };

    let db = Arc::new(CandyStore::open(dir.path(), config)?);

    // Seed initial keys
    for i in 0..KEYS {
        db.set(format!("key{i:04}"), format!("v0_{i:04}"))?;
    }

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for round in 0..ROUNDS {
                    for i in 0..KEYS {
                        let key = format!("key{i:04}");
                        let val = format!("v{round}_{t}_{i:04}");
                        db.set(&key, &val).unwrap();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Give compaction time to finish remaining work
    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    // All keys should still be readable
    for i in 0..KEYS {
        let key = format!("key{i:04}");
        assert!(db.get(&key)?.is_some(), "key {key} should exist");
    }

    let stats = db.stats();
    assert!(
        stats.num_compactions > 0,
        "compaction should have run during concurrent updates"
    );
    assert!(
        stats.reclaimed_bytes > 0,
        "reclaimed_bytes should be positive after concurrent updates + compaction"
    );

    Ok(())
}

#[test]
fn test_concurrent_removes_trigger_compaction() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    let config = Config {
        max_data_file_size: 1024,
        compaction_min_threshold: 256,
        ..Config::default()
    };

    let db = Arc::new(CandyStore::open(dir.path(), config)?);

    // Create keys spread across many files
    for i in 0..300 {
        db.set(format!("key{i:04}"), vec![b'x'; 64])?;
    }

    let files_before = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|s| s.starts_with("data_"))
        })
        .count();
    assert!(files_before > 2);

    // Remove all keys concurrently — tombstone waste should trigger compaction
    const THREADS: usize = 8;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for i in (t..300).step_by(THREADS) {
                    let _ = db.remove(format!("key{i:04}")).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Wait for compaction
    for _ in 0..200 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let files_now = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("data_"))
            })
            .count();
        if files_now < files_before {
            break;
        }
    }

    let files_after = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|s| s.starts_with("data_"))
        })
        .count();
    assert!(
        files_after < files_before,
        "compaction should remove files after concurrent removes: before={files_before}, after={files_after}"
    );

    for i in 0..300 {
        assert_eq!(db.get(format!("key{i:04}"))?, None);
    }

    Ok(())
}
