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

    {
        let db = CandyStore::open(dir.path(), config)?;

        for i in 0..200 {
            db.set(format!("key{i:04}"), vec![b'a'; 64])?;
        }

        for i in 0..200 {
            assert_eq!(db.remove(format!("key{i:04}"))?, Some(vec![b'a'; 64]));
        }

        assert!(
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e
                    .file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("data_")))
                .count()
                > 1,
            "expected multiple data files before reopen"
        );
    }

    let db = CandyStore::open(dir.path(), config)?;

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

    let files_before = count_data_files();
    assert!(files_before > 1, "expected compaction backlog after reopen");

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

    let config = Config {
        max_data_file_size: 256,
        compaction_min_threshold: 128,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;

        for i in 0..180 {
            db.set(format!("key{i:04}"), vec![b'a'; 96])?;
        }

        for i in 0..180 {
            assert_eq!(db.remove(format!("key{i:04}"))?, Some(vec![b'a'; 96]));
        }
    }

    let db = CandyStore::open(dir.path(), config)?;

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

    let files_before = count_data_files();
    assert!(
        files_before > 17,
        "expected a large stale-file backlog before compaction starts: {files_before}"
    );

    for _ in 0..300 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        if count_data_files() <= files_before.saturating_sub(17) {
            break;
        }
    }

    let files_after = count_data_files();
    assert!(
        files_after <= files_before.saturating_sub(17),
        "compaction worker should drain a large backlog after being woken: before={files_before}, after={files_after}"
    );

    for i in 0..180 {
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
