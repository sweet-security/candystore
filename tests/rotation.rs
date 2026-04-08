mod common;

use candystore::{CandyStore, Config, Error, SetStatus};
use tempfile::tempdir;

#[test]
fn test_many_inserts_trigger_splits() -> Result<(), Error> {
    const KEYS: usize = 5000;

    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    for key_idx in 0..KEYS {
        let key = format!("split_key_{key_idx:05}");
        let value = format!("split_val_{key_idx:05}");
        assert!(matches!(db.set(&key, &value)?, SetStatus::CreatedNew));
    }

    for key_idx in 0..KEYS {
        let key = format!("split_key_{key_idx:05}");
        let value = format!("split_val_{key_idx:05}");
        assert_eq!(db.get(&key)?, Some(value.into()));
    }

    Ok(())
}

#[test]
fn test_rotation_preserves_reads() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), common::small_file_config())?;

    for key_idx in 0..512 {
        let key = format!("rotate_key_{key_idx:04}");
        let value = format!("rotate_val_{key_idx:04}_{}", "x".repeat(64));
        assert!(matches!(db.set(&key, &value)?, SetStatus::CreatedNew));
    }

    for key_idx in 0..512 {
        let key = format!("rotate_key_{key_idx:04}");
        assert!(db.get(&key)?.is_some(), "missing key after rotation: {key}");
    }

    Ok(())
}

#[test]
fn test_new_data_files_are_preallocated() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::small_file_config();
    let db = CandyStore::open(dir.path(), config)?;

    let file_len = std::fs::metadata(dir.path().join("data_0000"))
        .map_err(Error::IOError)?
        .len();
    assert_eq!(file_len, 4096 + config.max_data_file_size as u64);

    db.set("prealloc", [7u8; 128])?;
    drop(db);

    let reopened_len = std::fs::metadata(dir.path().join("data_0000"))
        .map_err(Error::IOError)?
        .len();
    assert_eq!(reopened_len, 4096 + config.max_data_file_size as u64);

    Ok(())
}

#[test]
fn test_splits_and_rotation_with_small_files() -> Result<(), Error> {
    const KEYS: usize = 5000;

    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), common::small_file_config())?;

    for key_idx in 0..KEYS {
        let key = format!("split_rotate_key_{key_idx:05}");
        let value = format!("split_rotate_val_{key_idx:05}_{}", "x".repeat(48));
        assert!(matches!(db.set(&key, &value)?, SetStatus::CreatedNew));
    }

    for key_idx in 0..KEYS {
        let key = format!("split_rotate_key_{key_idx:05}");
        let value = format!("split_rotate_val_{key_idx:05}_{}", "x".repeat(48));
        assert_eq!(db.get(&key)?, Some(value.into()));
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
        "expected rotation to create multiple data files"
    );

    let index_rows_size = std::fs::metadata(dir.path().join("rows"))
        .map_err(Error::IOError)?
        .len();
    assert!(
        index_rows_size > 4 * 4096,
        "expected index rows growth after row splitting, got rows size {index_rows_size}"
    );

    Ok(())
}

#[test]
fn test_splits_rotation_and_reopen_with_small_files() -> Result<(), Error> {
    const KEYS: usize = 5000;

    let dir = tempdir().unwrap();

    {
        let db = CandyStore::open(dir.path(), common::small_file_config())?;
        for key_idx in 0..KEYS {
            let key = format!("reopen_split_rotate_key_{key_idx:05}");
            let value = format!("reopen_split_rotate_val_{key_idx:05}_{}", "x".repeat(48));
            assert!(matches!(db.set(&key, &value)?, SetStatus::CreatedNew));
        }
    }

    let db = CandyStore::open(dir.path(), common::small_file_config())?;
    for key_idx in 0..KEYS {
        let key = format!("reopen_split_rotate_key_{key_idx:05}");
        let value = format!("reopen_split_rotate_val_{key_idx:05}_{}", "x".repeat(48));
        assert_eq!(db.get(&key)?, Some(value.into()));
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
        "expected rotation to persist multiple data files after reopen"
    );

    Ok(())
}
