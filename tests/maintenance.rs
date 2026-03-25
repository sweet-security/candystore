mod common;

use std::fs;

use candystore::{CandyStore, Config, Error};
use tempfile::tempdir;

#[test]
fn test_clear_resets_store_files_and_contents() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    let db = CandyStore::open(dir.path(), config)?;

    for i in 0..100 {
        db.set(format!("key{i:04}"), vec![b'x'; 64])?;
    }
    for i in 0..50 {
        db.set(format!("key{i:04}"), vec![b'y'; 64])?;
    }
    for i in 50..75 {
        db.remove(format!("key{i:04}"))?;
    }

    let data_files_before = std::fs::read_dir(dir.path())
        .map_err(Error::IOError)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("data_"))
        })
        .count();
    assert!(data_files_before > 1);

    fs::write(dir.path().join("extra.txt"), b"junk").map_err(Error::IOError)?;
    fs::create_dir(dir.path().join("extra_dir")).map_err(Error::IOError)?;
    fs::write(dir.path().join("extra_dir").join("nested.txt"), b"junk").map_err(Error::IOError)?;

    db.clear()?;

    assert!(db.get("key0000")?.is_none());
    assert_eq!(db.iter_items().count(), 0);
    assert!(!dir.path().join("extra.txt").exists());
    assert!(!dir.path().join("extra_dir").exists());

    let data_files_after = std::fs::read_dir(dir.path())
        .map_err(Error::IOError)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("data_"))
        })
        .count();
    assert_eq!(data_files_after, 1);

    db.set("fresh", "value")?;
    assert_eq!(db.get("fresh")?, Some(b"value".to_vec()));
    drop(db);

    let reopened = CandyStore::open(dir.path(), config)?;
    assert!(reopened.get("key0000")?.is_none());
    assert_eq!(reopened.get("fresh")?, Some(b"value".to_vec()));

    Ok(())
}

#[test]
fn test_explicit_close_releases_lock_and_persists_clean_shutdown() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    let db = CandyStore::open(dir.path(), config)?;
    db.set("key", "value")?;
    drop(db);

    let reopened = CandyStore::open(dir.path(), config)?;
    assert!(reopened.was_clean_shutdown());
    assert_eq!(reopened.get("key")?, Some(b"value".to_vec()));

    Ok(())
}
