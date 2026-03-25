mod common;

use std::collections::HashMap;

use candystore::{CandyStore, Config, Error};
use tempfile::tempdir;

#[test]
fn test_iter_items_empty_db() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let items: Vec<_> = db.iter_items().collect::<Result<_, _>>()?;
    assert!(items.is_empty());

    Ok(())
}

#[test]
fn test_iter_items_basic() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let mut expected = HashMap::new();
    for i in 0..100 {
        let key = format!("iter_key_{i:04}");
        let value = format!("iter_val_{i:04}");
        db.set(&key, &value)?;
        expected.insert(key.into_bytes(), value.into_bytes());
    }

    let items: HashMap<Vec<u8>, Vec<u8>> = db.iter_items().collect::<Result<_, _>>()?;
    assert_eq!(items.len(), expected.len());
    assert_eq!(items, expected);

    Ok(())
}

#[test]
fn test_iter_items_after_updates_and_removes() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    for i in 0..50 {
        db.set(format!("key_{i:04}"), format!("val_{i:04}"))?;
    }

    for i in 0..20 {
        db.set(format!("key_{i:04}"), format!("updated_{i:04}"))?;
    }

    for i in 40..50 {
        db.remove(format!("key_{i:04}"))?;
    }

    let items: HashMap<Vec<u8>, Vec<u8>> = db.iter_items().collect::<Result<_, _>>()?;
    assert_eq!(items.len(), 40);

    for i in 0..20 {
        let key = format!("key_{i:04}");
        assert_eq!(
            items.get(key.as_bytes()),
            Some(&format!("updated_{i:04}").into_bytes())
        );
    }
    for i in 20..40 {
        let key = format!("key_{i:04}");
        assert_eq!(
            items.get(key.as_bytes()),
            Some(&format!("val_{i:04}").into_bytes())
        );
    }
    for i in 40..50 {
        let key = format!("key_{i:04}");
        assert!(!items.contains_key(key.as_bytes()));
    }

    Ok(())
}

#[test]
fn test_iter_items_with_splits_and_rotation() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), common::small_file_config())?;

    let mut expected = HashMap::new();
    for i in 0..2000 {
        let key = format!("split_iter_key_{i:05}");
        let value = format!("split_iter_val_{i:05}_{}", "x".repeat(48));
        db.set(&key, &value)?;
        expected.insert(key.into_bytes(), value.into_bytes());
    }

    let items: HashMap<Vec<u8>, Vec<u8>> = db.iter_items().collect::<Result<_, _>>()?;
    assert_eq!(items.len(), expected.len());
    assert_eq!(items, expected);

    Ok(())
}
