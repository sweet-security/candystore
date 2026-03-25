mod common;

use candystore::{
    CandyStore, Config, Error, GetOrCreateStatus, MAX_KEY_LEN, MAX_VALUE_LEN, ReplaceStatus,
    SetStatus,
};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_basic() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    assert!(db.get("hello")?.is_none());

    assert!(matches!(db.set("hello", "world")?, SetStatus::CreatedNew));
    assert_eq!(db.get("hello")?, Some("world".into()));

    assert!(
        matches!(db.set("hello", "earth")?, SetStatus::PrevValue(ref value) if value == b"world")
    );
    assert_eq!(db.get("hello")?, Some("earth".into()));

    assert_eq!(db.remove("hello")?, Some("earth".into()));
    assert!(db.get("hello")?.is_none());
    assert!(db.remove("hello")?.is_none());

    Ok(())
}

#[test]
fn test_reopen_existing_db() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        assert!(matches!(db.set("hello", "world")?, SetStatus::CreatedNew));
        assert!(matches!(db.set("goodbye", "earth")?, SetStatus::CreatedNew));
    }

    let db = CandyStore::open(dir.path(), Config::default())?;
    assert_eq!(db.get("hello")?, Some("world".into()));
    assert_eq!(db.get("goodbye")?, Some("earth".into()));

    Ok(())
}

#[test]
fn test_reopen_with_different_hash_key_uses_persisted_key() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    let original_config = Config {
        hash_key: (1, 2),
        ..Config::default()
    };
    let different_config = Config {
        hash_key: (3, 4),
        ..original_config
    };

    {
        let db = CandyStore::open(dir.path(), original_config)?;
        db.set("hello", "world")?;
    }

    let db = CandyStore::open(dir.path(), different_config)?;
    assert_eq!(db.get("hello")?, Some("world".into()));
    db.set("goodbye", "earth")?;
    drop(db);

    let db = CandyStore::open(dir.path(), original_config)?;
    assert_eq!(db.get("hello")?, Some("world".into()));
    assert_eq!(db.get("goodbye")?, Some("earth".into()));

    Ok(())
}
#[test]
fn test_oversized_value_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = candystore::Config {
        max_data_file_size: 1024 * 1024,
        ..Default::default()
    };
    let db = candystore::CandyStore::open(dir.path(), config).unwrap();

    let large_value = vec![0u8; 2 * 1024 * 1024]; // 2MB

    // Should gracefully reject the oversized key/value pair
    let result = db.set("key", &large_value);
    assert!(result.is_err());
}

#[test]
fn test_max_key_len() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let key = vec![b'k'; MAX_KEY_LEN];
    db.set(&key, b"value")?;
    assert_eq!(db.get(&key)?, Some(b"value".to_vec()));

    Ok(())
}

#[test]
fn test_key_too_long() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let key = vec![b'k'; MAX_KEY_LEN + 1];
    assert!(db.set(&key, b"value").is_err());

    Ok(())
}

#[test]
fn test_max_value_len() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let value = vec![b'v'; MAX_VALUE_LEN];
    db.set(b"key", &value)?;
    assert_eq!(db.get(b"key")?, Some(value));

    Ok(())
}

#[test]
fn test_value_too_long() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let value = vec![b'v'; MAX_VALUE_LEN + 1];
    assert!(db.set(b"key", &value).is_err());

    Ok(())
}

#[test]
fn test_empty_key_and_value() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    db.set(b"", b"value")?;
    assert_eq!(db.get(b"")?, Some(b"value".to_vec()));

    db.set(b"empty", b"")?;
    assert_eq!(db.get(b"empty")?, Some(Vec::new()));

    Ok(())
}

#[test]
fn test_get_or_create_and_replace() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let created = db.get_or_create("hello", "world")?;
    assert!(matches!(created, GetOrCreateStatus::CreatedNew(ref value) if value == b"world"));

    let existing = db.get_or_create("hello", "other")?;
    assert!(matches!(existing, GetOrCreateStatus::ExistingValue(ref value) if value == b"world"));

    let wrong = db.replace("hello", "earth", Some(&"wrong"))?;
    assert!(matches!(wrong, ReplaceStatus::WrongValue(ref value) if value == b"world"));
    assert_eq!(db.get("hello")?, Some(b"world".to_vec()));

    let replaced = db.replace("hello", "earth", Some(&"world"))?;
    assert!(matches!(replaced, ReplaceStatus::PrevValue(ref value) if value == b"world"));
    assert_eq!(db.get("hello")?, Some(b"earth".to_vec()));

    let missing = db.replace("missing", "value", Option::<&str>::None)?;
    assert!(matches!(missing, ReplaceStatus::DoesNotExist));

    Ok(())
}

#[test]
fn test_get_or_create_is_atomic_under_contention() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default())?);
    let barrier = Arc::new(Barrier::new(8));

    let mut handles = Vec::new();
    for idx in 0..8 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let value = format!("value-{idx}");
            barrier.wait();
            db.get_or_create("shared", &value).unwrap()
        }));
    }

    let mut created_values = Vec::new();
    let mut seen_values = Vec::new();
    for handle in handles {
        match handle.join().unwrap() {
            GetOrCreateStatus::CreatedNew(value) => created_values.push(value),
            GetOrCreateStatus::ExistingValue(value) => seen_values.push(value),
        }
    }

    assert_eq!(created_values.len(), 1);
    let winning_value = created_values.pop().unwrap();
    assert_eq!(db.get("shared")?, Some(winning_value.clone()));
    assert!(seen_values.into_iter().all(|value| value == winning_value));

    Ok(())
}
