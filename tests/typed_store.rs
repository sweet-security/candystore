use std::sync::Arc;

use candystore::{CandyStore, CandyTypedStore, Config, MAX_USER_VALUE_SIZE, Result};
use tempfile::TempDir;

#[test]
fn test_typed_kv_store() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let kv = CandyTypedStore::<u32, String>::new(Arc::clone(&store));

    assert!(!kv.contains(&1u32)?);
    assert!(kv.set(&1u32, &"one".to_string())?.is_none());
    assert!(kv.contains(&1u32)?);
    assert_eq!(kv.get(&1u32)?, Some("one".to_string()));
    assert_eq!(kv.set(&1u32, &"uno".to_string())?, Some("one".to_string()));
    assert_eq!(kv.remove(&1u32)?, Some("uno".to_string()));
    assert!(kv.get(&1u32)?.is_none());
    assert!(!kv.contains(&1u32)?);
    Ok(())
}

#[test]
fn test_typed_atomic_ops() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let kv = CandyTypedStore::<u32, String>::new(Arc::clone(&store));

    assert_eq!(
        kv.get_or_create(&7u32, &"seven".to_string())?,
        "seven".to_string()
    );
    assert_eq!(
        kv.get_or_create(&7u32, &"changed".to_string())?,
        "seven".to_string()
    );
    assert_eq!(kv.get(&7u32)?, Some("seven".to_string()));
    assert!(
        kv.replace(&8u32, &"nope".to_string(), None::<&String>)?
            .is_none()
    );
    assert!(kv.get(&8u32)?.is_none());
    assert!(
        kv.replace(&7u32, &"nope".to_string(), Some(&"wrong".to_string()))?
            .is_none()
    );
    assert_eq!(kv.get(&7u32)?, Some("seven".to_string()));
    assert_eq!(
        kv.replace(&7u32, &"new".to_string(), None::<&String>)?,
        Some("seven".to_string())
    );
    assert_eq!(kv.get(&7u32)?, Some("new".to_string()));

    Ok(())
}

#[test]
fn test_typed_big_value_round_trip() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let kv = CandyTypedStore::<u32, Vec<u8>>::new(Arc::clone(&store));

    let key = 3u32;
    let big1 = vec![1u8; MAX_USER_VALUE_SIZE + 123];
    let big2 = vec![2u8; MAX_USER_VALUE_SIZE * 2 + 17];

    assert!(!kv.set_big(&key, &big1)?);
    assert_eq!(kv.get_big(&key)?, Some(big1.clone()));
    assert!(kv.set_big(&key, &big2)?);
    assert_eq!(kv.get_big(&key)?, Some(big2.clone()));
    assert!(kv.remove_big(&key)?);
    assert!(kv.get_big(&key)?.is_none());
    assert!(!kv.remove_big(&key)?);

    Ok(())
}
