use std::sync::Arc;

use candystore::{
    CandyStore, CandyTypedDeque, CandyTypedList, CandyTypedStore, Config, ListCompactionParams,
    MAX_VALUE_LEN, Result,
};
use tempfile::TempDir;

#[test]
fn test_typed_kv_store() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let kv = CandyTypedStore::<u32, String>::new(store.clone());

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
    let kv = CandyTypedStore::<u32, String>::new(store.clone());

    let got = kv.get_or_create(&7u32, &"seven".to_string())?;
    assert_eq!(got, "seven".to_string());

    // existing value returned unchanged
    let got_again = kv.get_or_create(&7u32, &"changed".to_string())?;
    assert_eq!(got_again, "seven".to_string());
    assert_eq!(kv.get(&7u32)?, Some("seven".to_string()));

    // replace succeeds only when present
    let missing = kv.replace(&8u32, &"nope".to_string(), None)?;
    assert!(missing.is_none());
    assert!(kv.get(&8u32)?.is_none());

    let replaced = kv.replace(&7u32, &"new".to_string(), None)?;
    assert_eq!(replaced, Some("seven".to_string()));
    assert_eq!(kv.get(&7u32)?, Some("new".to_string()));

    Ok(())
}

#[test]
fn test_typed_big_value_round_trip() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let kv = CandyTypedStore::<u32, Vec<u8>>::new(store.clone());

    let key = 3u32;
    let big1 = vec![1u8; MAX_VALUE_LEN + 123];
    let big2 = vec![2u8; MAX_VALUE_LEN * 2 + 17];

    assert!(!kv.set_big(&key, &big1)?);
    assert_eq!(kv.get_big(&key)?, Some(big1.clone()));

    assert!(kv.set_big(&key, &big2)?);
    assert_eq!(kv.get_big(&key)?, Some(big2.clone()));

    assert!(kv.remove_big(&key)?);
    assert!(kv.get_big(&key)?.is_none());
    assert!(!kv.remove_big(&key)?);

    Ok(())
}

#[test]
fn test_typed_queue_iter_rev() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let queue = CandyTypedDeque::<u32, u32>::new(store.clone());

    let qkey = 7u32;
    queue.push_tail(&qkey, &10u32)?;
    queue.push_tail(&qkey, &20u32)?;
    queue.push_tail(&qkey, &30u32)?;

    let fwd: Vec<_> = queue.iter(&qkey).map(|r| r.unwrap().1).collect();
    assert_eq!(fwd, vec![10, 20, 30]);

    let mut rev = fwd.clone();
    rev.reverse();
    assert_eq!(rev, vec![30, 20, 10]);

    Ok(())
}

#[test]
fn test_typed_queue_discard() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let queue = CandyTypedDeque::<u32, u32>::new(store.clone());

    let qkey = 5u32;
    queue.push_tail(&qkey, &1u32)?;
    queue.push_tail(&qkey, &2u32)?;
    assert_eq!(queue.len(&qkey)?, 2);

    assert!(queue.discard(&qkey)?);
    assert_eq!(queue.len(&qkey)?, 0);
    assert!(queue.pop_head(&qkey)?.is_none());
    Ok(())
}

#[test]
fn test_typed_list_iter_rev() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(store.clone());

    let lkey = 42u32;
    list.set(&lkey, &1u32, &"a".to_string())?;
    list.set(&lkey, &2u32, &"b".to_string())?;
    list.set(&lkey, &3u32, &"c".to_string())?;

    // Remove middle to leave a hole
    assert_eq!(list.remove(&lkey, &2u32)?, Some("b".to_string()));

    let fwd: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(fwd, vec![(1u32, "a".to_string()), (3u32, "c".to_string())]);

    let rev: Vec<_> = list.iter(&lkey).rev().map(|r| r.unwrap()).collect();
    assert_eq!(rev, vec![(3u32, "c".to_string()), (1u32, "a".to_string())]);

    Ok(())
}

#[test]
fn test_typed_list_admin_ops() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(store.clone());

    let lkey = 9u32;
    list.set(&lkey, &1u32, &"a".to_string())?;
    list.set(&lkey, &2u32, &"b".to_string())?;
    list.set(&lkey, &3u32, &"c".to_string())?;
    assert_eq!(list.len(&lkey)?, 3);

    // Move middle to head
    assert!(list.set_promoting(&lkey, &2u32)?);
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (2u32, "b".to_string()),
            (1u32, "a".to_string()),
            (3u32, "c".to_string())
        ]
    );

    // Move first to tail
    let existing = list.remove(&lkey, &1u32)?.unwrap();
    list.set(&lkey, &1u32, &existing)?;
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (2u32, "b".to_string()),
            (3u32, "c".to_string()),
            (1u32, "a".to_string())
        ]
    );

    // Remove one to create a hole, then compact
    assert_eq!(list.remove(&lkey, &3u32)?, Some("c".to_string()));
    let range_before = list.range(&lkey)?;
    assert!(!range_before.is_empty());
    assert_eq!(list.len(&lkey)?, 2);

    assert!(list.compact_if_needed(
        &lkey,
        ListCompactionParams {
            min_length: 0,
            min_holes_ratio: 0.0
        }
    )?);
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![(2u32, "b".to_string()), (1u32, "a".to_string())]
    );

    // Discard clears the list and span should be None
    assert!(list.discard(&lkey)?);
    assert!(list.is_empty(&lkey)?);
    assert!(list.range(&lkey)?.is_empty());
    Ok(())
}

#[test]
fn test_typed_list_retain() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(store.clone());

    let lkey = 1u32;
    list.set(&lkey, &10, &"ten".to_string())?;
    list.set(&lkey, &20, &"twenty".to_string())?;
    list.set(&lkey, &30, &"thirty".to_string())?;
    list.set(&lkey, &40, &"forty".to_string())?;

    // Retain only keys divisible by 20
    list.retain(&lkey, |k, _v| Ok(k % 20 == 0))?;

    let items: Vec<_> = list.iter(&lkey).map(|r| r.unwrap().0).collect();
    assert_eq!(items, vec![20, 40]);

    // Retain only values starting with "f"
    list.retain(&lkey, |_k, v| Ok(v.starts_with("f")))?;
    let items: Vec<_> = list.iter(&lkey).map(|r| r.unwrap().0).collect();
    assert_eq!(items, vec![40]);

    Ok(())
}

#[test]
fn test_typed_list_pop_peek() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(store.clone());
    let lkey = 100u32;

    list.set(&lkey, &1, &"one".to_string())?;
    list.set(&lkey, &2, &"two".to_string())?;

    assert_eq!(list.peek_head(&lkey)?, Some((1, "one".to_string())));
    assert_eq!(list.peek_tail(&lkey)?, Some((2, "two".to_string())));

    assert_eq!(list.pop_head(&lkey)?, Some((1, "one".to_string())));
    assert_eq!(list.len(&lkey)?, 1);

    assert_eq!(list.pop_tail(&lkey)?, Some((2, "two".to_string())));
    assert_eq!(list.len(&lkey)?, 0);

    assert!(list.pop_head(&lkey)?.is_none());

    Ok(())
}
