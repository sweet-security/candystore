use std::sync::Arc;

use candystore::{CandyStore, CandyTypedList, Config, ListCompactionParams, Result};
use tempfile::TempDir;

#[test]
fn test_typed_list_iter_rev() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(Arc::clone(&store));

    let lkey = 42u32;
    list.set(&lkey, &1u32, &"a".to_string())?;
    list.set(&lkey, &2u32, &"b".to_string())?;
    list.set(&lkey, &3u32, &"c".to_string())?;

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
    let list = CandyTypedList::<u32, u32, String>::new(Arc::clone(&store));

    let lkey = 9u32;
    list.set(&lkey, &1u32, &"a".to_string())?;
    list.set(&lkey, &2u32, &"b".to_string())?;
    list.set(&lkey, &3u32, &"c".to_string())?;
    assert_eq!(list.len(&lkey)?, 3);

    assert_eq!(
        list.set_promoting(&lkey, &2u32, &"b".to_string())?,
        Some("b".to_string())
    );
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (1u32, "a".to_string()),
            (3u32, "c".to_string()),
            (2u32, "b".to_string())
        ]
    );

    assert_eq!(
        list.set_promoting(&lkey, &3u32, &"cc".to_string())?,
        Some("c".to_string())
    );
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (1u32, "a".to_string()),
            (2u32, "b".to_string()),
            (3u32, "cc".to_string())
        ]
    );

    assert_eq!(list.set_promoting(&lkey, &4u32, &"d".to_string())?, None);
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (1u32, "a".to_string()),
            (2u32, "b".to_string()),
            (3u32, "cc".to_string()),
            (4u32, "d".to_string())
        ]
    );

    let existing = list.remove(&lkey, &1u32)?.unwrap();
    list.set(&lkey, &1u32, &existing)?;
    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (2u32, "b".to_string()),
            (3u32, "cc".to_string()),
            (4u32, "d".to_string()),
            (1u32, "a".to_string())
        ]
    );

    assert_eq!(list.remove(&lkey, &3u32)?, Some("cc".to_string()));
    let range_before = list.range(&lkey)?;
    assert!(!range_before.is_empty());
    assert_eq!(list.len(&lkey)?, 3);

    let _ = list.compact_if_needed(
        &lkey,
        ListCompactionParams {
            min_length: 0,
            min_holes_ratio: 0.0,
        },
    )?;

    let order: Vec<_> = list.iter(&lkey).map(|r| r.unwrap()).collect();
    assert_eq!(
        order,
        vec![
            (2u32, "b".to_string()),
            (4u32, "d".to_string()),
            (1u32, "a".to_string())
        ]
    );

    assert!(list.discard(&lkey)?);
    assert!(list.is_empty(&lkey)?);
    assert!(list.range(&lkey)?.is_empty());
    Ok(())
}

#[test]
fn test_typed_list_retain() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(Arc::clone(&store));

    let lkey = 1u32;
    list.set(&lkey, &10u32, &"ten".to_string())?;
    list.set(&lkey, &20u32, &"twenty".to_string())?;
    list.set(&lkey, &30u32, &"thirty".to_string())?;
    list.set(&lkey, &40u32, &"forty".to_string())?;

    list.retain(&lkey, |k, _| Ok(*k % 20 == 0))?;
    let items: Vec<_> = list.iter(&lkey).map(|r| r.unwrap().0).collect();
    assert_eq!(items, vec![20u32, 40u32]);

    list.retain(&lkey, |_, v| Ok(v.starts_with('f')))?;
    let items: Vec<_> = list.iter(&lkey).map(|r| r.unwrap().0).collect();
    assert_eq!(items, vec![40u32]);
    assert_eq!(list.range(&lkey)?.len(), list.len(&lkey)?);

    Ok(())
}

#[test]
fn test_typed_list_pop_peek() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let list = CandyTypedList::<u32, u32, String>::new(Arc::clone(&store));

    let lkey = 100u32;
    list.set(&lkey, &1u32, &"one".to_string())?;
    list.set(&lkey, &2u32, &"two".to_string())?;

    assert_eq!(list.peek_head(&lkey)?, Some((1u32, "one".to_string())));
    assert_eq!(list.peek_tail(&lkey)?, Some((2u32, "two".to_string())));

    assert_eq!(list.pop_head(&lkey)?, Some((1u32, "one".to_string())));
    assert_eq!(list.len(&lkey)?, 1);

    assert_eq!(list.pop_tail(&lkey)?, Some((2u32, "two".to_string())));
    assert_eq!(list.len(&lkey)?, 0);
    assert!(list.pop_head(&lkey)?.is_none());

    Ok(())
}
