use std::sync::Arc;

use candystore::{CandyStore, CandyTypedDeque, Config, Result};
use tempfile::TempDir;

#[test]
fn test_typed_queue_iter_rev() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let queue = CandyTypedDeque::<u32, u32>::new(Arc::clone(&store));

    let qkey = 7u32;
    queue.push_tail(&qkey, &10u32)?;
    queue.push_tail(&qkey, &20u32)?;
    queue.push_tail(&qkey, &30u32)?;

    let fwd: Vec<_> = queue.iter(&qkey).map(|r| r.unwrap().1).collect();
    assert_eq!(fwd, vec![10, 20, 30]);

    let rev: Vec<_> = queue.iter(&qkey).rev().map(|r| r.unwrap().1).collect();
    assert_eq!(rev, vec![30, 20, 10]);

    Ok(())
}

#[test]
fn test_typed_queue_discard() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let queue = CandyTypedDeque::<u32, u32>::new(Arc::clone(&store));

    let qkey = 5u32;
    queue.push_tail(&qkey, &1u32)?;
    queue.push_tail(&qkey, &2u32)?;
    assert_eq!(queue.len(&qkey)?, 2);
    assert_eq!(
        queue.range(&qkey)?,
        9223372036854775808usize..9223372036854775810usize
    );

    assert!(queue.discard(&qkey)?);
    assert_eq!(queue.len(&qkey)?, 0);
    assert!(queue.pop_head(&qkey)?.is_none());
    assert!(queue.is_empty(&qkey)?);
    Ok(())
}

#[test]
fn test_typed_queue_empty_push_head_has_simple_range_semantics() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default())?);
    let queue = CandyTypedDeque::<u32, u32>::new(Arc::clone(&store));

    let qkey = 11u32;
    assert!(queue.range(&qkey)?.is_empty());

    queue.push_head(&qkey, &99u32)?;
    assert_eq!(queue.peek_head(&qkey)?, Some(99u32));
    assert_eq!(queue.peek_tail(&qkey)?, Some(99u32));
    assert_eq!(queue.len(&qkey)?, 1);

    Ok(())
}
