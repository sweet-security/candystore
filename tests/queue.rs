mod common;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use candystore::{CandyStore, Config, Error};
use tempfile::tempdir;

#[test]
fn test_queue_fifo() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    db.push_to_queue_tail(&b"my_queue"[..], &b"item1"[..])?;
    db.push_to_queue_tail(&b"my_queue"[..], &b"item2"[..])?;
    db.push_to_queue_tail(&b"my_queue"[..], &b"item3"[..])?;

    assert_eq!(db.queue_len(&b"my_queue"[..])?, 3);
    assert_eq!(
        db.pop_queue_head(&b"my_queue"[..])?,
        Some(b"item1".to_vec())
    );
    assert_eq!(
        db.pop_queue_head(&b"my_queue"[..])?,
        Some(b"item2".to_vec())
    );
    assert_eq!(
        db.pop_queue_head(&b"my_queue"[..])?,
        Some(b"item3".to_vec())
    );
    assert_eq!(db.pop_queue_head(&b"my_queue"[..])?, None);
    assert_eq!(db.queue_len(&b"my_queue"[..])?, 0);

    Ok(())
}

#[test]
fn test_queue_lifo() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    db.push_to_queue_tail(&b"stack"[..], &b"item1"[..])?;
    db.push_to_queue_tail(&b"stack"[..], &b"item2"[..])?;
    db.push_to_queue_tail(&b"stack"[..], &b"item3"[..])?;

    assert_eq!(db.pop_queue_tail(&b"stack"[..])?, Some(b"item3".to_vec()));
    assert_eq!(db.pop_queue_tail(&b"stack"[..])?, Some(b"item2".to_vec()));
    assert_eq!(db.pop_queue_tail(&b"stack"[..])?, Some(b"item1".to_vec()));
    assert_eq!(db.pop_queue_tail(&b"stack"[..])?, None);

    Ok(())
}

#[test]
fn test_queue_deque() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    db.push_to_queue_head(&b"deque"[..], &b"1"[..])?;
    db.push_to_queue_head(&b"deque"[..], &b"2"[..])?;
    db.push_to_queue_tail(&b"deque"[..], &b"3"[..])?;
    db.push_to_queue_tail(&b"deque"[..], &b"4"[..])?;

    assert_eq!(db.queue_len(&b"deque"[..])?, 4);
    assert_eq!(db.peek_queue_head(&b"deque"[..])?, Some(b"2".to_vec()));
    assert_eq!(db.peek_queue_tail(&b"deque"[..])?, Some(b"4".to_vec()));
    assert_eq!(db.pop_queue_head(&b"deque"[..])?, Some(b"2".to_vec()));
    assert_eq!(db.pop_queue_tail(&b"deque"[..])?, Some(b"4".to_vec()));
    assert_eq!(db.pop_queue_head(&b"deque"[..])?, Some(b"1".to_vec()));
    assert_eq!(db.pop_queue_tail(&b"deque"[..])?, Some(b"3".to_vec()));
    assert_eq!(db.pop_queue_head(&b"deque"[..])?, None);

    Ok(())
}

#[test]
fn test_queue_with_idx_methods() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let first = db.push_to_queue_tail(&b"idxq"[..], &b"a"[..])?;
    let second = db.push_to_queue_tail(&b"idxq"[..], &b"b"[..])?;
    let third = db.push_to_queue_head(&b"idxq"[..], &b"z"[..])?;

    assert!(third < first && first < second);
    assert_eq!(
        db.peek_queue_head_with_idx(&b"idxq"[..])?,
        Some((third, b"z".to_vec()))
    );
    assert_eq!(
        db.peek_queue_tail_with_idx(&b"idxq"[..])?,
        Some((second, b"b".to_vec()))
    );
    assert_eq!(
        db.pop_queue_head_with_idx(&b"idxq"[..])?,
        Some((third, b"z".to_vec()))
    );
    assert_eq!(
        db.pop_queue_tail_with_idx(&b"idxq"[..])?,
        Some((second, b"b".to_vec()))
    );
    assert_eq!(
        db.pop_queue_head_with_idx(&b"idxq"[..])?,
        Some((first, b"a".to_vec()))
    );
    assert_eq!(db.pop_queue_tail_with_idx(&b"idxq"[..])?, None);

    Ok(())
}

#[test]
fn test_queue_empty_push_head_has_stable_value_semantics() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    assert!(db.queue_range(&b"head_first"[..])?.is_empty());

    let idx = db.push_to_queue_head(&b"head_first"[..], &b"x"[..])?;
    assert_eq!(
        db.peek_queue_head_with_idx(&b"head_first"[..])?,
        Some((idx, b"x".to_vec()))
    );
    assert_eq!(
        db.peek_queue_tail_with_idx(&b"head_first"[..])?,
        Some((idx, b"x".to_vec()))
    );
    assert_eq!(db.queue_len(&b"head_first"[..])?, 1);

    Ok(())
}

#[test]
fn test_queue_peek_skips_holes_like_legacy_candystore() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let first = db.push_to_queue_tail(&b"peek_holes_head"[..], &b"v1"[..])?;
    let second = db.push_to_queue_tail(&b"peek_holes_head"[..], &b"v2"[..])?;
    let third = db.push_to_queue_tail(&b"peek_holes_head"[..], &b"v3"[..])?;

    assert_eq!(
        db.remove_from_queue(&b"peek_holes_head"[..], second)?,
        Some(b"v2".to_vec())
    );
    assert_eq!(
        db.remove_from_queue(&b"peek_holes_head"[..], first)?,
        Some(b"v1".to_vec())
    );
    assert_eq!(
        db.peek_queue_head_with_idx(&b"peek_holes_head"[..])?,
        Some((third, b"v3".to_vec()))
    );
    assert_eq!(db.queue_range(&b"peek_holes_head"[..])?, second..third + 1);

    let first = db.push_to_queue_tail(&b"peek_holes_tail"[..], &b"v1"[..])?;
    let second = db.push_to_queue_tail(&b"peek_holes_tail"[..], &b"v2"[..])?;
    let third = db.push_to_queue_tail(&b"peek_holes_tail"[..], &b"v3"[..])?;

    assert_eq!(
        db.remove_from_queue(&b"peek_holes_tail"[..], second)?,
        Some(b"v2".to_vec())
    );
    assert_eq!(
        db.remove_from_queue(&b"peek_holes_tail"[..], third)?,
        Some(b"v3".to_vec())
    );
    assert_eq!(
        db.peek_queue_tail_with_idx(&b"peek_holes_tail"[..])?,
        Some((first, b"v1".to_vec()))
    );
    assert_eq!(db.queue_range(&b"peek_holes_tail"[..])?, first..third);

    Ok(())
}

#[test]
fn test_extend_queue_returns_inserted_range() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let first = db.extend_queue(&b"bulk"[..], [&b"v1"[..], &b"v2"[..], &b"v3"[..]])?;
    assert_eq!(first.len(), 3);
    assert_eq!(db.queue_range(&b"bulk"[..])?, first.clone());

    let second = db.extend_queue(&b"bulk"[..], [&b"v4"[..], &b"v5"[..]])?;
    assert_eq!(second.start, first.end);
    assert_eq!(second.len(), 2);
    assert_eq!(db.queue_range(&b"bulk"[..])?, first.start..second.end);

    let items: Vec<_> = db.iter_queue(&b"bulk"[..]).collect::<Result<_, _>>()?;
    assert_eq!(
        items,
        vec![
            (first.start, b"v1".to_vec()),
            (first.start + 1, b"v2".to_vec()),
            (first.start + 2, b"v3".to_vec()),
            (second.start, b"v4".to_vec()),
            (second.start + 1, b"v5".to_vec()),
        ]
    );

    Ok(())
}

#[test]
fn test_queue_persistence() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        db.push_to_queue_tail(&b"q1"[..], &b"val1"[..])?;
        db.push_to_queue_tail(&b"q1"[..], &b"val2"[..])?;
    }

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        assert_eq!(db.queue_len(&b"q1"[..])?, 2);
        assert_eq!(db.pop_queue_head(&b"q1"[..])?, Some(b"val1".to_vec()));
    }

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        assert_eq!(db.queue_len(&b"q1"[..])?, 1);
        assert_eq!(db.pop_queue_head(&b"q1"[..])?, Some(b"val2".to_vec()));
        assert_eq!(db.pop_queue_head(&b"q1"[..])?, None);
    }

    Ok(())
}

#[test]
fn test_queue_reverse_iteration_skips_holes() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    db.push_to_queue_tail(&b"q_rev_iter"[..], &b"v1"[..])?;
    db.push_to_queue_tail(&b"q_rev_iter"[..], &b"v2"[..])?;
    db.push_to_queue_tail(&b"q_rev_iter"[..], &b"v3"[..])?;
    db.push_to_queue_tail(&b"q_rev_iter"[..], &b"v4"[..])?;

    assert_eq!(db.pop_queue_head(&b"q_rev_iter"[..])?, Some(b"v1".to_vec()));
    assert_eq!(db.pop_queue_head(&b"q_rev_iter"[..])?, Some(b"v2".to_vec()));

    let rev_items: Vec<_> = db
        .iter_queue(&b"q_rev_iter"[..])
        .rev()
        .map(|res| res.unwrap().1)
        .collect();
    assert_eq!(rev_items, vec![b"v4".to_vec(), b"v3".to_vec()]);

    let fwd_items: Vec<_> = db
        .iter_queue(&b"q_rev_iter"[..])
        .map(|res| res.unwrap().1)
        .collect();
    assert_eq!(fwd_items, vec![b"v3".to_vec(), b"v4".to_vec()]);

    Ok(())
}

#[test]
fn test_multiple_queues() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    db.push_to_queue_tail(&b"q1"[..], &b"v1"[..])?;
    db.push_to_queue_tail(&b"q2"[..], &b"v2"[..])?;

    assert_eq!(db.pop_queue_head(&b"q1"[..])?, Some(b"v1".to_vec()));
    assert_eq!(db.pop_queue_head(&b"q2"[..])?, Some(b"v2".to_vec()));

    Ok(())
}

#[test]
fn test_queue_remove_hole_is_skipped() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let idx1 = db.push_to_queue_tail(&b"holey"[..], &b"v1"[..])?;
    let idx2 = db.push_to_queue_tail(&b"holey"[..], &b"v2"[..])?;
    let idx3 = db.push_to_queue_tail(&b"holey"[..], &b"v3"[..])?;

    assert!(idx1 < idx2 && idx2 < idx3);
    assert_eq!(
        db.remove_from_queue(&b"holey"[..], idx2)?,
        Some(b"v2".to_vec())
    );

    let items: Vec<_> = db.iter_queue(&b"holey"[..]).collect::<Result<_, _>>()?;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].1, b"v1".to_vec());
    assert_eq!(items[1].1, b"v3".to_vec());

    Ok(())
}

#[test]
fn test_queue_concurrency() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), common::small_file_config())?);
    let queue = b"concurrent_queue";

    let producers = 4;
    let items_per_producer = 1000;
    let consumers = 4;
    let finished = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicUsize::new(0));

    let mut consumer_handles = Vec::new();
    for _ in 0..consumers {
        let db = db.clone();
        let finished = finished.clone();
        let consumed = consumed.clone();
        consumer_handles.push(std::thread::spawn(move || {
            loop {
                match db.pop_queue_head(&queue[..]).unwrap() {
                    Some(_) => {
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {
                        if finished.load(Ordering::Relaxed) {
                            match db.pop_queue_head(&queue[..]).unwrap() {
                                Some(_) => {
                                    consumed.fetch_add(1, Ordering::Relaxed);
                                }
                                None => break,
                            }
                        } else {
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }));
    }

    let mut producer_handles = Vec::new();
    for producer in 0..producers {
        let db = db.clone();
        producer_handles.push(std::thread::spawn(move || {
            for item in 0..items_per_producer {
                let value = format!("p{producer}-{item}");
                db.push_to_queue_tail(&queue[..], value.as_bytes()).unwrap();
            }
        }));
    }

    for handle in producer_handles {
        handle.join().unwrap();
    }
    finished.store(true, Ordering::Relaxed);

    for handle in consumer_handles {
        handle.join().unwrap();
    }

    assert_eq!(
        consumed.load(Ordering::Relaxed),
        producers * items_per_producer
    );
    assert_eq!(db.queue_len(&queue[..])?, 0);

    Ok(())
}
