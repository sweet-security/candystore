use candystore::{
    CandyStore, Config, GetOrCreateStatus, ListCompactionParams, ReplaceStatus, SetStatus,
};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_list_set_get_len() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"l1";

    assert_eq!(db.list_len(list).unwrap(), 0);

    assert!(matches!(
        db.set_in_list(list, b"k1", b"v1").unwrap(),
        SetStatus::CreatedNew
    ));
    assert!(matches!(
        db.set_in_list(list, b"k2", b"v2").unwrap(),
        SetStatus::CreatedNew
    ));

    assert_eq!(db.list_len(list).unwrap(), 2);
    assert_eq!(db.get_from_list(list, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get_from_list(list, b"k2").unwrap(), Some(b"v2".to_vec()));

    assert!(
        matches!(db.set_in_list(list, b"k1", b"v1b").unwrap(), SetStatus::PrevValue(ref value) if value == b"v1")
    );
    assert_eq!(db.list_len(list).unwrap(), 2);
    assert_eq!(
        db.get_from_list(list, b"k1").unwrap(),
        Some(b"v1b".to_vec())
    );
}

#[test]
fn test_list_remove_and_iteration() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"l2";

    db.set_in_list(list, b"a", b"1").unwrap();
    db.set_in_list(list, b"b", b"2").unwrap();
    db.set_in_list(list, b"c", b"3").unwrap();

    assert_eq!(
        db.remove_from_list(list, b"a").unwrap(),
        Some(b"1".to_vec())
    );
    let items: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(
        items,
        vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec())
        ]
    );

    assert_eq!(
        db.remove_from_list(list, b"c").unwrap(),
        Some(b"3".to_vec())
    );
    let items: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(items, vec![(b"b".to_vec(), b"2".to_vec())]);

    assert_eq!(
        db.remove_from_list(list, b"b").unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(db.list_len(list).unwrap(), 0);
    assert_eq!(db.iter_list(list).count(), 0);
}

#[test]
fn test_list_iteration_skips_holes_and_reverse() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"l3";

    db.set_in_list(list, b"k1", b"v1").unwrap();
    db.set_in_list(list, b"k2", b"v2").unwrap();
    db.set_in_list(list, b"k3", b"v3").unwrap();
    db.set_in_list(list, b"k4", b"v4").unwrap();
    db.remove_from_list(list, b"k2").unwrap();

    let forward: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(
        forward,
        vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
            (b"k4".to_vec(), b"v4".to_vec()),
        ]
    );

    let reverse: Vec<_> = db
        .iter_list(list)
        .rev()
        .map(|entry| entry.unwrap())
        .collect();
    assert_eq!(
        reverse,
        vec![
            (b"k4".to_vec(), b"v4".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
            (b"k1".to_vec(), b"v1".to_vec()),
        ]
    );
}

#[test]
fn test_list_persistence_and_discard() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = CandyStore::open(&path, Config::default()).unwrap();
        db.set_in_list(b"persist", b"k", b"v").unwrap();
    }

    {
        let db = CandyStore::open(&path, Config::default()).unwrap();
        assert_eq!(db.list_len(b"persist").unwrap(), 1);
        assert_eq!(
            db.get_from_list(b"persist", b"k").unwrap(),
            Some(b"v".to_vec())
        );
        assert!(db.discard_list(b"persist").unwrap());
        assert_eq!(db.list_len(b"persist").unwrap(), 0);
    }
}

#[test]
fn test_list_promoting_matches_legacy_tail_semantics() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"promo";

    db.set_in_list(list, b"a", b"1").unwrap();
    db.set_in_list(list, b"b", b"2").unwrap();
    db.set_in_list(list, b"c", b"3").unwrap();

    assert!(
        matches!(db.set_in_list_promoting(list, b"b", b"2x").unwrap(), SetStatus::PrevValue(ref value) if value == b"2")
    );

    let items: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(items.last().unwrap(), &(b"b".to_vec(), b"2x".to_vec()));

    assert!(matches!(
        db.set_in_list_promoting(list, b"d", b"4").unwrap(),
        SetStatus::CreatedNew
    ));

    let items: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(items.last().unwrap(), &(b"d".to_vec(), b"4".to_vec()));
}

#[test]
fn test_list_compact_if_needed() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"compact";

    db.set_in_list(list, b"a", b"1").unwrap();
    db.set_in_list(list, b"b", b"2").unwrap();
    db.set_in_list(list, b"c", b"3").unwrap();
    db.remove_from_list(list, b"b").unwrap();

    assert!(
        db.compact_list_if_needed(
            list,
            ListCompactionParams {
                min_length: 1,
                min_holes_ratio: 0.2
            }
        )
        .unwrap()
    );
    let items: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(
        items,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec())
        ]
    );
    assert!(
        !db.compact_list_if_needed(
            list,
            ListCompactionParams {
                min_length: 1,
                min_holes_ratio: 0.5
            }
        )
        .unwrap()
    );
}

#[test]
fn test_replace_and_get_or_create_in_list() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"replace";

    assert!(
        matches!(db.get_or_create_in_list(list, b"k1", b"v1").unwrap(), GetOrCreateStatus::CreatedNew(ref value) if value == b"v1")
    );
    assert!(
        matches!(db.get_or_create_in_list(list, b"k1", b"other").unwrap(), GetOrCreateStatus::ExistingValue(ref value) if value == b"v1")
    );
    assert!(
        matches!(db.replace_in_list(list, b"k1", b"v2", Some(b"zz")).unwrap(), ReplaceStatus::WrongValue(ref value) if value == b"v1")
    );
    assert!(
        matches!(db.replace_in_list(list, b"k1", b"v2", None::<&[u8]>).unwrap(), ReplaceStatus::PrevValue(ref value) if value == b"v1")
    );
    assert_eq!(db.get_from_list(list, b"k1").unwrap(), Some(b"v2".to_vec()));
    assert!(matches!(
        db.replace_in_list(list, b"missing", b"v", None::<&[u8]>)
            .unwrap(),
        ReplaceStatus::DoesNotExist
    ));
}

#[test]
fn test_list_pop_peek_and_retain() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"poppeek";

    db.set_in_list(list, b"k1", b"v1").unwrap();
    db.set_in_list(list, b"k2", b"v2").unwrap();
    db.set_in_list(list, b"k3", b"v3").unwrap();

    assert_eq!(
        db.peek_list_head(list).unwrap().unwrap(),
        (b"k1".to_vec(), b"v1".to_vec())
    );
    assert_eq!(
        db.peek_list_tail(list).unwrap().unwrap(),
        (b"k3".to_vec(), b"v3".to_vec())
    );
    assert_eq!(
        db.pop_list_head(list).unwrap().unwrap(),
        (b"k1".to_vec(), b"v1".to_vec())
    );
    assert_eq!(
        db.pop_list_tail(list).unwrap().unwrap(),
        (b"k3".to_vec(), b"v3".to_vec())
    );

    db.set_in_list(list, b"k4", b"v4").unwrap();
    db.set_in_list(list, b"k5", b"v5").unwrap();
    db.retain_in_list(list, |key, _| Ok(key != b"k4")).unwrap();

    let items: Vec<_> = db.iter_list(list).map(|entry| entry.unwrap()).collect();
    assert_eq!(
        items,
        vec![
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k5".to_vec(), b"v5".to_vec())
        ]
    );
    assert!(
        !db.compact_list_if_needed(
            list,
            ListCompactionParams {
                min_length: 1,
                min_holes_ratio: 0.1,
            },
        )
        .unwrap()
    );
}

#[test]
fn test_list_compaction_uses_span_like_legacy_candystore() {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default()).unwrap();
    let list = b"span_compact";

    for idx in 0..10u8 {
        db.set_in_list(list, &[idx], &[idx]).unwrap();
    }
    for idx in 1..9u8 {
        db.remove_from_list(list, &[idx]).unwrap();
    }

    assert!(
        db.compact_list_if_needed(
            list,
            ListCompactionParams {
                min_length: 5,
                min_holes_ratio: 0.5,
            },
        )
        .unwrap()
    );
}

#[test]
fn test_list_concurrency_basic() {
    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default()).unwrap());
    let list_key = b"concurrent_list";
    let num_threads = 8;
    let items_per_thread = 200;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = Vec::new();
    for thread_idx in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for item_idx in 0..items_per_thread {
                let key = format!("t{thread_idx}-{item_idx}");
                let value = format!("val-{thread_idx}-{item_idx}");
                db.set_in_list(list_key, key.as_bytes(), value.as_bytes())
                    .unwrap();
            }

            for item_idx in 0..items_per_thread {
                let key = format!("t{thread_idx}-{item_idx}");
                db.remove_from_list(list_key, key.as_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(db.list_len(list_key).unwrap(), 0);
    assert_eq!(db.iter_list(list_key).count(), 0);
}

#[test]
fn test_list_concurrency_promoting() {
    let dir = tempdir().unwrap();
    let db = Arc::new(CandyStore::open(dir.path(), Config::default()).unwrap());
    let list_key = b"concurrent_list_promo";
    let num_threads = 4;
    let items_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    for idx in 0..100 {
        db.set_in_list(list_key, format!("base-{idx}").as_bytes(), b"base")
            .unwrap();
    }

    let mut handles = Vec::new();
    for thread_idx in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for item_idx in 0..items_per_thread {
                let key = format!("t{thread_idx}-{item_idx}");
                db.set_in_list_promoting(list_key, key.as_bytes(), b"val")
                    .unwrap();
            }

            for base_idx in 0..50 {
                let key = format!("base-{base_idx}");
                db.set_in_list_promoting(list_key, key.as_bytes(), b"base-promoted")
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let expected_len = 100 + num_threads * items_per_thread;
    assert_eq!(db.list_len(list_key).unwrap(), expected_len);
    assert_eq!(db.iter_list(list_key).count(), expected_len);
}
