use candystore::{
    CandyStore, Config, GetOrCreateStatus, ListCompactionParams, ReplaceStatus, SetStatus,
};
use tempfile::TempDir;

#[test]
fn test_list_set_get_len() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l1";

    assert_eq!(store.list_len(list).unwrap(), 0);

    assert!(matches!(
        store.set_in_list(list, b"k1", b"v1").unwrap(),
        SetStatus::CreatedNew
    ));
    assert!(matches!(
        store.set_in_list(list, b"k2", b"v2").unwrap(),
        SetStatus::CreatedNew
    ));

    assert_eq!(store.list_len(list).unwrap(), 2);

    assert_eq!(
        store.get_from_list(list, b"k1").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        store.get_from_list(list, b"k2").unwrap(),
        Some(b"v2".to_vec())
    );

    // Update existing key returns old value and keeps length
    assert!(matches!(
        store.set_in_list(list, b"k1", b"v1b").unwrap(),
        SetStatus::PrevValue(ref v) if v == b"v1"
    ));
    assert_eq!(store.list_len(list).unwrap(), 2);
    assert_eq!(
        store.get_from_list(list, b"k1").unwrap(),
        Some(b"v1b".to_vec())
    );
}

#[test]
fn test_list_remove_and_iteration() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l2";

    store.set_in_list(list, b"a", b"1").unwrap();
    store.set_in_list(list, b"b", b"2").unwrap();
    store.set_in_list(list, b"c", b"3").unwrap();

    assert_eq!(store.list_len(list).unwrap(), 3);

    // Remove head; iteration should start at next present item
    assert_eq!(
        store.remove_from_list(list, b"a").unwrap(),
        Some(b"1".to_vec())
    );
    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert_eq!(
        items,
        vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );
    assert_eq!(store.list_len(list).unwrap(), 2);

    // Remove tail; iteration should end at remaining tail
    assert_eq!(
        store.remove_from_list(list, b"c").unwrap(),
        Some(b"3".to_vec())
    );
    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert_eq!(items, vec![(b"b".to_vec(), b"2".to_vec())]);
    assert_eq!(store.list_len(list).unwrap(), 1);

    // Remove last element resets metadata
    assert_eq!(
        store.remove_from_list(list, b"b").unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(store.list_len(list).unwrap(), 0);
    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert!(items.is_empty());
}

#[test]
fn test_list_iteration_skips_holes() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l3";

    store.set_in_list(list, b"k1", b"v1").unwrap();
    store.set_in_list(list, b"k2", b"v2").unwrap();
    store.set_in_list(list, b"k3", b"v3").unwrap();

    assert_eq!(
        store.remove_from_list(list, b"k2").unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(store.list_len(list).unwrap(), 2);

    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert_eq!(
        items,
        vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ]
    );
}

#[test]
fn test_list_reverse_iteration_skips_holes() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l3_rev";

    store.set_in_list(list, b"k1", b"v1").unwrap();
    store.set_in_list(list, b"k2", b"v2").unwrap();
    store.set_in_list(list, b"k3", b"v3").unwrap();
    store.set_in_list(list, b"k4", b"v4").unwrap();

    // Create a hole in the middle
    store.remove_from_list(list, b"k2").unwrap();

    let rev_items: Vec<_> = store.iter_list(list).rev().map(|r| r.unwrap()).collect();

    assert_eq!(
        rev_items,
        vec![
            (b"k4".to_vec(), b"v4".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
            (b"k1".to_vec(), b"v1".to_vec()),
        ]
    );

    // Forward iteration still works after reverse
    let fwd_items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert_eq!(
        fwd_items,
        vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
            (b"k4".to_vec(), b"v4".to_vec()),
        ]
    );
}

#[test]
fn test_list_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    {
        let store = CandyStore::open(&path, Config::default()).unwrap();
        store.set_in_list(b"persist", b"k", b"v").unwrap();
    }

    {
        let store = CandyStore::open(&path, Config::default()).unwrap();
        assert_eq!(store.list_len(b"persist").unwrap(), 1);
        assert_eq!(
            store.get_from_list(b"persist", b"k").unwrap(),
            Some(b"v".to_vec())
        );
    }
}

#[test]
fn test_list_discard() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l_discard";

    store.set_in_list(list, b"a", b"1").unwrap();
    store.set_in_list(list, b"b", b"2").unwrap();
    assert_eq!(store.list_len(list).unwrap(), 2);

    assert!(store.discard_list(list).unwrap());
    assert_eq!(store.list_len(list).unwrap(), 0);
    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert!(items.is_empty());
}

#[test]
fn test_list_set_in_list_promoting_moves_to_head() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l_promote";

    store.set_in_list(list, b"a", b"1").unwrap();
    store.set_in_list(list, b"b", b"2").unwrap();
    store.set_in_list(list, b"c", b"3").unwrap();

    assert!(matches!(
        store
            .set_in_list_promoting(list, b"b", b"2x")
            .unwrap(),
        SetStatus::PrevValue(ref v) if v == b"2"
    ));

    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert_eq!(items.first().unwrap().0, b"b".to_vec());
    assert_eq!(items.first().unwrap().1, b"2x".to_vec());
}

#[test]
fn test_list_compact_if_needed() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l_compact";

    store.set_in_list(list, b"a", b"1").unwrap();
    store.set_in_list(list, b"b", b"2").unwrap();
    store.set_in_list(list, b"c", b"3").unwrap();
    store.remove_from_list(list, b"b").unwrap();

    assert_eq!(store.list_len(list).unwrap(), 2);

    let params = ListCompactionParams {
        min_length: 1,
        min_holes_ratio: 0.2,
    };
    assert!(store.compact_list_if_needed(list, params).unwrap());
    let items: Vec<_> = store.iter_list(list).map(|r| r.unwrap()).collect();
    assert_eq!(
        items,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]
    );

    // No compaction needed when already dense
    let params = ListCompactionParams {
        min_length: 1,
        min_holes_ratio: 0.5,
    };
    assert!(!store.compact_list_if_needed(list, params).unwrap());
}

#[test]
fn test_replace_and_get_or_create_in_list() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l_replace";

    // get_or_create creates when missing
    let created = store.get_or_create_in_list(list, b"k1", b"v1").unwrap();
    assert!(matches!(created, GetOrCreateStatus::CreatedNew(_)));

    // subsequent returns existing
    let existing = store
        .get_or_create_in_list(list, b"k1", b"different")
        .unwrap();
    assert!(matches!(existing, GetOrCreateStatus::ExistingValue(ref v) if v == b"v1"));

    // replace with wrong expected
    let wrong = store
        .replace_in_list(list, b"k1", b"v2", Some(b"zz"))
        .unwrap();
    assert!(matches!(wrong, ReplaceStatus::WrongValue(ref v) if v == b"v1"));

    // replace succeeds without expectation
    let replaced = store.replace_in_list(list, b"k1", b"v2", None).unwrap();
    assert!(matches!(replaced, ReplaceStatus::PrevValue(ref v) if v == b"v1"));
    assert_eq!(
        store.get_from_list(list, b"k1").unwrap(),
        Some(b"v2".to_vec())
    );

    // replace missing key
    let missing = store
        .replace_in_list(list, b"k_missing", b"v", None)
        .unwrap();
    assert!(matches!(missing, ReplaceStatus::DoesNotExist));
}

#[test]
fn test_list_pop_peek() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l_pop_peek";

    store.set_in_list(list, b"k1", b"v1").unwrap();
    store.set_in_list(list, b"k2", b"v2").unwrap();
    store.set_in_list(list, b"k3", b"v3").unwrap();

    // Peek head/tail
    let head = store.peek_list_head(list).unwrap().unwrap();
    assert_eq!(head.0, b"k1");
    assert_eq!(head.1, b"v1");

    let tail = store.peek_list_tail(list).unwrap().unwrap();
    assert_eq!(tail.0, b"k3");
    assert_eq!(tail.1, b"v3");

    // Pop head
    let popped_head = store.pop_list_head(list).unwrap().unwrap();
    assert_eq!(popped_head.0, b"k1");
    assert_eq!(popped_head.1, b"v1");
    assert_eq!(store.list_len(list).unwrap(), 2);

    // Pop tail
    let popped_tail = store.pop_list_tail(list).unwrap().unwrap();
    assert_eq!(popped_tail.0, b"k3");
    assert_eq!(popped_tail.1, b"v3");
    assert_eq!(store.list_len(list).unwrap(), 1);

    // Remaining item
    let remaining = store.peek_list_head(list).unwrap().unwrap();
    assert_eq!(remaining.0, b"k2");
    assert_eq!(remaining.1, b"v2");
}

#[test]
fn test_list_retain() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let list = b"l_retain";

    store.set_in_list(list, b"k1", b"v1").unwrap();
    store.set_in_list(list, b"k2", b"v2").unwrap();
    store.set_in_list(list, b"k3", b"v3").unwrap();

    store.retain_in_list(list, |k, _| Ok(k != b"k2")).unwrap();

    assert_eq!(store.list_len(list).unwrap(), 2);
    assert!(store.get_from_list(list, b"k1").unwrap().is_some());
    assert!(store.get_from_list(list, b"k2").unwrap().is_none());
    assert!(store.get_from_list(list, b"k3").unwrap().is_some());
}
