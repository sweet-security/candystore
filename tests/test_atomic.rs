use candystore::{CandyStore, Config, GetOrCreateStatus, ReplaceStatus};

#[test]
fn test_atomic_get_or_create_and_replace() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    // get_or_create inserts when missing
    let first = store.get_or_create(b"a", b"one").unwrap();
    match first {
        GetOrCreateStatus::CreatedNew(v) => assert_eq!(v, b"one".to_vec()),
        other => panic!("expected CreatedNew, got {:?}", other),
    }
    // subsequent call returns existing and does not overwrite
    let second = store.get_or_create(b"a", b"two").unwrap();
    match second {
        GetOrCreateStatus::ExistingValue(v) => assert_eq!(v, b"one".to_vec()),
        other => panic!("expected ExistingValue, got {:?}", other),
    }
    assert_eq!(store.get(b"a").unwrap(), Some(b"one".to_vec()));

    // replace only works when key exists
    let missing = store.replace(b"missing", b"nope", None).unwrap();
    assert!(matches!(missing, ReplaceStatus::DoesNotExist));
    assert!(store.get(b"missing").unwrap().is_none());

    let replaced = store.replace(b"a", b"three", None).unwrap();
    assert!(matches!(replaced, ReplaceStatus::PrevValue(v) if v == b"one".to_vec()));
    assert_eq!(store.get(b"a").unwrap(), Some(b"three".to_vec()));
}
