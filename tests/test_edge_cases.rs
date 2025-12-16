use candystore::{CandyStore, Config, MAX_KEY_LEN, MAX_VALUE_LEN};

#[test]
fn test_max_key_len() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let key = vec![b'k'; MAX_KEY_LEN];
    let val = b"value";

    store.set(&key, val).unwrap();
    assert_eq!(store.get(&key).unwrap(), Some(val.to_vec()));
}

#[test]
fn test_max_value_len() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let key = b"key";
    let val = vec![b'v'; MAX_VALUE_LEN];

    store.set(key, &val).unwrap();
    assert_eq!(store.get(key).unwrap(), Some(val));
}

#[test]
fn test_key_too_long() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let key = vec![b'k'; MAX_KEY_LEN + 1];
    let val = b"value";

    let res = store.set(&key, val);
    assert!(res.is_err());
}

#[test]
fn test_value_too_long() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let key = b"key";
    let val = vec![b'v'; MAX_VALUE_LEN + 1];

    let res = store.set(key, &val);
    assert!(res.is_err());
}

#[test]
fn test_empty_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let key = b"";
    let val = b"value";

    // Assuming empty keys are allowed, or should they fail?
    // Usually KV stores allow empty keys.
    store.set(key, val).unwrap();
    assert_eq!(store.get(key).unwrap(), Some(val.to_vec()));
}

#[test]
fn test_empty_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = CandyStore::open(dir.path(), Config::default()).unwrap();

    let key = b"key";
    let val = b"";

    store.set(key, val).unwrap();
    assert_eq!(store.get(key).unwrap(), Some(val.to_vec()));
}
