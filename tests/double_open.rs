use candystore::{CandyStore, Config, Error};
use tempfile::tempdir;

#[test]
fn test_double_open_fails() {
    let dir = tempdir().unwrap();
    let _db1 = CandyStore::open(dir.path(), Config::default()).unwrap();

    // from the same thread
    let db2_res = CandyStore::open(dir.path(), Config::default());
    assert!(matches!(db2_res, Err(Error::LockfileTaken(_, _))));

    // from a different thread
    std::thread::spawn(move || {
        let db3_res = CandyStore::open(dir.path(), Config::default());
        assert!(matches!(db3_res, Err(Error::LockfileTaken(_, _))));
    })
    .join()
    .unwrap();
}
