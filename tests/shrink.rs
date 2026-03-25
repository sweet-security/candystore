use candystore::{CandyStore, Config, Result, SetStatus};

const ROW_WIDTH: usize = 16 * 21;

#[test]
fn test_shrink_to_fit_preserves_remaining_keys() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        initial_capacity: 4 * ROW_WIDTH,
        ..Config::default()
    };
    let store = CandyStore::open(dir.path(), config)?;

    for i in 0..10_000 {
        let key = format!("key_{i}");
        assert!(matches!(
            store.set(key.as_bytes(), b"value")?,
            SetStatus::CreatedNew
        ));
    }

    let before = store.capacity();

    for i in 0..9_000 {
        let key = format!("key_{i}");
        store.remove(key.as_bytes())?;
    }

    let shrunk_rows = store.shrink_to_fit_blocking(0.2)?;
    assert!(shrunk_rows > 0);
    assert!(store.capacity() <= before);

    for i in 9_000..10_000 {
        let key = format!("key_{i}");
        assert_eq!(store.get(key.as_bytes())?, Some(b"value".to_vec()));
    }

    for i in 0..9_000 {
        let key = format!("key_{i}");
        assert_eq!(store.get(key.as_bytes())?, None);
    }

    Ok(())
}
