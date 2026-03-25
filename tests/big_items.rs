use candystore::{CandyStore, Config, Error, MAX_USER_VALUE_SIZE};
use tempfile::tempdir;

fn patterned_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|idx| (idx % 251) as u8).collect()
}

#[test]
fn test_set_get_remove_big() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let value = patterned_bytes(MAX_USER_VALUE_SIZE + 4096);

    assert!(!db.set_big("blob", &value)?);
    assert_eq!(db.get_big("blob")?, Some(value.clone()));
    assert!(db.remove_big("blob")?);
    assert_eq!(db.get_big("blob")?, None);
    assert!(!db.remove_big("blob")?);

    Ok(())
}

#[test]
fn test_set_big_reports_replacement() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(dir.path(), Config::default())?;

    let first = patterned_bytes(MAX_USER_VALUE_SIZE + 17);
    let second = patterned_bytes(MAX_USER_VALUE_SIZE * 2 + 33);

    assert!(!db.set_big("blob", &first)?);
    assert!(db.set_big("blob", &second)?);
    assert_eq!(db.get_big("blob")?, Some(second));

    Ok(())
}

#[test]
fn test_big_persists_across_reopen() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let value = patterned_bytes(MAX_USER_VALUE_SIZE * 2 + 123);

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        db.set_big("blob", &value)?;
    }

    let reopened = CandyStore::open(dir.path(), Config::default())?;
    assert_eq!(reopened.get_big("blob")?, Some(value));

    Ok(())
}

#[test]
fn test_big_can_exceed_single_value_limit() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let db = CandyStore::open(
        dir.path(),
        Config {
            max_data_file_size: 16 * 1024,
            ..Config::default()
        },
    )?;

    let value = patterned_bytes(MAX_USER_VALUE_SIZE * 3 + 777);
    db.set_big("rotating_blob", &value)?;
    assert_eq!(db.get_big("rotating_blob")?, Some(value));

    Ok(())
}
