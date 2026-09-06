//! Golden-fixture test guarding on-disk format compatibility.
//!
//! `tests/fixtures/v1_store` was written by a released build. Opening it must
//! succeed without replaying entries or recreating the index, and its contents
//! must match exactly. If the format legitimately changes, bump the version
//! constants, add an upgrade path, and regenerate with:
//!
//! `cargo test --test format_compat regenerate_fixture -- --ignored`

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use candystore::{
    CandyStore, CandyTypedDeque, CandyTypedList, CandyTypedStore, Config, Error, Result,
};
use tempfile::tempdir;

const FIXTURE: &str = "tests/fixtures/v1_store";
const NUM_KV: u32 = 120;
const BIG_LEN: usize = 10_000;
/// All live entries across namespaces: 101 user KV, list 19 data + 19 index +
/// 1 meta, queue 14 + 1 meta, big 4 chunks + 1 meta, typed KV 1, typed big
/// 4 + 1 meta, typed list 2 + 2 + 1 meta, typed deque 2 + 1 meta.
const TOTAL_ENTRIES: usize = 174;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE)
}

/// Small files so the fixture spans several rotations while staying tiny.
fn fixture_config() -> Config {
    Config {
        max_data_file_size: 4096,
        compaction_throughput_bytes_per_sec: 0,
        checkpoint_interval: None,
        checkpoint_delta_bytes: None,
        ..Config::default()
    }
}

fn kv_key(i: u32) -> String {
    format!("kv{i:04}")
}

fn expected_kv() -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut expected = BTreeMap::new();
    for i in 0..NUM_KV {
        let value = if i < 40 {
            format!("updated-{i}")
        } else {
            format!("value-{i}").repeat(2)
        };
        if !(60..80).contains(&i) {
            expected.insert(kv_key(i).into_bytes(), value.into_bytes());
        }
    }
    expected.insert(b"written-by-fixture".to_vec(), b"ok".to_vec());
    expected
}

fn populate(db: &Arc<CandyStore>) -> Result<()> {
    for i in 0..NUM_KV {
        db.set(kv_key(i), format!("value-{i}").repeat(2))?;
    }
    for i in 0..40 {
        db.set(kv_key(i), format!("updated-{i}"))?;
    }
    for i in 60..80 {
        db.remove(kv_key(i))?;
    }
    db.set("written-by-fixture", "ok")?;

    for i in 0..20 {
        db.set_in_list("list", &format!("item{i:03}"), &format!("lv{i}"))?;
    }
    db.remove_from_list("list", "item005")?;

    for i in 0..15 {
        db.push_to_queue_tail("queue", &format!("q{i}"))?;
    }
    db.pop_queue_head("queue")?;
    db.set_big("big", &vec![7u8; BIG_LEN])?;

    let typed = CandyTypedStore::<u32, String>::new(Arc::clone(db));
    typed.set(&42u32, &"typed".to_string())?;
    let typed_big = CandyTypedStore::<u32, Vec<u8>>::new(Arc::clone(db));
    typed_big.set_big(&7u32, &vec![9u8; BIG_LEN])?;

    let typed_list = CandyTypedList::<String, u32, String>::new(Arc::clone(db));
    typed_list.set(&"tl".to_string(), &1u32, &"one".to_string())?;
    typed_list.set(&"tl".to_string(), &2u32, &"two".to_string())?;

    let typed_deque = CandyTypedDeque::<String, u64>::new(Arc::clone(db));
    typed_deque.push_tail(&"td".to_string(), &10u64)?;
    typed_deque.push_head(&"td".to_string(), &5u64)?;
    Ok(())
}

fn verify(db: &Arc<CandyStore>) -> Result<()> {
    verify_inner(db, true)
}

fn verify_inner(db: &Arc<CandyStore>, check_count: bool) -> Result<()> {
    let expected = expected_kv();
    let actual: BTreeMap<Vec<u8>, Vec<u8>> = db.iter_items().collect::<Result<_>>()?;
    assert_eq!(actual, expected);
    if check_count {
        assert_eq!(db.num_items(), TOTAL_ENTRIES);
    }

    let list: Vec<_> = db.iter_list("list").collect::<Result<_>>()?;
    assert_eq!(list.len(), 19);
    assert_eq!(list[0], (b"item000".to_vec(), b"lv0".to_vec()));
    assert_eq!(list[5], (b"item006".to_vec(), b"lv6".to_vec()));
    assert_eq!(db.list_len("list")?, 19);

    let queue: Vec<_> = db.iter_queue("queue").collect::<Result<_>>()?;
    assert_eq!(queue.len(), 14);
    assert_eq!(queue[0].1, b"q1".to_vec());
    assert_eq!(db.queue_len("queue")?, 14);
    assert_eq!(db.get_big("big")?, Some(vec![7u8; BIG_LEN]));

    let typed = CandyTypedStore::<u32, String>::new(Arc::clone(db));
    assert_eq!(typed.get(&42u32)?, Some("typed".to_string()));
    let typed_big = CandyTypedStore::<u32, Vec<u8>>::new(Arc::clone(db));
    assert_eq!(typed_big.get_big(&7u32)?, Some(vec![9u8; BIG_LEN]));

    let typed_list = CandyTypedList::<String, u32, String>::new(Arc::clone(db));
    let items: Vec<_> = typed_list.iter(&"tl".to_string()).collect::<Result<_>>()?;
    assert_eq!(items, vec![(1, "one".to_string()), (2, "two".to_string())]);

    let typed_deque = CandyTypedDeque::<String, u64>::new(Arc::clone(db));
    let items: Vec<_> = typed_deque
        .iter(&"td".to_string())
        .map(|r| r.map(|(_, v)| v))
        .collect::<Result<_>>()?;
    assert_eq!(items, vec![5, 10]);
    Ok(())
}

fn copy_fixture(dst: &Path) -> Result<()> {
    for entry in std::fs::read_dir(fixture_dir()).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        std::fs::copy(entry.path(), dst.join(entry.file_name())).map_err(Error::IOError)?;
    }
    Ok(())
}

/// Signature + version of the `index` and every `data_*` file.
fn header_prefixes(dir: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut prefixes = BTreeMap::new();
    for entry in std::fs::read_dir(dir).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "index" || name.starts_with("data_") {
            let bytes = std::fs::read(entry.path()).map_err(Error::IOError)?;
            prefixes.insert(name, bytes[..12].to_vec());
        }
    }
    Ok(prefixes)
}

#[test]
fn fixture_store_opens_without_rebuild_or_migration() -> Result<()> {
    let dir = tempdir().unwrap();
    copy_fixture(dir.path())?;
    let headers_before = header_prefixes(dir.path())?;
    assert!(
        headers_before.len() > 2,
        "fixture should contain an index and several data files"
    );

    let db = Arc::new(CandyStore::open(dir.path(), fixture_config())?);
    let stats = db.stats();
    assert_eq!(
        stats.num_rebuilt_entries, 0,
        "a cleanly closed fixture must not be replayed"
    );
    assert_eq!(stats.num_rebuild_purged_bytes, 0);
    assert_eq!(
        header_prefixes(dir.path())?,
        headers_before,
        "index/data headers changed on open: the fixture was migrated or recreated"
    );
    verify(&db)?;

    // The store must remain fully usable and reopenable after mutation.
    db.set("post-open", "v")?;
    db.set_in_list("list", "item999", "new")?;
    drop(db);
    let db = Arc::new(CandyStore::open(dir.path(), fixture_config())?);
    assert_eq!(db.get("post-open")?, Some(b"v".to_vec()));
    assert_eq!(db.list_len("list")?, 20);
    // +1 user entry, +2 list entries (data + index).
    assert_eq!(db.num_items(), TOTAL_ENTRIES + 3);
    Ok(())
}

/// Round-trips the fixture generator against the current build so the
/// fixture contents and `verify` stay in sync.
#[test]
fn fixture_generator_round_trips_on_current_build() -> Result<()> {
    let dir = tempdir().unwrap();
    {
        let db = Arc::new(CandyStore::open(dir.path(), fixture_config())?);
        populate(&db)?;
        verify(&db)?;
    }
    let db = Arc::new(CandyStore::open(dir.path(), fixture_config())?);
    verify(&db)
}

#[test]
#[ignore]
fn regenerate_fixture() -> Result<()> {
    let dir = fixture_dir();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(Error::IOError)?;
    {
        let db = Arc::new(CandyStore::open(&dir, fixture_config())?);
        populate(&db)?;
        // Older writers mis-tracked num_items; the reader test asserts it.
        verify_inner(&db, false)?;
        assert!(
            db.stats().num_data_files > 2,
            "fixture should span rotations"
        );
    }
    std::fs::remove_file(dir.join(".lockfile")).map_err(Error::IOError)?;
    Ok(())
}
