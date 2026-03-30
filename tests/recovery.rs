mod common;

use std::collections::HashSet;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use candystore::{CandyStore, CandyTypedDeque, CandyTypedList, CandyTypedStore, Config, Error};
use tempfile::tempdir;

use crate::common::checkpoint_slot_checksum;

const CHECKPOINT_SLOT_0_OFFSET: u64 = 128;
const CHECKPOINT_SLOT_STRIDE: u64 = 32;
const CHECKPOINT_SLOT_CHECKSUM_OFFSET: u64 = 24;

fn patterned_bytes_with_seed(len: usize, seed: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| (((idx * 31) + (seed * 17)) % 251) as u8)
        .collect()
}

fn rewrite_first_data_entry_header(
    dir: &std::path::Path,
    rewrite: impl FnOnce(u32) -> u32,
) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("data_0000"))
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(4096)).map_err(Error::IOError)?;
    let mut entry_header = [0u8; 8];
    file.read_exact(&mut entry_header).map_err(Error::IOError)?;

    let header = u32::from_le_bytes(entry_header[0..4].try_into().unwrap());
    let klen = u16::from_le_bytes(entry_header[4..6].try_into().unwrap()) as usize;
    let vlen = u16::from_le_bytes(entry_header[6..8].try_into().unwrap()) as usize;
    let entry_len = 4 + 4 + klen + vlen + 2;

    file.seek(SeekFrom::Start(4096)).map_err(Error::IOError)?;
    let mut entry = vec![0u8; entry_len];
    file.read_exact(&mut entry).map_err(Error::IOError)?;
    entry[0..4].copy_from_slice(&rewrite(header).to_le_bytes());

    let checksum = crc16_ibm3740_fast::hash(&entry[..entry_len - 2]) as u16;
    entry[entry_len - 2..entry_len].copy_from_slice(&checksum.to_le_bytes());

    file.seek(SeekFrom::Start(4096)).map_err(Error::IOError)?;
    file.write_all(&entry).map_err(Error::IOError)?;
    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

fn rewrite_data_file_ordinal(
    dir: &std::path::Path,
    file_idx: u16,
    ordinal: u64,
) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join(format!("data_{file_idx:04}")))
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(16)).map_err(Error::IOError)?;
    file.write_all(&ordinal.to_le_bytes())
        .map_err(Error::IOError)?;
    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

fn active_file_ordinal(dir: &std::path::Path) -> Result<u64, Error> {
    let mut max_ordinal: Option<u64> = None;

    for entry in std::fs::read_dir(dir).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with("data_") {
            continue;
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(Error::IOError)?;
        file.seek(SeekFrom::Start(16)).map_err(Error::IOError)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).map_err(Error::IOError)?;
        let ordinal = u64::from_le_bytes(buf);
        max_ordinal = Some(max_ordinal.map_or(ordinal, |current| current.max(ordinal)));
    }

    max_ordinal.ok_or_else(|| {
        Error::IOError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no data files found",
        ))
    })
}

fn data_files_by_ordinal(dir: &std::path::Path) -> Result<Vec<(u64, u64)>, Error> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(dir).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with("data_") {
            continue;
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(Error::IOError)?;
        file.seek(SeekFrom::Start(16)).map_err(Error::IOError)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).map_err(Error::IOError)?;
        let ordinal = u64::from_le_bytes(buf);
        let used_bytes = file
            .metadata()
            .map_err(Error::IOError)?
            .len()
            .saturating_sub(4096);
        files.push((ordinal, used_bytes));
    }

    files.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(files)
}

fn data_file_records_by_ordinal(
    dir: &std::path::Path,
) -> Result<Vec<(u16, u64, u64, std::path::PathBuf)>, Error> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(dir).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(file_idx) = name
            .strip_prefix("data_")
            .and_then(|suffix| suffix.parse::<u16>().ok())
        else {
            continue;
        };

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(Error::IOError)?;
        file.seek(SeekFrom::Start(16)).map_err(Error::IOError)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).map_err(Error::IOError)?;
        let ordinal = u64::from_le_bytes(buf);
        let used_bytes = file
            .metadata()
            .map_err(Error::IOError)?
            .len()
            .saturating_sub(4096);
        files.push((file_idx, ordinal, used_bytes, path));
    }

    files.sort_by_key(|(_, ordinal, _, _)| *ordinal);
    Ok(files)
}

fn write_commit_cursor(dir: &std::path::Path, offset: u64) -> Result<(), Error> {
    let ordinal = active_file_ordinal(dir)?;
    write_commit_cursor_for_ordinal(dir, ordinal, offset)
}

fn write_commit_cursor_for_ordinal(
    dir: &std::path::Path,
    ordinal: u64,
    offset: u64,
) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("index"))
        .map_err(Error::IOError)?;

    let generation = next_checkpoint_generation(&mut file)?;
    let checksum = checkpoint_slot_checksum(generation, ordinal, offset);
    let slot_offset = 128 + (generation as u64 % 2) * 32;

    file.seek(SeekFrom::Start(slot_offset))
        .map_err(Error::IOError)?;
    file.write_all(&generation.to_le_bytes())
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(slot_offset + 8))
        .map_err(Error::IOError)?;
    file.write_all(&ordinal.to_le_bytes())
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(slot_offset + 16))
        .map_err(Error::IOError)?;
    file.write_all(&offset.to_le_bytes())
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(slot_offset + 24))
        .map_err(Error::IOError)?;
    file.write_all(&checksum.to_le_bytes())
        .map_err(Error::IOError)?;
    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

fn next_checkpoint_generation(file: &mut std::fs::File) -> Result<u64, Error> {
    use std::io::Read;

    let mut max_generation = 0u64;
    for slot_offset in [128u64, 160u64] {
        file.seek(SeekFrom::Start(slot_offset))
            .map_err(Error::IOError)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).map_err(Error::IOError)?;
        max_generation = max_generation.max(u64::from_le_bytes(buf));
    }

    Ok(max_generation + 1)
}

fn corrupt_latest_checkpoint_slot_checksum(dir: &std::path::Path) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("index"))
        .map_err(Error::IOError)?;

    let mut latest_generation = 0u64;
    let mut latest_slot_offset = CHECKPOINT_SLOT_0_OFFSET;
    for slot_offset in [
        CHECKPOINT_SLOT_0_OFFSET,
        CHECKPOINT_SLOT_0_OFFSET + CHECKPOINT_SLOT_STRIDE,
    ] {
        file.seek(SeekFrom::Start(slot_offset))
            .map_err(Error::IOError)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).map_err(Error::IOError)?;
        let generation = u64::from_le_bytes(buf);
        if generation >= latest_generation {
            latest_generation = generation;
            latest_slot_offset = slot_offset;
        }
    }

    file.seek(SeekFrom::Start(
        latest_slot_offset + CHECKPOINT_SLOT_CHECKSUM_OFFSET,
    ))
    .map_err(Error::IOError)?;
    file.write_all(&0u32.to_le_bytes())
        .map_err(Error::IOError)?;
    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

fn active_data_file_path(dir: &std::path::Path) -> Result<std::path::PathBuf, Error> {
    let active_ordinal = active_file_ordinal(dir)?;

    for entry in std::fs::read_dir(dir).map_err(Error::IOError)? {
        let entry = entry.map_err(Error::IOError)?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with("data_") {
            continue;
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(Error::IOError)?;
        file.seek(SeekFrom::Start(16)).map_err(Error::IOError)?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).map_err(Error::IOError)?;
        if u64::from_le_bytes(buf) == active_ordinal {
            return Ok(path);
        }
    }

    Err(Error::IOError(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "active data file not found",
    )))
}

fn append_aligned_tail_garbage(dir: &std::path::Path, len: usize) -> Result<(), Error> {
    debug_assert_eq!(len % 16, 0);

    let path = active_data_file_path(dir)?;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(Error::IOError)?;
    file.write_all(&vec![0xA5; len]).map_err(Error::IOError)?;
    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

fn assert_rebuild_stats_non_zero(db: &CandyStore) {
    let stats = db.stats();
    assert!(
        stats.num_rebuilt_entries > 0,
        "expected rebuild to replay at least one entry"
    );
    assert!(
        stats.num_rebuild_purged_bytes > 0,
        "expected rebuild to trim a dirty file tail"
    );
}

fn wait_for_background_checkpoint(db: &CandyStore, previous_generation: u64) {
    let started_at = Instant::now();
    loop {
        let stats = db.stats();
        if stats.checkpoint_generation > previous_generation {
            return;
        }
        assert!(
            started_at.elapsed() < Duration::from_secs(3),
            "background checkpoint did not complete in time: prev_gen={previous_generation}, current_gen={}, uncheckpointed_bytes={}",
            stats.checkpoint_generation,
            stats.uncheckpointed_bytes,
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_checkpoint_generation_advance(db: &CandyStore, previous_generation: u64) {
    let started_at = Instant::now();
    loop {
        let stats = db.stats();
        if stats.checkpoint_generation > previous_generation {
            return;
        }
        assert!(
            started_at.elapsed() < Duration::from_secs(3),
            "background checkpoint generation did not advance in time: prev_gen={previous_generation}, current_gen={}",
            stats.checkpoint_generation,
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn test_recovery_after_dirty_shutdown() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key3", "val3")?;
        db.set("key2", "val2_updated")?;
        db.remove("key3")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        assert_eq!(db.get("key1")?, Some(b"val1".to_vec()));
        assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));
        assert!(db.get("key3")?.is_none());
    }

    Ok(())
}

#[test]
fn test_recovery_uses_persisted_hash_key_on_reopen() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let original_config = Config {
        hash_key: (1, 2),
        ..Config::default()
    };
    let different_config = Config {
        hash_key: (3, 4),
        ..original_config
    };

    {
        let db = CandyStore::open(dir.path(), original_config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), different_config)?;
        assert_eq!(db.get("key1")?, Some(b"val1".to_vec()));
        assert_eq!(db.get("key2")?, Some(b"val2".to_vec()));
        db.set("key3", "val3")?;
    }

    {
        let db = CandyStore::open(dir.path(), original_config)?;
        assert_eq!(db.get("key1")?, Some(b"val1".to_vec()));
        assert_eq!(db.get("key2")?, Some(b"val2".to_vec()));
        assert_eq!(db.get("key3")?, Some(b"val3".to_vec()));
    }

    Ok(())
}

#[test]
fn test_recovery_with_many_keys_and_splits() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::small_file_config();

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..500 {
            db.set(format!("k{i:04}"), format!("v{i:04}"))?;
        }
        for i in (0..500).step_by(3) {
            db.set(format!("k{i:04}"), format!("updated_{i}"))?;
        }
        for i in (0..500).step_by(7) {
            db.remove(format!("k{i:04}"))?;
        }
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;

        for i in 0..500 {
            let key = format!("k{i:04}");
            if i % 7 == 0 {
                assert!(db.get(&key)?.is_none(), "key {key} should be removed");
            } else if i % 3 == 0 {
                assert_eq!(
                    db.get(&key)?,
                    Some(format!("updated_{i}").into_bytes()),
                    "key {key} should be updated"
                );
            } else {
                assert_eq!(
                    db.get(&key)?,
                    Some(format!("v{i:04}").into_bytes()),
                    "key {key} should have original value"
                );
            }
        }
    }

    Ok(())
}

#[test]
fn test_rebuild_if_dirty_recovers_large_dataset_across_multiple_data_files() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 64 * 1024 * 1024,
        compaction_throughput_bytes_per_sec: 1024,
        ..Config::default()
    };
    const TARGET_NUM_DATA_FILES: u64 = 5;
    const VALUE_SIZE: usize = 60 * 1024;
    const NUM_REMOVALS: usize = 128;

    let total_keys;
    let removed_keys;

    {
        let db = CandyStore::open(dir.path(), config)?;
        let mut next_idx = 0usize;
        while db.stats().num_data_files < TARGET_NUM_DATA_FILES {
            let key = format!("large-rebuild-{next_idx:06}");
            let value = patterned_bytes_with_seed(VALUE_SIZE, next_idx);
            db.set(&key, &value)?;
            next_idx += 1;
        }

        total_keys = next_idx;
        removed_keys = ((total_keys - NUM_REMOVALS)..total_keys).collect::<HashSet<_>>();
        for idx in &removed_keys {
            let key = format!("large-rebuild-{idx:06}");
            assert!(
                db.remove(&key)?.is_some(),
                "expected {key} to exist before removal"
            );
        }

        let stats = db.stats();
        assert!(
            stats.num_data_files >= TARGET_NUM_DATA_FILES,
            "expected at least {TARGET_NUM_DATA_FILES} data files, got {}",
            stats.num_data_files
        );

        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(
            db.stats().num_data_files >= TARGET_NUM_DATA_FILES,
            "rebuild should preserve the multi-file dataset"
        );

        for idx in 0..total_keys {
            let key = format!("large-rebuild-{idx:06}");
            if removed_keys.contains(&idx) {
                assert!(
                    db.get(&key)?.is_none(),
                    "removed key {key} reappeared after rebuild"
                );
            } else {
                let expected = patterned_bytes_with_seed(VALUE_SIZE, idx);
                assert_eq!(
                    db.get(&key)?,
                    Some(expected),
                    "key {key} did not survive large rebuild correctly"
                );
            }
        }
    }

    Ok(())
}

#[test]
fn test_rebuild_if_dirty_recovers_with_corrupted_rows_checksum() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key2", "val2_updated")?;
        db.remove("key1")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(db.get("key1")?.is_none());
        assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(db.get("key1")?.is_none());
        assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));
    }

    Ok(())
}

#[test]
fn test_rebuild_if_dirty_rejects_unknown_data_entry_type() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db._abort_for_testing();
    }

    rewrite_first_data_entry_header(dir.path(), |header| (header & !(0b11 << 30)) | (0b10 << 30))?;

    match CandyStore::open(dir.path(), config) {
        Err(Error::IOError(io_err)) if io_err.kind() == std::io::ErrorKind::InvalidData => Ok(()),
        Err(err) => panic!("expected invalid-data error for unknown entry type, got {err}"),
        Ok(_) => panic!("expected open to fail for unknown entry type"),
    }
}

#[test]
fn test_rebuild_if_dirty_rejects_unknown_data_namespace() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db._abort_for_testing();
    }

    rewrite_first_data_entry_header(dir.path(), |header| {
        let cleared = header & !(0x3f << 24);
        cleared | (63 << 24)
    })?;

    match CandyStore::open(dir.path(), config) {
        Err(Error::IOError(io_err)) if io_err.kind() == std::io::ErrorKind::InvalidData => Ok(()),
        Err(err) => panic!("expected invalid-data error for unknown namespace, got {err}"),
        Ok(_) => panic!("expected open to fail for unknown namespace"),
    }
}

#[test]
fn test_open_rejects_duplicate_data_file_ordinals() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::small_file_config();

    {
        let db = CandyStore::open(dir.path(), config)?;
        for idx in 0..64 {
            db.set(format!("dup-ordinal-{idx:03}"), vec![b'x'; 512])?;
            if db.stats().num_data_files >= 2 {
                break;
            }
        }
        assert!(
            db.stats().num_data_files >= 2,
            "expected multiple data files"
        );
    }

    rewrite_data_file_ordinal(dir.path(), 1, 0x00bd_38a0_2a35_1cdf)?;

    match CandyStore::open(dir.path(), config) {
        Err(Error::IOError(io_err)) if io_err.kind() == std::io::ErrorKind::InvalidData => Ok(()),
        Err(err) => panic!("expected invalid-data error for duplicate ordinal, got {err}"),
        Ok(_) => panic!("expected open to fail for duplicate data file ordinals"),
    }
}

#[test]
fn test_rebuild_if_dirty_recovers_lists() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let list = b"rebuild-list";

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set_in_list(list, b"a", b"1")?;
        db.set_in_list(list, b"b", b"2")?;
        db.set_in_list(list, b"c", b"3")?;
        db.set_in_list(list, b"b", b"2b")?;
        db.remove_from_list(list, b"a")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert_eq!(db.list_len(list)?, 2);
        assert_eq!(db.get_from_list(list, b"a")?, None);
        assert_eq!(db.get_from_list(list, b"b")?, Some(b"2b".to_vec()));
        assert_eq!(db.get_from_list(list, b"c")?, Some(b"3".to_vec()));

        let items: Vec<_> = db.iter_list(list).collect::<Result<_, _>>()?;
        assert_eq!(
            items,
            vec![
                (b"b".to_vec(), b"2b".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
    }

    Ok(())
}

#[test]
fn test_rebuild_if_dirty_recovers_queues() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let queue = b"rebuild-queue";

    let first_idx;
    let keep_idx;
    let removed_idx;

    {
        let db = CandyStore::open(dir.path(), config)?;
        first_idx = db.push_to_queue_tail(queue, b"tail-1")?;
        keep_idx = db.push_to_queue_tail(queue, b"tail-2")?;
        removed_idx = db.push_to_queue_tail(queue, b"tail-3")?;
        db.push_to_queue_head(queue, b"head-0")?;

        assert_eq!(db.pop_queue_head(queue)?, Some(b"head-0".to_vec()));
        assert_eq!(
            db.remove_from_queue(queue, removed_idx)?,
            Some(b"tail-3".to_vec())
        );
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert_eq!(db.queue_len(queue)?, 2);
        assert_eq!(db.peek_queue_head(queue)?, Some(b"tail-1".to_vec()));
        assert_eq!(db.peek_queue_tail(queue)?, Some(b"tail-2".to_vec()));
        assert_eq!(db.remove_from_queue(queue, removed_idx)?, None);

        let items: Vec<_> = db.iter_queue(queue).collect::<Result<_, _>>()?;
        assert_eq!(
            items,
            vec![
                (first_idx, b"tail-1".to_vec()),
                (keep_idx, b"tail-2".to_vec()),
            ]
        );
    }

    Ok(())
}

#[test]
fn test_rebuild_if_dirty_recovers_typed_data() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let list_key = 7u32;
    let queue_key = 9u32;

    {
        let store = Arc::new(CandyStore::open(dir.path(), config)?);
        let typed_kv = CandyTypedStore::<u32, String>::new(Arc::clone(&store));
        let typed_list = CandyTypedList::<u32, u32, String>::new(Arc::clone(&store));
        let typed_queue = CandyTypedDeque::<u32, u32>::new(Arc::clone(&store));

        typed_kv.set(&1u32, &"one".to_string())?;
        typed_kv.set(&1u32, &"uno".to_string())?;
        typed_kv.set(&2u32, &"two".to_string())?;
        assert_eq!(typed_kv.remove(&2u32)?, Some("two".to_string()));

        typed_list.set(&list_key, &1u32, &"a".to_string())?;
        typed_list.set(&list_key, &2u32, &"b".to_string())?;
        typed_list.set(&list_key, &3u32, &"c".to_string())?;
        assert_eq!(typed_list.remove(&list_key, &2u32)?, Some("b".to_string()));

        typed_queue.push_tail(&queue_key, &10u32)?;
        typed_queue.push_tail(&queue_key, &20u32)?;
        typed_queue.push_head(&queue_key, &5u32)?;
        assert_eq!(typed_queue.pop_tail(&queue_key)?, Some(20u32));

        drop(typed_queue);
        drop(typed_list);
        drop(typed_kv);
        Arc::into_inner(store).unwrap()._abort_for_testing();
    }

    {
        let store = Arc::new(CandyStore::open(dir.path(), config)?);
        let typed_kv = CandyTypedStore::<u32, String>::new(Arc::clone(&store));
        let typed_list = CandyTypedList::<u32, u32, String>::new(Arc::clone(&store));
        let typed_queue = CandyTypedDeque::<u32, u32>::new(Arc::clone(&store));

        assert_eq!(typed_kv.get(&1u32)?, Some("uno".to_string()));
        assert_eq!(typed_kv.get(&2u32)?, None);

        let typed_list_items: Vec<_> = typed_list.iter(&list_key).collect::<Result<_, _>>()?;
        assert_eq!(
            typed_list_items,
            vec![(1u32, "a".to_string()), (3u32, "c".to_string())]
        );

        let typed_queue_items: Vec<_> = typed_queue.iter(&queue_key).collect::<Result<_, _>>()?;
        assert_eq!(typed_queue_items.len(), 2);
        assert_eq!(typed_queue_items[0].1, 5u32);
        assert_eq!(typed_queue_items[1].1, 10u32);
        assert_eq!(typed_queue.peek_head(&queue_key)?, Some(5u32));
        assert_eq!(typed_queue.peek_tail(&queue_key)?, Some(10u32));
        assert_eq!(
            typed_queue.peek_head_with_idx(&queue_key)?,
            Some(typed_queue_items[0])
        );
        assert_eq!(
            typed_queue.peek_tail_with_idx(&queue_key)?,
            Some(typed_queue_items[1])
        );
    }

    Ok(())
}

#[test]
fn test_reset_on_invalid_data_clears_corrupt_store() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        reset_on_invalid_data: true,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key", "value")?;
    }

    fs::write(dir.path().join("index"), b"bad").map_err(Error::IOError)?;
    fs::write(dir.path().join("rows"), b"bad").map_err(Error::IOError)?;
    fs::write(dir.path().join("extra.txt"), b"junk").map_err(Error::IOError)?;
    fs::create_dir(dir.path().join("extra_dir")).map_err(Error::IOError)?;
    fs::write(dir.path().join("extra_dir").join("nested.txt"), b"junk").map_err(Error::IOError)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(db.get("key")?.is_none());
    assert!(!dir.path().join("extra.txt").exists());
    assert!(!dir.path().join("extra_dir").exists());

    db.set("fresh", "value")?;
    assert_eq!(db.get("fresh")?, Some(b"value".to_vec()));

    Ok(())
}

#[test]
fn test_reset_on_invalid_data_clears_recovery_time_corruption() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        reset_on_invalid_data: true,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key", "value")?;
        db._abort_for_testing();
    }

    rewrite_first_data_entry_header(dir.path(), |header| (header & !(0b11 << 30)) | (0b10 << 30))?;
    fs::write(dir.path().join("extra.txt"), b"junk").map_err(Error::IOError)?;
    fs::create_dir(dir.path().join("extra_dir")).map_err(Error::IOError)?;
    fs::write(dir.path().join("extra_dir").join("nested.txt"), b"junk").map_err(Error::IOError)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(db.get("key")?.is_none());
    assert!(!dir.path().join("extra.txt").exists());
    assert!(!dir.path().join("extra_dir").exists());

    db.set("fresh", "value")?;
    assert_eq!(db.get("fresh")?, Some(b"value".to_vec()));

    Ok(())
}

#[test]
fn test_recover_from_truncated_data_file() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    {
        let db = candystore::CandyStore::open(dir.path(), candystore::Config::default())?;
        db.set("key1", "value1")?;
        db.set("key2", "value2")?;
    }

    // Corrupt the data file by truncating the last 5 bytes
    let data_file = std::fs::read_dir(dir.path())?
        .filter_map(|res| res.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("data_"))
        .unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(data_file.path())?;
    let len = file.metadata()?.len();
    file.set_len(len - 5)?;

    // We expect clear recovery (key2 was truncated, thus doesn't exist, but key1 is readable)
    let db = candystore::CandyStore::open(dir.path(), candystore::Config::default())?;
    assert_eq!(db.get("key1")?.as_deref(), Some("value1".as_bytes()));
    assert_eq!(db.get("key2")?, None);
    assert_eq!(db.num_items(), 1);
    Ok(())
}

#[test]
fn test_rebuild_if_dirty_recovers_from_invalid_commit_offset_without_double_counting()
-> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "value1")?;
        db.set("key2", "value2")?;
        db.set("key2", "value2_updated")?;
        db.set("key3", "value3")?;
    }

    write_commit_cursor(dir.path(), 5)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert_eq!(db.get("key1")?, Some(b"value1".to_vec()));
    assert_eq!(db.get("key2")?, Some(b"value2_updated".to_vec()));
    assert_eq!(db.get("key3")?, Some(b"value3".to_vec()));
    assert_eq!(db.num_items(), 3);
    Ok(())
}

#[test]
fn test_progressive_rebuild_resumes_from_checkpoint() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    // Phase 1: write data across multiple files, then crash.
    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..100 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }
    append_aligned_tail_garbage(dir.path(), 64)?;

    // Phase 2: reopen triggers rebuild. Verify all data survived.
    {
        let db = CandyStore::open(dir.path(), config)?;
        assert_rebuild_stats_non_zero(&db);
        for i in 0..100 {
            assert_eq!(
                db.get(format!("key{i:04}"))?,
                Some(format!("val{i:04}").into_bytes()),
                "key{i:04} missing after full rebuild"
            );
        }
        // Clean shutdown after rebuild should preserve all recovered data.
    }

    Ok(())
}

#[test]
fn test_progressive_rebuild_survives_interrupted_rebuild() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    // Phase 1: write data, crash.
    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..100 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    // Phase 2: reopen triggers rebuild which completes. Then crash again.
    {
        let db = CandyStore::open(dir.path(), config)?;
        // Write more data on top of the rebuilt index.
        for i in 100..150 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }
    append_aligned_tail_garbage(dir.path(), 64)?;

    // Phase 3: another rebuild should start from the persisted replay cursor
    // and recover everything written before the second crash.
    {
        let db = CandyStore::open(dir.path(), config)?;
        assert_rebuild_stats_non_zero(&db);
        for i in 0..150 {
            assert_eq!(
                db.get(format!("key{i:04}"))?,
                Some(format!("val{i:04}").into_bytes()),
                "key{i:04} missing after second rebuild"
            );
        }
    }

    Ok(())
}

#[test]
fn test_progressive_rebuild_with_trust_strategy_resumes_pending() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    // Phase 1: write data, crash.
    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..100 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    // Phase 2: reopen and write more, then crash.
    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 100..200 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }
    append_aligned_tail_garbage(dir.path(), 64)?;

    // Phase 3: reopen — recovery replays from the commit cursor, so all
    // data from phases 1+2 should be accessible.
    {
        let db = CandyStore::open(dir.path(), config)?;
        assert_rebuild_stats_non_zero(&db);
        for i in 0..200 {
            assert_eq!(
                db.get(format!("key{i:04}"))?,
                Some(format!("val{i:04}").into_bytes()),
                "key{i:04} missing after trust-or-rebuild"
            );
        }
    }

    Ok(())
}

#[test]
fn test_checkpoint_advances_recovery_cursor() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..100 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db.checkpoint()?;
        db._abort_for_testing();
    }

    let db = CandyStore::open(dir.path(), config)?;
    let stats = db.stats();
    assert_eq!(stats.num_rebuilt_entries, 0);
    assert_eq!(stats.num_rebuild_purged_bytes, 0);
    for i in 0..100 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            Some(format!("val{i:04}").into_bytes()),
            "key{i:04} missing after checkpointed reopen"
        );
    }

    Ok(())
}

#[test]
fn test_checkpoint_delta_bytes_advances_recovery_cursor_in_background() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 8 * 1024,
        compaction_min_threshold: u32::MAX,
        compaction_throughput_bytes_per_sec: 0,
        checkpoint_interval: None,
        checkpoint_delta_bytes: Some(512),
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        let initial_generation = db.stats().checkpoint_generation;
        for i in 0..24 {
            db.set(
                format!("delta-bg-key{i:04}"),
                format!("delta-bg-val{i:04}-{}", "x".repeat(96)),
            )?;
        }
        wait_for_background_checkpoint(&db, initial_generation);
        db._abort_for_testing();
    }

    let db = CandyStore::open(dir.path(), config)?;
    let stats = db.stats();
    assert!(
        stats.num_rebuilt_entries < 24,
        "threshold-triggered background checkpoint should avoid replaying the entire store"
    );
    assert_eq!(stats.num_rebuild_purged_bytes, 0);
    for i in 0..24 {
        assert_eq!(
            db.get(format!("delta-bg-key{i:04}"))?,
            Some(format!("delta-bg-val{i:04}-{}", "x".repeat(96)).into_bytes()),
            "delta-bg-key{i:04} missing after threshold-triggered background checkpoint"
        );
    }

    Ok(())
}

#[test]
fn test_checkpoint_interval_advances_recovery_cursor_in_background() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 8 * 1024,
        compaction_min_threshold: u32::MAX,
        compaction_throughput_bytes_per_sec: 0,
        checkpoint_interval: Some(Duration::from_millis(50)),
        checkpoint_delta_bytes: None,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        let initial_generation = db.stats().checkpoint_generation;
        for i in 0..24 {
            db.set(
                format!("interval-bg-key{i:04}"),
                format!("interval-bg-val{i:04}-{}", "y".repeat(96)),
            )?;
        }
        wait_for_background_checkpoint(&db, initial_generation);
        db._abort_for_testing();
    }

    let db = CandyStore::open(dir.path(), config)?;
    let stats = db.stats();
    assert_eq!(stats.num_rebuilt_entries, 0);
    assert_eq!(stats.num_rebuild_purged_bytes, 0);
    for i in 0..24 {
        assert_eq!(
            db.get(format!("interval-bg-key{i:04}"))?,
            Some(format!("interval-bg-val{i:04}-{}", "y".repeat(96)).into_bytes()),
            "interval-bg-key{i:04} missing after interval-triggered background checkpoint"
        );
    }

    Ok(())
}

#[test]
fn test_rotation_advances_recovery_cursor_in_background() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 2048,
        compaction_min_threshold: u32::MAX,
        compaction_throughput_bytes_per_sec: 0,
        checkpoint_interval: None,
        checkpoint_delta_bytes: None,
        ..Config::default()
    };

    let total_keys;
    {
        let db = CandyStore::open(dir.path(), config)?;
        let initial_generation = db.stats().checkpoint_generation;
        let mut next_idx = 0usize;
        while db.stats().num_data_files < 2 {
            db.set(
                format!("rotate-bg-key{next_idx:04}"),
                format!("rotate-bg-val{next_idx:04}-{}", "z".repeat(96)),
            )?;
            next_idx += 1;
        }
        total_keys = next_idx;
        wait_for_checkpoint_generation_advance(&db, initial_generation);
        db._abort_for_testing();
    }

    let db = CandyStore::open(dir.path(), config)?;
    let stats = db.stats();
    assert!(
        stats.num_rebuilt_entries < total_keys as u64,
        "rotation-triggered checkpoint should avoid replaying the entire store"
    );
    assert_eq!(stats.num_rebuild_purged_bytes, 0);
    for i in 0..total_keys {
        assert_eq!(
            db.get(format!("rotate-bg-key{i:04}"))?,
            Some(format!("rotate-bg-val{i:04}-{}", "z".repeat(96)).into_bytes()),
            "rotate-bg-key{i:04} missing after rotation-triggered background checkpoint"
        );
    }

    Ok(())
}

#[test]
fn test_progressive_rebuild_ignores_bogus_checkpoint_offset() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..100 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    // Write a bogus commit cursor offset beyond any data — rebuild should
    // fall back to replaying from offset 0.
    write_commit_cursor(dir.path(), 0xFFFF_FFFF)?;

    let db = CandyStore::open(dir.path(), config)?;
    for i in 0..100 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            Some(format!("val{i:04}").into_bytes()),
            "key{i:04} missing after restart-from-scratch rebuild"
        );
    }

    Ok(())
}

#[test]
fn test_progressive_rebuild_falls_back_to_older_valid_checkpoint_slot() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 1024,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..100 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    write_commit_cursor(dir.path(), 0)?;
    write_commit_cursor(dir.path(), 0xFFFF_FFFF)?;
    corrupt_latest_checkpoint_slot_checksum(dir.path())?;

    let db = CandyStore::open(dir.path(), config)?;
    for i in 0..100 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            Some(format!("val{i:04}").into_bytes()),
            "key{i:04} missing after fallback to older valid checkpoint slot"
        );
    }

    Ok(())
}

#[test]
fn test_clean_reopen_rebuilds_invalid_active_checkpoint_across_multiple_data_files()
-> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::small_file_config();

    let total_base_keys;

    {
        let db = CandyStore::open(dir.path(), config)?;
        let mut next_idx = 0usize;
        while db.stats().num_data_files < 3 {
            let key = format!("multifile-base-{next_idx:04}");
            let value = patterned_bytes_with_seed(512, next_idx);
            db.set(&key, &value)?;
            next_idx += 1;
        }
        total_base_keys = next_idx;

        assert!(
            db.stats().num_data_files >= 3,
            "expected multiple data files before corrupting checkpoint"
        );
        assert_eq!(db.num_items(), total_base_keys);
    }

    write_commit_cursor(dir.path(), 0xFFFF_FFFF)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(
        db.stats().num_data_files >= 3,
        "expected the multi-file layout to survive recovery"
    );

    for idx in 0..total_base_keys {
        let key = format!("multifile-base-{idx:04}");
        assert_eq!(db.get(&key)?, Some(patterned_bytes_with_seed(512, idx)));
    }

    assert_eq!(db.num_items(), total_base_keys);

    Ok(())
}

#[test]
fn test_recovery_replays_later_files_after_checkpointing_an_older_file() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::small_file_config();

    let total_keys;

    {
        let db = CandyStore::open(dir.path(), config)?;
        let mut next_idx = 0usize;
        while db.stats().num_data_files < 3 {
            let key = format!("older-cursor-{next_idx:04}");
            let value = patterned_bytes_with_seed(512, next_idx);
            db.set(&key, &value)?;
            next_idx += 1;
        }
        total_keys = next_idx;
    }

    let files = data_files_by_ordinal(dir.path())?;
    assert!(
        files.len() >= 3,
        "expected multiple data files for replay test"
    );
    let (older_ordinal, older_used_bytes) = files[0];
    write_commit_cursor_for_ordinal(dir.path(), older_ordinal, older_used_bytes)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(
        db.stats().num_rebuilt_entries > 0,
        "expected recovery to replay later files after rewinding commit cursor"
    );
    for idx in 0..total_keys {
        let key = format!("older-cursor-{idx:04}");
        assert_eq!(db.get(&key)?, Some(patterned_bytes_with_seed(512, idx)));
    }
    assert_eq!(db.num_items(), total_keys);

    Ok(())
}

#[test]
fn test_recovery_replays_later_files_when_checkpoint_file_is_missing() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 16 * 1024,
        compaction_min_threshold: u32::MAX,
        compaction_throughput_bytes_per_sec: 0,
        ..Config::default()
    };

    let mut expected = Vec::new();
    {
        let db = CandyStore::open(dir.path(), config)?;
        let mut update_idx = 0usize;
        let final_hot = loop {
            let value = patterned_bytes_with_seed(6 * 1024, update_idx);
            db.set("hot", &value)?;
            update_idx += 1;
            if db.stats().num_data_files >= 5 {
                break value;
            }
        };
        expected.push(("hot".to_owned(), final_hot));

        for idx in 0..4usize {
            let key = format!("tail-live-{idx:02}");
            let value = patterned_bytes_with_seed(2048, 10_000 + idx);
            db.set(&key, &value)?;
            expected.push((key, value));
        }
    }

    let files = data_file_records_by_ordinal(dir.path())?;
    assert!(
        files.len() >= 4,
        "expected enough rotated files to simulate a missing checkpoint file"
    );

    let (_, missing_ordinal, missing_used_bytes, missing_path) = &files[1];
    write_commit_cursor_for_ordinal(dir.path(), *missing_ordinal, *missing_used_bytes)?;
    fs::remove_file(missing_path).map_err(Error::IOError)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(
        db.stats().num_rebuilt_entries > 0,
        "expected recovery to replay entries after a missing checkpoint file"
    );
    for (key, value) in expected {
        assert_eq!(
            db.get(&key)?,
            Some(value),
            "{key} missing after replaying past a missing checkpoint file"
        );
    }

    Ok(())
}

#[test]
fn test_recovery_ignores_missing_compacted_files_before_checkpoint_cursor() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 16 * 1024,
        compaction_min_threshold: u32::MAX,
        compaction_throughput_bytes_per_sec: 0,
        ..Config::default()
    };

    let mut expected = Vec::new();
    {
        let db = CandyStore::open(dir.path(), config)?;
        let mut update_idx = 0usize;
        let final_hot = loop {
            let value = patterned_bytes_with_seed(6 * 1024, 20_000 + update_idx);
            db.set("hot", &value)?;
            update_idx += 1;
            if db.stats().num_data_files >= 5 {
                break value;
            }
        };
        expected.push(("hot".to_owned(), final_hot));

        for idx in 0..6usize {
            let key = format!("post-cursor-live-{idx:02}");
            let value = patterned_bytes_with_seed(1536, 30_000 + idx);
            db.set(&key, &value)?;
            expected.push((key, value));
        }
    }

    let files = data_file_records_by_ordinal(dir.path())?;
    assert!(
        files.len() >= 5,
        "expected enough files to simulate compacted files before the checkpoint cursor"
    );

    let (_, checkpoint_ordinal, checkpoint_used_bytes, _) = &files[2];
    let (_, _, _, missing_path) = &files[0];
    write_commit_cursor_for_ordinal(dir.path(), *checkpoint_ordinal, *checkpoint_used_bytes)?;
    fs::remove_file(missing_path).map_err(Error::IOError)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(
        db.stats().num_rebuilt_entries > 0,
        "expected recovery to replay entries after skipping compacted files before the cursor"
    );
    for (key, value) in expected {
        assert_eq!(
            db.get(&key)?,
            Some(value),
            "{key} missing after skipping compacted files before the checkpoint cursor"
        );
    }

    Ok(())
}
