mod common;

use std::collections::HashSet;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use candystore::{
    CandyStore, CandyTypedDeque, CandyTypedList, CandyTypedStore, Config, Error, RebuildStrategy,
};
use tempfile::tempdir;

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

#[test]
fn test_clean_shutdown_flag() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        assert!(db.was_clean_shutdown());
        db.set("hello", "world")?;
    }

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        assert!(db.was_clean_shutdown());
        assert_eq!(db.get("hello")?, Some("world".into()));
    }

    Ok(())
}

#[test]
fn test_dirty_shutdown_detected() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::rebuild_if_dirty_config();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("hello", "world")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(!db.was_clean_shutdown());
    }

    Ok(())
}

#[test]
fn test_recovery_after_dirty_shutdown() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::rebuild_if_dirty_config();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key3", "val3")?;
        db.set("key2", "val2_updated")?;
        db.remove("key3")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(!db.was_clean_shutdown());
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
        rebuild_strategy: RebuildStrategy::RebuildIfDirty,
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
        assert!(!db.was_clean_shutdown());
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
fn test_recovery_rebuilds_waste_stats() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::rebuild_if_dirty_config();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key3", "val3")?;
        db.set("key2", "val2_updated")?;
        db.remove("key3")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(!db.was_clean_shutdown());
        let stats = db.stats();
        assert_eq!(stats.waste_bytes, 64);
    }

    Ok(())
}

#[test]
fn test_recovery_with_many_keys_and_splits() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let mut config = common::small_file_config();
    config.rebuild_strategy = RebuildStrategy::RebuildIfDirty;

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
        assert!(!db.was_clean_shutdown());

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
        rebuild_strategy: RebuildStrategy::RebuildIfDirty,
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
        assert!(!db.was_clean_shutdown());
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
    let config = common::rebuild_if_dirty_config();

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key2", "val2_updated")?;
        db.remove("key1")?;
        db._abort_for_testing();
    }

    common::corrupt_first_row_checksum(dir.path());

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(!db.was_clean_shutdown());
        assert!(db.get("key1")?.is_none());
        assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(db.was_clean_shutdown());
        assert!(db.get("key1")?.is_none());
        assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));
    }

    Ok(())
}

#[test]
fn test_rebuild_if_dirty_rejects_unknown_data_entry_type() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = common::rebuild_if_dirty_config();

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
    let config = common::rebuild_if_dirty_config();

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
    let config = common::rebuild_if_dirty_config();
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
        assert!(!db.was_clean_shutdown());
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
    let config = common::rebuild_if_dirty_config();
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
        assert!(!db.was_clean_shutdown());
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
    let config = common::rebuild_if_dirty_config();
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

        assert!(!store.was_clean_shutdown());

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
fn test_fail_if_dirty_rejects_reopen() -> Result<(), Error> {
    let dir = tempdir().unwrap();

    {
        let db = CandyStore::open(dir.path(), Config::default())?;
        db.set("key", "value")?;
        db._abort_for_testing();
    }

    assert!(matches!(
        CandyStore::open(dir.path(), Config::default()),
        Err(Error::DirtyIndex)
    ));

    Ok(())
}

#[test]
fn test_trust_dirty_index_if_checksum_correct_or_fail() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        rebuild_strategy: RebuildStrategy::TrustDirtyIndexIfChecksumCorrectOrFail,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key2", "val2_updated")?;
        db._abort_for_testing();
    }

    {
        let db = CandyStore::open(dir.path(), config)?;
        assert!(!db.was_clean_shutdown());
        assert_eq!(db.get("key1")?, Some(b"val1".to_vec()));
        assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));
        let waste_after_trust = db.stats().waste_bytes;
        drop(db);

        let waste_after_clean = CandyStore::open(dir.path(), config)?.stats().waste_bytes;
        assert_eq!(waste_after_clean, waste_after_trust);
    }

    Ok(())
}

#[test]
fn test_trust_dirty_index_fails_on_checksum_mismatch() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        rebuild_strategy: RebuildStrategy::TrustDirtyIndexIfChecksumCorrectOrFail,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key", "value")?;
        db._abort_for_testing();
    }

    common::corrupt_first_row_checksum(dir.path());

    assert!(matches!(
        CandyStore::open(dir.path(), config),
        Err(Error::IOError(io_err)) if io_err.kind() == std::io::ErrorKind::InvalidData
    ));

    Ok(())
}

#[test]
fn test_trust_dirty_index_rebuilds_on_checksum_mismatch() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        rebuild_strategy: RebuildStrategy::TrustDirtyIndexIfChecksumCorrectOrRebuild,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
        db.set("key2", "val2_updated")?;
        db._abort_for_testing();
    }

    common::corrupt_first_row_checksum(dir.path());

    let db = CandyStore::open(dir.path(), config)?;
    assert!(!db.was_clean_shutdown());
    assert_eq!(db.get("key1")?, Some(b"val1".to_vec()));
    assert_eq!(db.get("key2")?, Some(b"val2_updated".to_vec()));

    Ok(())
}

#[test]
fn test_reset_db_if_dirty_clears_state() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        rebuild_strategy: RebuildStrategy::ResetDBIfDirty,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key", "value")?;
        db._abort_for_testing();
    }

    fs::write(dir.path().join("extra.txt"), b"junk").map_err(Error::IOError)?;
    fs::create_dir(dir.path().join("extra_dir")).map_err(Error::IOError)?;
    fs::write(dir.path().join("extra_dir").join("nested.txt"), b"junk").map_err(Error::IOError)?;

    let db = CandyStore::open(dir.path(), config)?;
    assert!(!db.was_clean_shutdown());
    assert!(db.get("key")?.is_none());
    assert!(!dir.path().join("extra.txt").exists());
    assert!(!dir.path().join("extra_dir").exists());

    Ok(())
}

#[test]
fn test_reset_on_invalid_data_clears_corrupt_store() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        rebuild_strategy: RebuildStrategy::RebuildIfDirty,
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
    assert!(!db.was_clean_shutdown());
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
    Ok(())
}
