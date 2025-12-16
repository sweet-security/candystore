use candystore::{CandyStore, Config, RecoveryMode};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

#[test]
fn test_truncated_data_file_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        ..Default::default()
    };

    let num_items = 1000;

    // 1. Create and populate DB
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..num_items {
            store
                .set(
                    format!("key:{:04}", i).as_bytes(),
                    format!("val:{:04}", i).as_bytes(),
                )
                .unwrap();
        }
        // Ensure everything is flushed to disk
        store.flush().unwrap();
    }

    let data_path = dir.path().join("data_00000.db");
    let file_len = std::fs::metadata(&data_path).unwrap().len();

    // 2. Truncate the data file to remove the last ~100 items
    // Each item is roughly: header(8) + key(8) + val(8) = 24 bytes.
    // 100 items ~ 2400 bytes.
    let truncate_len = file_len - 2400;
    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        file.set_len(truncate_len).unwrap();
    }

    // 3. Reopen the store
    // The index still points to the truncated locations.
    // We expect get() to return None for the truncated items because read_kv will fail/return error,
    // and find_match_in_row treats errors as "not found" (or continues searching).
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    let mut missing_count = 0;
    for i in 0..num_items {
        let key = format!("key:{:04}", i);
        let val = store.get(key.as_bytes()).unwrap();
        if val.is_none() {
            missing_count += 1;
        } else {
            assert_eq!(val, Some(format!("val:{:04}", i).as_bytes().to_vec()));
        }
    }

    println!("Missing items after truncation: {}", missing_count);
    assert!(missing_count > 0, "Should have lost some items");
    assert!(missing_count < num_items, "Should not have lost all items");
}

#[test]
fn test_truncated_data_file_queues() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        ..Default::default()
    };

    let num_queues = 100;
    let items_per_queue = 10;

    // 1. Create and populate DB
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..items_per_queue {
            for queue_idx in 0..num_queues {
                store
                    .push_to_queue_tail(
                        format!("queue:{}", queue_idx).as_str(),
                        format!("val:{:04}", i).as_bytes(),
                    )
                    .unwrap();
            }
        }
        store.flush().unwrap();
    }

    let data_path = dir.path().join("data_00000.db");
    let file_len = std::fs::metadata(&data_path).unwrap().len();

    // 2. Truncate the data file to remove the last ~100 items
    // Each item is roughly: header(8) + key(8) + val(8) = 24 bytes + queue overhead.
    // Let's assume 40 bytes per item.
    // We want to remove roughly 100 items (10 per queue).
    let truncate_amount = 2400;
    let truncate_len = file_len - truncate_amount;

    println!("File len: {}, Truncate len: {}", file_len, truncate_len);

    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        file.set_len(truncate_len).unwrap();
    }

    // 3. Reopen
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    let mut total_missing = 0;
    for queue_idx in 0..num_queues {
        let queue_key = format!("queue:{}", queue_idx);
        let items: Vec<_> = store.iter_queue(&queue_key).collect();

        // We expect fewer than 100 items
        let count = items.len();
        assert!(
            count <= items_per_queue,
            "Queue {} should have missing items (found {})",
            queue_idx,
            count
        );

        if count == 0 {
            println!("Queue {} is completely empty", queue_idx);
        }

        total_missing += items_per_queue - count;

        // Verify the items we DO have are correct and in order
        let mut first_q_idx = None;
        for (idx, res) in items.into_iter().enumerate() {
            let (q_idx, val) = res.unwrap();
            let expected_val = format!("val:{:04}", idx);

            assert_eq!(val, expected_val.as_bytes());

            if first_q_idx.is_none() {
                first_q_idx = Some(q_idx);
            }
            assert_eq!(q_idx, first_q_idx.unwrap() + idx);
        }
    }

    println!("Total missing items: {}", total_missing);
    assert!(total_missing > 0);
    assert!(total_missing < num_queues * items_per_queue); // not all lost
}

#[test]
fn test_zeroed_tail_data_file_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        ..Default::default()
    };

    let num_items = 1000;

    // 1. Create and populate DB
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..num_items {
            store
                .set(
                    format!("key:{:04}", i).as_bytes(),
                    format!("val:{:04}", i).as_bytes(),
                )
                .unwrap();
        }
        store.flush().unwrap();
    }

    let data_path = dir.path().join("data_00000.db");
    let file_len = std::fs::metadata(&data_path).unwrap().len();

    // 2. Zero out the last ~100 items
    let zero_len = 2400;
    let start_offset = file_len - zero_len;
    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        let zeros = vec![0u8; zero_len as usize];
        file.write_all_at(&zeros, start_offset).unwrap();
    }

    // 3. Reopen
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    let mut missing_count = 0;
    for i in 0..num_items {
        let key = format!("key:{:04}", i);
        let val = store.get(key.as_bytes()).unwrap();
        if val.is_none() {
            missing_count += 1;
        } else {
            assert_eq!(val, Some(format!("val:{:04}", i).as_bytes().to_vec()));
        }
    }

    println!("Missing items after zeroing: {}", missing_count);
    assert!(missing_count > 0, "Should have lost some items");
    assert!(missing_count < num_items, "Should not have lost all items");
}

#[test]
fn test_truncated_data_file_lists() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        recovery_mode: RecoveryMode::RebuildIndexIfCorrupted,
        ..Default::default()
    };

    let num_lists = 100;
    let items_per_list = 10;

    // 1. Create and populate DB
    {
        let store = CandyStore::open(dir.path(), config.clone()).unwrap();
        for i in 0..items_per_list {
            for list_idx in 0..num_lists {
                store
                    .set_in_list(
                        format!("list:{}", list_idx).as_str(),
                        format!("key:{:04}", i).as_bytes(),
                        format!("val:{:04}", i).as_bytes(),
                    )
                    .unwrap();
            }
        }
        store.flush().unwrap();
    }

    let data_path = dir.path().join("data_00000.db");
    let file_len = std::fs::metadata(&data_path).unwrap().len();

    // 2. Truncate the data file to remove the last ~100 items
    // Each item is roughly: header(8) + key(8) + val(8) = 24 bytes.
    // But list items might have more overhead (pointers).
    // Let's assume 24 bytes per item.
    // We want to remove roughly 100 items (10 per list).
    let truncate_amount = 2400;
    let truncate_len = file_len - truncate_amount;

    println!("File len: {}, Truncate len: {}", file_len, truncate_len);

    {
        let file = OpenOptions::new().write(true).open(&data_path).unwrap();
        file.set_len(truncate_len).unwrap();
    }

    // 3. Reopen
    let store = CandyStore::open(dir.path(), config.clone()).unwrap();

    let mut total_missing = 0;
    for list_idx in 0..num_lists {
        let list_key = format!("list:{}", list_idx);
        let items: Vec<_> = store.iter_list(&list_key).collect();

        // We expect fewer than 100 items
        let count = items.len();
        assert!(
            count <= items_per_list,
            "List {} should have missing items (found {})",
            list_idx,
            count
        );

        if count == 0 {
            println!("List {} is completely empty", list_idx);
        }

        total_missing += items_per_list - count;

        // Verify the items we DO have are correct and in order
        for (idx, res) in items.into_iter().enumerate() {
            let (k, v) = res.unwrap();
            let expected_key = format!("key:{:04}", idx);
            let expected_val = format!("val:{:04}", idx);

            assert_eq!(k, expected_key.as_bytes());
            assert_eq!(v, expected_val.as_bytes());
        }
    }

    println!("Total missing items: {}", total_missing);
    assert!(total_missing > 0);
    assert!(total_missing < num_lists * items_per_list); // not all lost
}
