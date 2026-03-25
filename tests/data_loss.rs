use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use candystore::{CandyStore, Config};

fn first_data_file_path(dir: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("data_"))
        .unwrap()
        .path()
}

fn zero_range(path: &std::path::Path, start: u64, len: usize) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();
    file.write_all(&vec![0u8; len]).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn test_zeroed_tail_data_file_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default();

    {
        let store = CandyStore::open(dir.path(), config).unwrap();
        for idx in 0..1000 {
            store
                .set(
                    format!("key:{idx:04}").as_bytes(),
                    format!("val:{idx:04}").as_bytes(),
                )
                .unwrap();
        }
        store.flush().unwrap();
    }

    let data_path = first_data_file_path(dir.path());
    let file_len = std::fs::metadata(&data_path).unwrap().len();
    let zero_len = 2400usize;
    zero_range(&data_path, file_len - zero_len as u64, zero_len);

    let store = CandyStore::open(dir.path(), config).unwrap();
    let mut missing = 0;
    for idx in 0..1000 {
        let key = format!("key:{idx:04}");
        match store.get(key.as_bytes()).unwrap() {
            Some(value) => assert_eq!(value, format!("val:{idx:04}").into_bytes()),
            None => missing += 1,
        }
    }

    assert!(missing > 0);
    assert!(missing < 1000);
}

#[test]
fn test_truncated_data_file_queues() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let num_queues = 50;
    let items_per_queue = 10;

    {
        let store = CandyStore::open(dir.path(), config).unwrap();
        for item_idx in 0..items_per_queue {
            for queue_idx in 0..num_queues {
                store
                    .push_to_queue_tail(
                        format!("queue:{queue_idx}").as_str(),
                        format!("val:{item_idx:04}").as_bytes(),
                    )
                    .unwrap();
            }
        }
        store.flush().unwrap();
    }

    let data_path = first_data_file_path(dir.path());
    let file = OpenOptions::new().write(true).open(&data_path).unwrap();
    let file_len = file.metadata().unwrap().len();
    file.set_len(file_len - 2400).unwrap();

    let store = CandyStore::open(dir.path(), config).unwrap();

    let mut total_missing = 0;
    for queue_idx in 0..num_queues {
        let queue_key = format!("queue:{queue_idx}");
        let items: Vec<_> = store
            .iter_queue(&queue_key)
            .collect::<candystore::Result<Vec<_>>>()
            .unwrap();

        assert!(items.len() <= items_per_queue);
        total_missing += items_per_queue - items.len();

        for (idx, (_queue_idx, value)) in items.into_iter().enumerate() {
            assert_eq!(value, format!("val:{idx:04}").into_bytes());
        }
    }

    assert!(total_missing > 0);
    assert!(total_missing < num_queues * items_per_queue);
}

#[test]
fn test_truncated_data_file_lists() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let num_lists = 50;
    let items_per_list = 10;

    {
        let store = CandyStore::open(dir.path(), config).unwrap();
        for item_idx in 0..items_per_list {
            for list_idx in 0..num_lists {
                store
                    .set_in_list(
                        format!("list:{list_idx}").as_str(),
                        format!("key:{item_idx:04}").as_bytes(),
                        format!("val:{item_idx:04}").as_bytes(),
                    )
                    .unwrap();
            }
        }
        store.flush().unwrap();
    }

    let data_path = first_data_file_path(dir.path());
    let file = OpenOptions::new().write(true).open(&data_path).unwrap();
    let file_len = file.metadata().unwrap().len();
    file.set_len(file_len - 2400).unwrap();

    let store = CandyStore::open(dir.path(), config).unwrap();

    let mut total_missing = 0;
    for list_idx in 0..num_lists {
        let list_key = format!("list:{list_idx}");
        let items: Vec<_> = store
            .iter_list(&list_key)
            .collect::<candystore::Result<Vec<_>>>()
            .unwrap();

        assert!(items.len() <= items_per_list);
        total_missing += items_per_list - items.len();

        for (idx, (key, value)) in items.into_iter().enumerate() {
            assert_eq!(key, format!("key:{idx:04}").into_bytes());
            assert_eq!(value, format!("val:{idx:04}").into_bytes());
        }
    }

    assert!(total_missing > 0);
    assert!(total_missing < num_lists * items_per_list);
}
