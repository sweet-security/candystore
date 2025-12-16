use candystore::{CandyStore, Config};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_list_concurrency_basic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let store = Arc::new(CandyStore::open(temp_dir.path(), config).unwrap());
    let list_key = b"concurrent_list";

    let num_threads = 8;
    let items_per_thread = 500;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for t in 0..num_threads {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            // Insert items
            for i in 0..items_per_thread {
                let key = format!("t{}-{}", t, i);
                let val = format!("val-{}-{}", t, i);
                store
                    .set_in_list(list_key, key.as_bytes(), val.as_bytes())
                    .unwrap();
            }

            // Verify items exist
            for i in 0..items_per_thread {
                let key = format!("t{}-{}", t, i);
                let val = format!("val-{}-{}", t, i);
                let got = store.get_from_list(list_key, key.as_bytes()).unwrap();
                assert_eq!(got, Some(val.as_bytes().to_vec()));
            }

            // Remove items
            for i in 0..items_per_thread {
                let key = format!("t{}-{}", t, i);
                store.remove_from_list(list_key, key.as_bytes()).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify list is empty
    assert_eq!(store.list_len(list_key).unwrap(), 0);
    assert_eq!(store.iter_list(list_key).count(), 0);
}

#[test]
fn test_list_concurrency_promoting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let store = Arc::new(CandyStore::open(temp_dir.path(), config).unwrap());
    let list_key = b"concurrent_list_promo";

    let num_threads = 4;
    let items_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    // Pre-populate list to have some contention
    for i in 0..100 {
        store
            .set_in_list(list_key, format!("base-{}", i).as_bytes(), b"base")
            .unwrap();
    }

    let mut handles = vec![];

    for t in 0..num_threads {
        let store = store.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            // Insert with promotion (moves to head)
            for i in 0..items_per_thread {
                let key = format!("t{}-{}", t, i);
                store
                    .set_in_list_promoting(list_key, key.as_bytes(), b"val")
                    .unwrap();
            }

            // Promote existing base items randomly
            for i in 0..50 {
                let key = format!("base-{}", i); // Contention on base items
                store
                    .set_in_list_promoting(list_key, key.as_bytes(), b"base-promoted")
                    .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify total length
    // 100 base items + (num_threads * items_per_thread)
    let expected_len = 100 + (num_threads * items_per_thread);
    assert_eq!(store.list_len(list_key).unwrap(), expected_len);
    assert_eq!(store.iter_list(list_key).count(), expected_len);
}
