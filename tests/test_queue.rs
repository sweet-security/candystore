use std::sync::Arc;

use candystore::{CandyStore, Config};
use tempfile::TempDir;

#[test]
fn test_queue_fifo() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();

    let queue = b"my_queue";

    store.push_to_queue_tail(queue, b"item1").unwrap();
    store.push_to_queue_tail(queue, b"item2").unwrap();
    store.push_to_queue_tail(queue, b"item3").unwrap();

    assert_eq!(store.queue_len(queue).unwrap(), 3);

    assert_eq!(
        store.pop_queue_head(queue).unwrap(),
        Some(b"item1".to_vec())
    );
    assert_eq!(
        store.pop_queue_head(queue).unwrap(),
        Some(b"item2".to_vec())
    );
    assert_eq!(
        store.pop_queue_head(queue).unwrap(),
        Some(b"item3".to_vec())
    );
    assert_eq!(store.pop_queue_head(queue).unwrap(), None);

    assert_eq!(store.queue_len(queue).unwrap(), 0);
}

#[test]
fn test_queue_lifo() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();

    let queue = b"stack";

    store.push_to_queue_tail(queue, b"item1").unwrap();
    store.push_to_queue_tail(queue, b"item2").unwrap();
    store.push_to_queue_tail(queue, b"item3").unwrap();

    assert_eq!(
        store.pop_queue_tail(queue).unwrap(),
        Some(b"item3".to_vec())
    );
    assert_eq!(
        store.pop_queue_tail(queue).unwrap(),
        Some(b"item2".to_vec())
    );
    assert_eq!(
        store.pop_queue_tail(queue).unwrap(),
        Some(b"item1".to_vec())
    );
    assert_eq!(store.pop_queue_tail(queue).unwrap(), None);
}

#[test]
fn test_queue_deque() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();

    let queue = b"deque";

    // Push head: 2, 1
    store.push_to_queue_head(queue, b"1").unwrap();
    store.push_to_queue_head(queue, b"2").unwrap();

    // Push tail: 3, 4
    store.push_to_queue_tail(queue, b"3").unwrap();
    store.push_to_queue_tail(queue, b"4").unwrap();

    // Queue should be: 2, 1, 3, 4
    assert_eq!(store.queue_len(queue).unwrap(), 4);

    assert_eq!(store.peek_queue_head(queue).unwrap(), Some(b"2".to_vec()));
    assert_eq!(store.peek_queue_tail(queue).unwrap(), Some(b"4".to_vec()));

    assert_eq!(store.pop_queue_head(queue).unwrap(), Some(b"2".to_vec()));
    assert_eq!(store.pop_queue_tail(queue).unwrap(), Some(b"4".to_vec()));
    assert_eq!(store.pop_queue_head(queue).unwrap(), Some(b"1".to_vec()));
    assert_eq!(store.pop_queue_tail(queue).unwrap(), Some(b"3".to_vec()));
    assert_eq!(store.pop_queue_head(queue).unwrap(), None);
}

#[test]
fn test_queue_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    {
        let store = CandyStore::open(&path, Config::default()).unwrap();
        store.push_to_queue_tail(b"q1", b"val1").unwrap();
        store.push_to_queue_tail(b"q1", b"val2").unwrap();
    }

    {
        let store = CandyStore::open(&path, Config::default()).unwrap();
        assert_eq!(store.queue_len(b"q1").unwrap(), 2);
        assert_eq!(store.pop_queue_head(b"q1").unwrap(), Some(b"val1".to_vec()));
        // Crash simulation: we popped val1, so head moved.
    }

    {
        let store = CandyStore::open(&path, Config::default()).unwrap();
        assert_eq!(store.queue_len(b"q1").unwrap(), 1);
        assert_eq!(store.pop_queue_head(b"q1").unwrap(), Some(b"val2".to_vec()));
        assert_eq!(store.pop_queue_head(b"q1").unwrap(), None);
    }
}

#[test]
fn test_queue_reverse_iteration_skips_holes() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();
    let queue = b"q_rev_iter";

    store.push_to_queue_tail(queue, b"v1").unwrap();
    store.push_to_queue_tail(queue, b"v2").unwrap();
    store.push_to_queue_tail(queue, b"v3").unwrap();
    store.push_to_queue_tail(queue, b"v4").unwrap();

    // Pop two to skip v1 and v2, leaving v3/v4 in the queue with lower indices empty
    assert_eq!(store.pop_queue_head(queue).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(store.pop_queue_head(queue).unwrap(), Some(b"v2".to_vec()));

    let rev_items: Vec<_> = store
        .iter_queue(queue)
        .rev()
        .map(|r| r.unwrap().1)
        .collect();
    assert_eq!(rev_items, vec![b"v4".to_vec(), b"v3".to_vec()]);

    let fwd_items: Vec<_> = store.iter_queue(queue).map(|r| r.unwrap().1).collect();
    assert_eq!(fwd_items, vec![b"v3".to_vec(), b"v4".to_vec()]);
}
#[test]
fn test_multiple_queues() {
    let temp_dir = TempDir::new().unwrap();
    let store = CandyStore::open(temp_dir.path(), Config::default()).unwrap();

    store.push_to_queue_tail(b"q1", b"v1").unwrap();
    store.push_to_queue_tail(b"q2", b"v2").unwrap();

    assert_eq!(store.pop_queue_head(b"q1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(store.pop_queue_head(b"q2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn test_queue_concurrency() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(CandyStore::open(temp_dir.path(), Config::default()).unwrap());
    let queue = b"concurrent_queue";

    let num_producers = 4;
    let items_per_producer = 2000;
    let num_consumers = 4;

    let mut handles = vec![];
    let finished_producing = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Consumers
    let consumed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..num_consumers {
        let store = store.clone();
        let consumed_count = consumed_count.clone();
        let finished_producing = finished_producing.clone();
        handles.push(std::thread::spawn(move || {
            loop {
                match store.pop_queue_head(queue).unwrap() {
                    Some(_) => {
                        consumed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    None => {
                        if finished_producing.load(std::sync::atomic::Ordering::Relaxed) {
                            // Try one last time to ensure we didn't miss a race where
                            // pop returned None -> producer pushed -> producer set flag -> we read flag
                            match store.pop_queue_head(queue).unwrap() {
                                Some(_) => {
                                    consumed_count
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                None => break,
                            }
                        } else {
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }));
    }

    // Producers
    let mut producer_handles = vec![];
    for i in 0..num_producers {
        let store = store.clone();
        producer_handles.push(std::thread::spawn(move || {
            for j in 0..items_per_producer {
                let val = format!("p{}-{}", i, j);
                store.push_to_queue_tail(queue, val.as_bytes()).unwrap();
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    finished_producing.store(true, std::sync::atomic::Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        consumed_count.load(std::sync::atomic::Ordering::Relaxed),
        num_producers * items_per_producer
    );
    assert_eq!(store.queue_len(queue).unwrap(), 0);
}
