use candystore::{CandyStore, Config};
use std::{
    hint::black_box,
    sync::{Arc, atomic::AtomicU64},
    thread,
    time::Instant,
};

fn run_perf(
    store: Arc<CandyStore>,
    n: u32,
    n_threads: usize,
    key_size: usize,
    val_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut handles = Vec::new();

    let inserts_us = Arc::new(AtomicU64::new(0));
    let updates_us = Arc::new(AtomicU64::new(0));
    let pos_gets_us = Arc::new(AtomicU64::new(0));
    let neg_gets_us = Arc::new(AtomicU64::new(0));
    let iter_us = Arc::new(AtomicU64::new(0));
    let removes_us = Arc::new(AtomicU64::new(0));

    for t in 0..n_threads {
        let store = store.clone();
        let inserts_us = inserts_us.clone();
        let updates_us = updates_us.clone();
        let pos_gets_us = pos_gets_us.clone();
        let neg_gets_us = neg_gets_us.clone();
        let iter_us = iter_us.clone();
        let removes_us = removes_us.clone();

        let handle = thread::spawn(move || {
            let mut key = vec![b'k'; key_size.max(4)];
            let value1 = vec![b'v'; val_size];
            let value2 = vec![b'V'; val_size];
            let start_idx = t as u32 * n;
            let end_idx = start_idx + n;

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.set(&key, &value1).unwrap();
                }
                let duration = t0.elapsed();
                inserts_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.set(&key, &value2).unwrap();
                }
                let duration = t0.elapsed();
                updates_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.get(&key).unwrap().unwrap();
                }
                let duration = t0.elapsed();
                pos_gets_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let mut key = vec![b'Q'; key_size.max(4)];
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    assert!(store.get(&key).unwrap().is_none());
                }
                let duration = t0.elapsed();
                neg_gets_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                black_box(store.iter_items().count());
                let duration = t0.elapsed();
                iter_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.remove(&key).unwrap();
                }
                let duration = t0.elapsed();
                removes_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        });
        handles.push(handle);
    }

    println!(
        "Testing key-value using {} threads, each with {} items (key size: {}, value size: {})",
        n_threads, n, key_size, val_size
    );

    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "    Inserts: {} us/op",
        inserts_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );
    println!(
        "    Updates: {} us/op",
        updates_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );
    println!(
        "    Positive Lookups: {} us/op",
        pos_gets_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );
    println!(
        "    Negative Lookups: {} us/op",
        neg_gets_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );
    println!(
        "    Iter all: {} us/op",
        iter_us.load(std::sync::atomic::Ordering::Relaxed) as f64 / (n_threads * n as usize) as f64
    );
    println!(
        "    Removes: {} us/op\n",
        removes_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );

    Ok(())
}

fn run_queue_perf(
    store: Arc<CandyStore>,
    n: u32,
    n_threads: usize,
    val_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut handles = Vec::new();

    let pushes_us = Arc::new(AtomicU64::new(0));
    let pops_us = Arc::new(AtomicU64::new(0));

    for _ in 0..n_threads {
        let store = store.clone();
        let pushes_us = pushes_us.clone();
        let pops_us = pops_us.clone();

        let handle = thread::spawn(move || {
            let value = vec![b'v'; val_size];
            {
                let t0 = Instant::now();
                for _ in 0..n {
                    store.push_to_queue_tail("myqueue", &value).unwrap();
                }
                let duration = t0.elapsed();
                pushes_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                for _ in 0..n {
                    store.pop_queue_head("myqueue").unwrap().unwrap();
                }
                let duration = t0.elapsed();
                pops_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        });
        handles.push(handle);
    }

    println!(
        "Testing a queue using {} threads, each with {} items (value size: {})",
        n_threads, n, val_size
    );
    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "    Pushes: {} us/op",
        pushes_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );
    println!(
        "    Pops: {} us/op\n",
        pops_us.load(std::sync::atomic::Ordering::Relaxed) as f64 / (n_threads * n as usize) as f64
    );

    Ok(())
}

fn run_list_perf(
    store: Arc<CandyStore>,
    n: u32,
    n_threads: usize,
    key_size: usize,
    val_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut handles = Vec::new();

    let sets_us = Arc::new(AtomicU64::new(0));
    let gets_us = Arc::new(AtomicU64::new(0));
    let removes_us = Arc::new(AtomicU64::new(0));

    for t in 0..n_threads {
        let store = store.clone();
        let sets_us = sets_us.clone();
        let gets_us = gets_us.clone();
        let removes_us = removes_us.clone();

        let handle = thread::spawn(move || {
            let mut key = vec![b'k'; key_size.max(4)];
            let value = vec![b'v'; val_size];
            let start_idx = t as u32 * n;
            let end_idx = start_idx + n;

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.set_in_list("mylist", &key, &value).unwrap();
                }
                let duration = t0.elapsed();
                sets_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.get_from_list("mylist", &key).unwrap();
                }
                let duration = t0.elapsed();
                gets_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            {
                let t0 = Instant::now();
                for i in start_idx..end_idx {
                    key[..4].copy_from_slice(&i.to_le_bytes());
                    store.remove_from_list("mylist", &key).unwrap();
                }
                let duration = t0.elapsed();
                removes_us.fetch_add(
                    duration.as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        });
        handles.push(handle);
    }

    println!(
        "Testing a list using {} threads, each with {} items (value size: {})",
        n_threads, n, val_size
    );
    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "    Sets: {} us/op",
        sets_us.load(std::sync::atomic::Ordering::Relaxed) as f64 / (n_threads * n as usize) as f64
    );
    println!(
        "    Gets: {} us/op",
        gets_us.load(std::sync::atomic::Ordering::Relaxed) as f64 / (n_threads * n as usize) as f64
    );
    println!(
        "    Removes: {} us/op\n",
        removes_us.load(std::sync::atomic::Ordering::Relaxed) as f64
            / (n_threads * n as usize) as f64
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let store = Arc::new(CandyStore::open(dir.path(), Config::default())?);

    // single threaded
    run_perf(store.clone(), 1_000_000, 1, 16, 16)?;
    run_perf(store.clone(), 100_000, 1, 1024, 4096)?;

    // multi threaded
    run_perf(store.clone(), 250_000, 4, 16, 16)?;
    //run_perf(store.clone(), 10_000, 20, 16, 16)?;

    // queues
    run_queue_perf(store.clone(), 500_000, 1, 16)?;
    run_queue_perf(store.clone(), 100_000, 4, 16)?;

    // lists
    run_list_perf(store.clone(), 500_000, 1, 16, 16)?;
    run_list_perf(store.clone(), 100_000, 4, 16, 16)?;

    Ok(())
}
