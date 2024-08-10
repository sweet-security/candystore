use std::{
    hint::black_box,
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};
use vicky_store::{Config, Result, VickyStore};

fn run2(msg: &str, iters: u32, mut func: impl FnMut() -> Result<()>) -> Result<()> {
    let t0 = Instant::now();
    func()?;
    let t1 = Instant::now();
    println!(
        "{msg}: {:.3}us",
        ((t1.duration_since(t0).as_nanos() as f64) / 1000.0) / (iters as f64)
    );
    Ok(())
}

fn run(msg: &str, iters: u32, mut func: impl FnMut(u32) -> Result<()>) -> Result<()> {
    run2(msg, iters, || {
        for i in 0u32..iters {
            func(i)?;
        }
        Ok(())
    })
}

fn test_small_keys(num_keys: u32) -> Result<()> {
    for pre_split in [true, false] {
        let db = VickyStore::open(
            "./dbdir",
            Config {
                expected_number_of_keys: if pre_split { num_keys as usize } else { 0 },
                ..Default::default()
            },
        )?;

        db.clear()?;

        if pre_split {
            println!("{num_keys} small entries with pre-split");
        } else {
            println!("{num_keys} small entries without pre-split");
        }

        run("  Small entries insert", num_keys, |i| {
            db.set(&(i * 2).to_le_bytes(), "xxx")?;
            Ok(())
        })?;

        run("  Small entries get 100% existing", num_keys, |i| {
            let val = db.get(&(i * 2).to_le_bytes())?;
            black_box(val.unwrap());
            Ok(())
        })?;

        run("  Small entries get 50% existing", num_keys, |i| {
            let val = db.get(&(i * 2).to_le_bytes())?;
            black_box(val.unwrap());
            Ok(())
        })?;

        run("  Small entries removal", num_keys, |i| {
            let val = db.remove(&(i * 2).to_le_bytes())?;
            black_box(val.unwrap());
            Ok(())
        })?;

        db.clear()?;

        run("  Small entries mixed", num_keys, |i| {
            db.set(&(i * 2).to_le_bytes(), "xxx")?;
            let val = db.get(&(i / 2).to_le_bytes())?;
            black_box(val);
            if i % 8 == 7 {
                db.remove(&(i / 2).to_le_bytes())?;
            }
            Ok(())
        })?;

        println!();
    }

    Ok(())
}

fn test_large_keys(num_keys: u32) -> Result<()> {
    for pre_split in [true, false] {
        let db = VickyStore::open(
            "./dbdir",
            Config {
                expected_number_of_keys: if pre_split { num_keys as usize } else { 0 },
                ..Default::default()
            },
        )?;

        db.clear()?;

        if pre_split {
            println!("{num_keys} large entries with pre-split");
        } else {
            println!("{num_keys} large entries without pre-split");
        }

        run("  Large entries insert", num_keys, |i| {
            let mut key = [99u8; 100];
            key[0..4].copy_from_slice(&i.to_le_bytes());
            let val = [7u8; 300];
            db.set(&key, &val)?;
            Ok(())
        })?;

        run("  Large entries get 100% existing", num_keys, |i| {
            let mut key = [99u8; 100];
            key[0..4].copy_from_slice(&i.to_le_bytes());
            let val = db.get(&key)?;
            black_box(val);
            Ok(())
        })?;

        run("  Large entries removal", num_keys, |i| {
            let mut key = [99u8; 100];
            key[0..4].copy_from_slice(&i.to_le_bytes());
            let val = db.remove(&(i * 2).to_le_bytes())?;
            black_box(val);
            Ok(())
        })?;

        println!();
    }

    Ok(())
}

fn test_collections(num_colls: u32, num_items_per_coll: u32) -> Result<()> {
    let db = VickyStore::open(
        "./dbdir",
        Config {
            expected_number_of_keys: (num_colls * num_items_per_coll) as usize,
            ..Default::default()
        },
    )?;

    println!("{num_colls} collections with {num_items_per_coll} items in each");
    run2("  Inserts", num_colls * num_items_per_coll, || {
        for coll in 0..num_colls {
            for item in 0..num_items_per_coll {
                db.set_in_collection(&coll.to_le_bytes(), &item.to_le_bytes(), "xxx")?;
            }
        }
        Ok(())
    })?;

    run2("  Updates", num_colls * num_items_per_coll, || {
        for coll in 0..num_colls {
            for item in 0..num_items_per_coll {
                db.set_in_collection(&coll.to_le_bytes(), &item.to_le_bytes(), "yyy")?;
            }
        }
        Ok(())
    })?;

    run2("  Gets", num_colls * num_items_per_coll, || {
        for coll in 0..num_colls {
            for item in 0..num_items_per_coll {
                let val = db.get_from_collection(&coll.to_le_bytes(), &item.to_le_bytes())?;
                black_box(val);
            }
        }
        Ok(())
    })?;

    run2("  Iterations", num_colls * num_items_per_coll, || {
        for coll in 0..num_colls {
            let count = db.iter_collection(&coll.to_le_bytes()).count();
            black_box(count);
            debug_assert_eq!(count, num_items_per_coll as usize);
        }
        Ok(())
    })?;

    run2(
        "  Removal of 50% items",
        num_colls * num_items_per_coll / 2,
        || {
            for coll in 0..num_colls {
                for item in 0..num_items_per_coll {
                    if item % 2 == 0 {
                        let val =
                            db.remove_from_collection(&coll.to_le_bytes(), &item.to_le_bytes())?;
                        black_box(val.unwrap());
                    }
                }
            }
            Ok(())
        },
    )?;

    run2("  Discards", num_colls * num_items_per_coll / 2, || {
        for coll in 0..num_colls {
            db.discard_collection(&coll.to_le_bytes())?;
        }
        Ok(())
    })?;

    println!();

    Ok(())
}

fn test_concurrency_without_contention(num_threads: u32, num_keys: u32) -> Result<()> {
    for pre_split in [true, false] {
        let db = Arc::new(VickyStore::open(
            "./dbdir",
            Config {
                expected_number_of_keys: if pre_split {
                    (num_threads * num_keys) as usize
                } else {
                    0
                },
                ..Default::default()
            },
        )?);
        db.clear()?;

        if pre_split {
            println!("No-contention: {num_threads} threads accessing {num_keys} different keys - with pre-split");
        } else {
            println!(
                "No-contention: {num_threads} threads accessing {num_keys} different keys - without pre-split"
            );
        }

        let insert_time_ns = Arc::new(AtomicU64::new(0));
        let get_time_ns = Arc::new(AtomicU64::new(0));
        let removal_time_ns = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];
        for thd in 0..num_threads {
            let db = db.clone();
            let insert_time_ns = insert_time_ns.clone();
            let get_time_ns = get_time_ns.clone();
            let removal_time_ns = removal_time_ns.clone();

            let h = std::thread::spawn(move || {
                {
                    let t0 = Instant::now();
                    for i in thd * num_keys..(thd + 1) * num_keys {
                        db.set(&i.to_le_bytes(), &thd.to_le_bytes())?;
                    }
                    insert_time_ns.fetch_add(
                        Instant::now().duration_since(t0).as_nanos() as u64,
                        std::sync::atomic::Ordering::SeqCst,
                    );
                }

                {
                    let t0 = Instant::now();
                    for i in thd * num_keys..(thd + 1) * num_keys {
                        let val = db.get(&i.to_le_bytes())?;
                        debug_assert_eq!(val, Some(thd.to_le_bytes().to_vec()));
                        black_box(val.unwrap());
                    }
                    get_time_ns.fetch_add(
                        Instant::now().duration_since(t0).as_nanos() as u64,
                        std::sync::atomic::Ordering::SeqCst,
                    );
                }

                {
                    let t0 = Instant::now();
                    for i in thd * num_keys..(thd + 1) * num_keys {
                        let val = db.remove(&i.to_le_bytes())?;
                        debug_assert!(val.is_some());
                        black_box(val.unwrap());
                    }
                    removal_time_ns.fetch_add(
                        Instant::now().duration_since(t0).as_nanos() as u64,
                        std::sync::atomic::Ordering::SeqCst,
                    );
                }

                Result::<()>::Ok(())
            });
            handles.push(h);
        }
        for h in handles {
            h.join().unwrap()?;
        }

        let insert_time_ns = insert_time_ns.load(std::sync::atomic::Ordering::SeqCst) as f64;
        let get_time_ns = get_time_ns.load(std::sync::atomic::Ordering::SeqCst) as f64;
        let removal_time_ns = removal_time_ns.load(std::sync::atomic::Ordering::SeqCst) as f64;
        let ops = (num_threads * num_keys) as f64;

        println!("  Inserts: {:.3}us", (insert_time_ns / 1000.0) / ops);
        println!("  Gets: {:.3}us", (get_time_ns / 1000.0) / ops);
        println!("  Removals: {:.3}us", (removal_time_ns / 1000.0) / ops);
        println!();
    }

    Ok(())
}

fn do_inserts(
    thd: u32,
    num_keys: u32,
    insert_time_ns: &Arc<AtomicU64>,
    db: &Arc<VickyStore>,
) -> Result<()> {
    let t0 = Instant::now();
    for i in 0..num_keys {
        db.set(&i.to_le_bytes(), &thd.to_le_bytes())?;
    }
    insert_time_ns.fetch_add(
        Instant::now().duration_since(t0).as_nanos() as u64,
        std::sync::atomic::Ordering::SeqCst,
    );
    Ok(())
}

fn do_gets(num_keys: u32, get_time_ns: &Arc<AtomicU64>, db: &Arc<VickyStore>) -> Result<()> {
    let t0 = Instant::now();
    for i in 0..num_keys {
        let val = db.get(&i.to_le_bytes())?;
        black_box(val);
    }
    get_time_ns.fetch_add(
        Instant::now().duration_since(t0).as_nanos() as u64,
        std::sync::atomic::Ordering::SeqCst,
    );
    Ok(())
}

fn do_removals(
    num_keys: u32,
    removal_time_ns: &Arc<AtomicU64>,
    db: &Arc<VickyStore>,
) -> Result<()> {
    let t0 = Instant::now();
    for i in 0..num_keys {
        let val = db.remove(&i.to_le_bytes())?;
        black_box(val);
    }
    removal_time_ns.fetch_add(
        Instant::now().duration_since(t0).as_nanos() as u64,
        std::sync::atomic::Ordering::SeqCst,
    );
    Ok(())
}

fn test_concurrency_with_contention(num_threads: u32, num_keys: u32) -> Result<()> {
    for pre_split in [true, false] {
        let db = Arc::new(VickyStore::open(
            "./dbdir",
            Config {
                expected_number_of_keys: if pre_split {
                    (num_threads * num_keys) as usize
                } else {
                    0
                },
                ..Default::default()
            },
        )?);
        db.clear()?;

        if pre_split {
            println!(
                "Contention: {num_threads} threads accessing {num_keys} same keys - with pre-split"
            );
        } else {
            println!("Contention: {num_threads} threads accessing {num_keys} same keys - without pre-split");
        }

        let insert_time_ns = Arc::new(AtomicU64::new(0));
        let get_time_ns = Arc::new(AtomicU64::new(0));
        let removal_time_ns = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];
        for thd in 0..num_threads {
            let db = db.clone();
            let insert_time_ns = insert_time_ns.clone();
            let get_time_ns = get_time_ns.clone();
            let removal_time_ns = removal_time_ns.clone();

            let h = std::thread::spawn(move || {
                if thd % 3 == 0 {
                    do_inserts(thd, num_keys, &insert_time_ns, &db)?;
                    do_gets(num_keys, &get_time_ns, &db)?;
                    do_removals(num_keys, &removal_time_ns, &db)?;
                } else if thd % 3 == 1 {
                    do_gets(num_keys, &get_time_ns, &db)?;
                    do_removals(num_keys, &removal_time_ns, &db)?;
                    do_inserts(thd, num_keys, &insert_time_ns, &db)?;
                } else {
                    do_removals(num_keys, &removal_time_ns, &db)?;
                    do_inserts(thd, num_keys, &insert_time_ns, &db)?;
                    do_gets(num_keys, &get_time_ns, &db)?;
                }

                Result::<()>::Ok(())
            });
            handles.push(h);
        }
        for h in handles {
            h.join().unwrap()?;
        }

        let insert_time_ns = insert_time_ns.load(std::sync::atomic::Ordering::SeqCst) as f64;
        let get_time_ns = get_time_ns.load(std::sync::atomic::Ordering::SeqCst) as f64;
        let removal_time_ns = removal_time_ns.load(std::sync::atomic::Ordering::SeqCst) as f64;
        let ops = (num_threads * num_keys) as f64;

        println!("  Inserts: {:.3}us", (insert_time_ns / 1000.0) / ops);
        println!("  Gets: {:.3}us", (get_time_ns / 1000.0) / ops);
        println!("  Removals: {:.3}us", (removal_time_ns / 1000.0) / ops);
        println!();
    }

    Ok(())
}

fn main() -> Result<()> {
    test_small_keys(1_000_000)?;
    test_large_keys(500_000)?;
    test_collections(10, 100_000)?;
    test_concurrency_without_contention(10, 100_000)?;
    test_concurrency_with_contention(10, 1_000_000)?;

    Ok(())
}
