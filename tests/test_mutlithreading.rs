mod common;

use std::sync::{atomic::AtomicUsize, Arc};

use rand::random;
use vicky_store::{Config, Result, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_multithreaded() -> Result<()> {
    run_in_tempdir(|dir| {
        for attempt in 0..10 {
            let db = Arc::new(VickyStore::open(
                dir,
                Config {
                    max_shard_size: 20 * 1024,
                    min_compaction_threashold: 10 * 1024,
                    ..Default::default()
                },
            )?);

            const NUM_ITEMS: usize = 10_000;
            let succ_gets = Arc::new(AtomicUsize::new(0));
            let succ_removals = Arc::new(AtomicUsize::new(0));

            let mut thds = Vec::new();
            for thid in 0..50 {
                let db = db.clone();
                let succ_gets = succ_gets.clone();
                let succ_removals = succ_removals.clone();
                let handle = std::thread::spawn(move || -> Result<()> {
                    let value = format!("data{thid}");
                    for i in 0..NUM_ITEMS {
                        let key = format!("key{i}");
                        db.set(&key, &value)?;

                        if random::<f32>() > 0.8 {
                            if db.remove(&key)?.is_some() {
                                succ_removals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        } else {
                            let val2 = db.get(&key)?;
                            if let Some(val2) = val2 {
                                assert!(val2.starts_with(b"data"));
                                succ_gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                    }
                    Ok(())
                });
                //handle.join().unwrap().unwrap();
                thds.push(handle);
            }

            for thd in thds {
                thd.join().unwrap()?;
            }

            let gets = succ_gets.load(std::sync::atomic::Ordering::SeqCst);
            let removals = succ_removals.load(std::sync::atomic::Ordering::SeqCst);

            let stats = db.stats();
            println!(
                "[{attempt}] {stats:?} gets={gets} removals={removals} diff={}",
                gets - removals
            );

            assert_eq!(db.iter().count(), stats.num_entries);
            assert!(
                stats.num_entries >= (NUM_ITEMS * 7) / 10
                    && stats.num_entries <= (NUM_ITEMS * 9) / 10
            );
            db.clear()?;
        }
        Ok(())
    })
}
