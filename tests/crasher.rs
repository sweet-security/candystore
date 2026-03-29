#![cfg(unix)]

use std::ptr::null_mut;
use std::time::Duration;
use std::{ops::Range, sync::atomic::AtomicU64, sync::atomic::Ordering::SeqCst};

use candystore::{CandyStore, Config, Result};
use rand::RngExt;

#[cfg(debug_assertions)]
const TARGET: u32 = 100_000;
#[cfg(debug_assertions)]
const SLEEP_RANGE: Range<u64> = 300..800;

#[cfg(not(debug_assertions))]
const TARGET: u32 = 1_000_000;
#[cfg(not(debug_assertions))]
const SLEEP_RANGE: Range<u64> = 50..500;

fn get_config() -> Config {
    Config {
        max_data_file_size: 64 * 1024 * 1024,
        compaction_min_threshold: 8 * 1024 * 1024,
        hash_key: (0xb047_a3ef_b334_9804, 0x807d_3135_878e_9b27),
        initial_capacity: 1024,
        max_concurrency: 64,
        ..Default::default()
    }
}

const DB_DIR: &str = "/tmp/dbdir_crash";

fn record_rebuild_stats(shared_stuff: &SharedStuff, store: &CandyStore) {
    let stats = store.stats();
    shared_stuff
        .total_num_rebuilt_entries
        .fetch_add(stats.num_rebuilt_entries, SeqCst);
    shared_stuff
        .total_num_dropped_bytes_on_rebuild
        .fetch_add(stats.num_rebuild_purged_bytes, SeqCst);
}

fn child_inserts(shared_stuff: &SharedStuff) -> Result<()> {
    // our job is to create 1M entries while being killed by our evil parent

    let store = CandyStore::open(DB_DIR, get_config())?;
    record_rebuild_stats(shared_stuff, &store);
    let highest_bytes = store.get("highest")?.unwrap_or(vec![0, 0, 0, 0]);
    let highest = u32::from_le_bytes(highest_bytes.try_into().unwrap());

    if highest == TARGET - 1 {
        println!("child finished (already at {highest})");
        return Ok(());
    }

    println!("child starting at {highest}");

    for i in highest..TARGET {
        store.set(i.to_le_bytes(), "i am a key")?;
        store.set("highest", i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_removals(shared_stuff: &SharedStuff) -> Result<()> {
    // our job is to remove 1M entries while being killed by our evil parent

    let store = CandyStore::open(DB_DIR, get_config())?;
    record_rebuild_stats(shared_stuff, &store);
    let lowest_bytes = store.get("lowest")?.unwrap_or(vec![0, 0, 0, 0]);
    let lowest = u32::from_le_bytes(lowest_bytes.try_into().unwrap());

    if lowest == TARGET - 1 {
        println!("child finished (already at {lowest})");
        return Ok(());
    }

    println!("child starting at {lowest}");

    assert!(!store.contains("highest")?, "\"highest\" got resurrected");

    for i in lowest..TARGET {
        store.remove(i.to_le_bytes())?;
        store.set("lowest", i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_list_inserts(shared_stuff: &SharedStuff) -> Result<()> {
    // our job is to insert 1M entries to a list while being killed by our evil parent

    let store = CandyStore::open(DB_DIR, get_config())?;
    record_rebuild_stats(shared_stuff, &store);

    let highest_bytes = store.get("list_highest")?.unwrap_or(vec![0, 0, 0, 0]);
    let highest = u32::from_le_bytes(highest_bytes.try_into().unwrap());

    if highest == TARGET - 1 {
        println!("child finished (already at {highest})");
        return Ok(());
    }

    println!("child starting at {highest}");

    for i in highest..TARGET {
        store.set_in_list("xxx", &i.to_le_bytes(), "yyy")?;
        store.set("list_highest", i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_list_removals(shared_stuff: &SharedStuff) -> Result<()> {
    // our job is to remove 1M entries to a list while being killed by our evil parent

    let store = CandyStore::open(DB_DIR, get_config())?;
    record_rebuild_stats(shared_stuff, &store);

    let lowest_bytes = store.get("list_lowest")?.unwrap_or(vec![0, 0, 0, 0]);
    let lowest = u32::from_le_bytes(lowest_bytes.try_into().unwrap());

    if lowest == TARGET - 1 {
        println!("child finished (already at {lowest})");
        return Ok(());
    }

    println!("child starting at {lowest}",);
    const Q: u32 = TARGET / 4;
    const Q1: u32 = Q;
    const Q2: u32 = 2 * Q;
    const Q3: u32 = 3 * Q;
    const Q4: u32 = 4 * Q;
    const _: () = assert!(Q4 == TARGET);

    for i in lowest..TARGET {
        let j = match i {
            0..Q1 => i,                      // remove head [0..250K), remaining [250K..1M)
            Q1..Q2 => TARGET - 1 - (i - Q1), // remove tail [750K..1M), reamining [250K..750K)
            Q2..Q3 => i,                     // remove middle [500K..750K), remaining [250K..500K)
            Q3..Q4 => i - Q2,                // remove head [250K..500K), remaining [)
            _ => unreachable!(),
        };

        let old = store.remove_from_list("xxx", &j.to_le_bytes())?;

        assert!(
            old.is_none() || old == Some("yyy".into()),
            "{i} old={old:?}"
        );
        store.set("list_lowest", i.to_le_bytes())?;
    }

    println!("child finished");

    Ok(())
}

fn child_list_iterator_removals(shared_stuff: &SharedStuff) -> Result<()> {
    let store = CandyStore::open(DB_DIR, get_config())?;
    record_rebuild_stats(shared_stuff, &store);

    if rand::random() {
        //println!("FWD");
        for (i, res) in store.iter_list("xxx").enumerate() {
            let (k, v) = res?;
            let v2 = u32::from_le_bytes(v.try_into().unwrap());
            if i == 0 {
                println!("FWD child starts at {v2}");
            }
            store.remove_from_list("xxx", &k)?;
        }
    } else {
        //println!("BACK");
        for (i, res) in store.iter_list("xxx").rev().enumerate() {
            let (k, v) = res?;
            let v2 = u32::from_le_bytes(v.try_into().unwrap());
            if i == 0 {
                println!("BACK child starts at {v2}");
            }
            store.remove_from_list("xxx", &k)?;
        }
    }

    println!("child finished");

    Ok(())
}

fn parent_run(
    shared_stuff: &SharedStuff,
    child_name: &str,
    mut child_func: impl FnMut(&SharedStuff) -> Result<()>,
) -> Result<()> {
    println!("======== Parent starts {child_name} ========");
    for i in 0.. {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            let res = child_func(shared_stuff);
            if let Err(e) = res {
                eprintln!("Child failed: {}", e);
                shared_stuff.failed.store(1, SeqCst);
            }
            unsafe { libc::exit(0) };
        } else {
            // parent
            let dur = Duration::from_millis(rand::rng().random_range(SLEEP_RANGE));
            std::thread::sleep(dur);
            let mut status = 0i32;
            let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if rc == 0 {
                if shared_stuff.failed.load(SeqCst) != 0 {
                    unsafe { libc::waitpid(pid, &mut status, 0) };
                    panic!("child crashed at iteration {i}");
                }

                println!("[{i}] killing child after {dur:?}");
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::wait(&mut status);
                };
                if shared_stuff.failed.load(SeqCst) != 0 {
                    panic!("child crashed at iteration {i}");
                }
            } else {
                assert!(rc > 0);
                if (!libc::WIFSIGNALED(status) && libc::WEXITSTATUS(status) != 0)
                    || shared_stuff.failed.load(SeqCst) != 0
                {
                    panic!("child crashed at iteration {i}");
                }

                println!("child finished in {i} iterations");
                break;
            }
        }
    }
    Ok(())
}

struct SharedStuff {
    failed: AtomicU64,
    total_num_rebuilt_entries: AtomicU64,
    total_num_dropped_bytes_on_rebuild: AtomicU64,
}

#[test]
fn test_crash_recovery() -> Result<()> {
    // Only run on Linux because of fork/mmap
    if cfg!(not(target_os = "linux")) {
        return Ok(());
    }

    _ = std::fs::remove_dir_all(DB_DIR);

    let map_addr = unsafe {
        libc::mmap(
            null_mut(),
            4096,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(map_addr, libc::MAP_FAILED);

    let shared_stuff = unsafe { &*(map_addr as *const SharedStuff) };
    shared_stuff.failed.store(0, SeqCst);
    shared_stuff.total_num_rebuilt_entries.store(0, SeqCst);
    shared_stuff
        .total_num_dropped_bytes_on_rebuild
        .store(0, SeqCst);

    parent_run(shared_stuff, "child_inserts", child_inserts)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open(DB_DIR, get_config())?;
        assert_eq!(
            store.remove("highest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );
        let mut count = 0;
        for res in store.iter_items() {
            let (k, v) = res?;
            assert_eq!(v, b"i am a key");
            let k = u32::from_le_bytes(k.try_into().unwrap());
            assert!(k < TARGET);
            count += 1;
        }
        assert_eq!(count, TARGET);

        println!("DB validated successfully");
    }

    parent_run(shared_stuff, "child_removals", child_removals)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open(DB_DIR, get_config())?;
        assert_eq!(
            store.remove("lowest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );
        let items = store.iter_items().collect::<Vec<_>>();
        assert_eq!(items.len(), 0, "{items:?}");

        println!("DB validated successfully");
    }

    parent_run(shared_stuff, "child_list_inserts", child_list_inserts)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open(DB_DIR, get_config())?;
        assert_eq!(
            store.remove("list_highest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );

        for (i, res) in store.iter_list("xxx").enumerate() {
            let (k, v) = res?;
            assert_eq!(u32::from_le_bytes(k.try_into().unwrap()), i as u32);
            assert_eq!(v, b"yyy");
        }

        println!("DB validated successfully");
    }

    parent_run(shared_stuff, "child_list_removals", child_list_removals)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open(DB_DIR, get_config())?;
        assert_eq!(
            store.remove("list_lowest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );

        assert_eq!(store.iter_list("xxx").count(), 0);

        println!("leaked: {}", store.iter_items().count());
        store.discard_list("xxx")?;

        println!("DB validated successfully");
    }

    {
        println!("Parent creates {} members in a list...", TARGET);

        let store = CandyStore::open(DB_DIR, get_config())?;
        let t0 = std::time::Instant::now();
        for i in 0u32..TARGET {
            if i % 100000 == 0 {
                println!("{i}");
            }
            store.set_in_list("xxx", &i.to_le_bytes(), &i.to_le_bytes())?;
        }
        println!(
            "{}us",
            std::time::Instant::now().duration_since(t0).as_micros()
        );
    }

    parent_run(
        shared_stuff,
        "child_list_iterator_removals",
        child_list_iterator_removals,
    )?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open(DB_DIR, get_config())?;

        assert_eq!(store.iter_list("xxx").count(), 0);

        // we will surely leak some entries that were unlinked from the list before they were removed
        println!("leaked: {}", store.iter_items().count());
        store.discard_list("xxx")?;

        println!("DB validated successfully");
    }

    println!(
        "rebuilt_entries_total={} dropped_bytes_on_rebuild_total={}",
        shared_stuff.total_num_rebuilt_entries.load(SeqCst),
        shared_stuff.total_num_dropped_bytes_on_rebuild.load(SeqCst)
    );

    _ = std::fs::remove_dir_all(DB_DIR);
    Ok(())
}
