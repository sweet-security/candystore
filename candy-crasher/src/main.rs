use std::ptr::null_mut;
use std::time::Duration;
use std::{ops::Range, sync::atomic::AtomicU64, sync::atomic::Ordering::SeqCst};

use candystore::{CandyStore, Config, Result};
use rand::Rng;

const TARGET: u32 = 1_000_000;

fn child_inserts() -> Result<()> {
    // our job is to create 1M entries while being killed by our evil parent

    let store = CandyStore::open("dbdir", Config::default())?;
    let highest_bytes = store.get("highest")?.unwrap_or(vec![0, 0, 0, 0]);
    let highest = u32::from_le_bytes([
        highest_bytes[0],
        highest_bytes[1],
        highest_bytes[2],
        highest_bytes[3],
    ]);

    if highest == TARGET - 1 {
        println!("child finished (already at {highest})");
        return Ok(());
    }

    println!("child starting at {highest}");

    for i in highest..TARGET {
        store.set(&i.to_le_bytes(), "i am a key")?;
        store.set("highest", &i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_removals() -> Result<()> {
    // our job is to remove 1M entries while being killed by our evil parent

    let store = CandyStore::open("dbdir", Config::default())?;
    let lowest_bytes = store.get("lowest")?.unwrap_or(vec![0, 0, 0, 0]);
    let lowest = u32::from_le_bytes([
        lowest_bytes[0],
        lowest_bytes[1],
        lowest_bytes[2],
        lowest_bytes[3],
    ]);

    if lowest == TARGET - 1 {
        println!("child finished (already at {lowest})");
        return Ok(());
    }

    println!("child starting at {lowest}");

    for i in lowest..TARGET {
        store.remove(&i.to_le_bytes())?;
        store.set("lowest", &i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_list_inserts() -> Result<()> {
    // our job is to insert 1M entries to a list while being killed by our evil parent

    let store = CandyStore::open("dbdir", Config::default())?;

    let highest_bytes = store.get("list_highest")?.unwrap_or(vec![0, 0, 0, 0]);
    let highest = u32::from_le_bytes([
        highest_bytes[0],
        highest_bytes[1],
        highest_bytes[2],
        highest_bytes[3],
    ]);

    if highest == TARGET - 1 {
        println!("child finished (already at {highest})");
        return Ok(());
    }

    println!("child starting at {highest}");

    for i in highest..TARGET {
        store.set_in_list("xxx", &i.to_le_bytes(), "yyy")?;
        store.set("list_highest", &i.to_le_bytes())?;
    }
    println!("child finished");

    Ok(())
}

fn child_list_removals() -> Result<()> {
    // our job is to remove 1M entries to a list while being killed by our evil parent

    let store = CandyStore::open("dbdir", Config::default())?;

    let lowest_bytes = store.get("list_lowest")?.unwrap_or(vec![0, 0, 0, 0]);
    let lowest = u32::from_le_bytes([
        lowest_bytes[0],
        lowest_bytes[1],
        lowest_bytes[2],
        lowest_bytes[3],
    ]);

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
        store.set("list_lowest", &i.to_le_bytes())?;
    }

    println!("child finished");

    Ok(())
}

fn child_list_iterator_removals() -> Result<()> {
    let store = CandyStore::open("dbdir", Config::default())?;

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
        for (i, res) in store.iter_list_backwards("xxx").enumerate() {
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
    mut child_func: impl FnMut() -> Result<()>,
    sleep: Range<u64>,
) -> Result<()> {
    for i in 0.. {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            let res = child_func();
            if res.is_err() {
                shared_stuff.failed.store(1, SeqCst);
            }
            res.unwrap();
            unsafe { libc::exit(0) };
        } else {
            // parent
            std::thread::sleep(Duration::from_millis(
                rand::thread_rng().gen_range(sleep.clone()),
            ));
            let mut status = 0i32;
            let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if rc == 0 {
                if shared_stuff.failed.load(SeqCst) != 0 {
                    unsafe { libc::waitpid(pid, &mut status, 0) };
                    panic!("child crashed at iteration {i}");
                }

                println!("[{i}] killing child");
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
}

fn main() -> Result<()> {
    _ = std::fs::remove_dir_all("dbdir");

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

    // let store = CandyStore::open(
    //     "dbdir",
    //     Config {
    //         expected_number_of_keys: 1_000_000,
    //         ..Default::default()
    //     },
    // )?;
    // drop(store);

    parent_run(shared_stuff, child_inserts, 10..300)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("highest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );
        let mut count = 0;
        for res in store.iter() {
            let (k, v) = res?;
            assert_eq!(v, b"i am a key");
            let k = u32::from_le_bytes([k[0], k[1], k[2], k[3]]);
            assert!(k < TARGET);
            count += 1;
        }
        assert_eq!(count, TARGET);

        println!("DB validated successfully");
    }

    parent_run(shared_stuff, child_removals, 10..30)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("lowest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );
        assert_eq!(store.iter().count(), 0);

        println!("DB validated successfully");
    }

    parent_run(shared_stuff, child_list_inserts, 10..300)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("list_highest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );

        for (i, res) in store.iter_list("xxx").enumerate() {
            let (k, v) = res?;
            assert_eq!(k, (i as u32).to_le_bytes());
            assert_eq!(v, b"yyy");
        }

        println!("DB validated successfully");
    }

    parent_run(shared_stuff, child_list_removals, 10..30)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open("dbdir", Config::default())?;
        assert_eq!(
            store.remove("list_lowest")?,
            Some((TARGET - 1).to_le_bytes().to_vec())
        );

        assert_eq!(store.iter_list("xxx").count(), 0);

        println!("leaked: {}", store.iter_raw().count());
        store.discard_list("xxx")?;

        println!("DB validated successfully");
    }

    {
        println!("Parent creates 1M members in a list...");

        let store = CandyStore::open(
            "dbdir",
            Config {
                expected_number_of_keys: 1_000_000,
                ..Default::default()
            },
        )?;
        let t0 = std::time::Instant::now();
        for i in 0u32..1_000_000 {
            if i % 65536 == 0 {
                println!("{i}");
            }
            store.push_to_list_tail("xxx", &i.to_le_bytes())?;
        }
        println!(
            "{}us",
            std::time::Instant::now().duration_since(t0).as_micros()
        );
    }

    parent_run(shared_stuff, child_list_iterator_removals, 10..200)?;

    {
        println!("Parent starts validating the DB...");

        let store = CandyStore::open("dbdir", Config::default())?;

        assert_eq!(store.iter_list("xxx").count(), 0);

        // we will surely leak some entries that were unlinked from the list before they were removed
        println!("leaked: {}", store.iter_raw().count());
        store.discard_list("xxx")?;

        println!("DB validated successfully");
    }

    Ok(())
}
