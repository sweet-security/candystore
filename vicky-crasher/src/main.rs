use std::time::Duration;

use rand::Rng;
use vicky_store::{Config, Result, VickyStore};

const TARGET: u32 = 1_000_000;

fn child_func() -> Result<()> {
    // our job is to create 1M entries while being killed by our evil parent

    let store = VickyStore::open("dbdir", Config::default())?;
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

fn main() -> Result<()> {
    _ = std::fs::remove_dir_all("dbdir");

    for i in 0.. {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            child_func()?;
            return Ok(());
        } else {
            // parent
            std::thread::sleep(Duration::from_millis(rand::thread_rng().gen_range(10..300)));
            let mut status = 0i32;
            let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if rc == 0 {
                println!("[{i}] killing child");
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::wait(&mut status);
                };
            } else {
                assert!(rc > 0);
                println!("child finished in {i} iterations");
                break;
            }
        }
    }

    println!("Parent starts validating the DB...");

    let store = VickyStore::open("dbdir", Config::default())?;
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

    Ok(())
}
