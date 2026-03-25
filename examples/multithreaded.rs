use core::str;
use std::{sync::Arc, time::Duration};

use candystore::{CandyStore, Config, Result};

fn main() -> Result<()> {
    _ = std::fs::remove_dir_all("/tmp/candy-dir");
    let db = Arc::new(CandyStore::open("/tmp/candy-dir-mt", Config::default())?);

    // clone db and spawn thread 1
    let db1 = db.clone();
    let h1 = std::thread::spawn(move || -> Result<()> {
        for i in 0..100 {
            db1.set(format!("key{i}"), "thread 1")?;
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    });

    // clone db and spawn thread 2
    let db2 = db.clone();
    let h2 = std::thread::spawn(move || -> Result<()> {
        for i in 0..100 {
            db2.set(format!("key{i}"), "thread 2")?;
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    });

    h1.join().unwrap()?;
    h2.join().unwrap()?;

    for res in db.iter_items() {
        let (k, v) = res?;
        println!(
            "{} = {}",
            str::from_utf8(&k).unwrap(),
            str::from_utf8(&v).unwrap()
        );
    }

    // key35 = thread 1
    // key41 = thread 1
    // key52 = thread 2
    // key59 = thread 2
    // key48 = thread 2
    // key85 = thread 2
    // key91 = thread 2
    // key26 = thread 1
    // key31 = thread 1
    // ...

    Ok(())
}
