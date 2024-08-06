use core::str;
use std::{sync::Arc, time::Duration};

use vicky_store::{Config, GetOrCreateStatus, Result, VickyStore};

// prints:
//   thread 2 owns the lock
//   thread 1 failed to own lock (owned by thread 2)
//   thread 0 failed to own lock (owned by thread 2)
//   thread 1 owns the lock
//   thread 0 failed to own lock (owned by thread 1)
//   thread 2 failed to own lock (owned by thread 1)
//   thread 0 owns the lock
//   ...

fn main() -> Result<()> {
    let db = Arc::new(VickyStore::open("/tmp/vicky-dir", Config::default())?);

    // clear the DB just in case we has something there before. in real-life scenarios you would probably
    // not clear the DB every time
    db.clear()?;

    let mut handles = vec![];
    for thd in 0..3 {
        let db = db.clone();
        let h = std::thread::spawn(move || {
            let name = format!("thread {thd}");
            for _ in 0..5 {
                match db.get_or_create("mylock", &name)? {
                    GetOrCreateStatus::CreatedNew(_) => {
                        println!("{name} owns the lock");
                        std::thread::sleep(Duration::from_millis(1));
                        db.remove("mylock")?;
                    }
                    GetOrCreateStatus::ExistingValue(owner) => {
                        println!(
                            "{name} failed to own lock (owned by {})",
                            str::from_utf8(&owner).unwrap()
                        );
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Result::<()>::Ok(())
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap()?;
    }

    Ok(())
}
