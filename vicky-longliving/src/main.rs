use std::sync::{atomic::AtomicU64, Arc};

use vicky_store::{Config, Result, VickyStore, VickyTypedList};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    assert!(args.len() == 2, "expected a single count argument");
    let size = args
        .get(1)
        .expect("count argument required")
        .parse()
        .expect("count must be a usize");

    let db = Arc::new(VickyStore::open("dbdir", Config::default())?);
    db.clear()?;

    let mut handles = vec![];

    let ops = Arc::new(AtomicU64::new(0));

    for thd in 0..10 {
        let db = db.clone();
        let ops = ops.clone();
        let h = std::thread::spawn(move || {
            println!("started thread {thd}");
            let typed = VickyTypedList::<String, usize, String>::new(db.clone());
            for i in 0..size {
                if i % 10000 == 0 {
                    println!("thread {thd} at {i} {:?}", db.stats());
                }
                typed.set("mylist".into(), thd * size + i, "xxx".into())?;
                ops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i > 1000 {
                    typed.remove("mylist".into(), &(thd * size + i - 1000))?;
                    ops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }

            Result::<()>::Ok(())
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap()?;
    }
    println!("ops={}", ops.load(std::sync::atomic::Ordering::Relaxed));

    Ok(())
}
