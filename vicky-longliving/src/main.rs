use std::sync::{atomic::AtomicU64, Arc};

use vicky_store::{Config, Result, VickyStore, VickyTypedList};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    assert!(
        args.len() == 4,
        "usage: {} <num_threads> <num_iters> <tail_length>",
        args[0]
    );
    let num_threads: usize = args[1].parse().expect("num_threads not a number");
    let num_iters: usize = args[2].parse().expect("num_iters not a number");
    let tail_length: usize = args[3].parse().expect("tail_length not a number");

    let db = Arc::new(VickyStore::open("dbdir", Config::default())?);
    db.clear()?;

    let mut handles = vec![];

    let ops = Arc::new(AtomicU64::new(0));

    for thd in 0..num_threads {
        let db = db.clone();
        let ops = ops.clone();
        let h = std::thread::spawn(move || {
            println!("started thread {thd}");
            let typed = VickyTypedList::<String, usize, String>::new(db.clone());
            for i in 0..num_iters {
                if i % 10000 == 0 {
                    println!("thread {thd} at {i} {:?}", db.stats());
                }
                typed.set("mylist".into(), &(thd * num_iters + i), "xxx")?;
                ops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= tail_length {
                    typed.remove("mylist".into(), &(thd * num_iters + i - tail_length))?;
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
