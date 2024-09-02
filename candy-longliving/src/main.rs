use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

use candystore::{CandyStore, CandyTypedList, Config, Result};

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

    let db = Arc::new(CandyStore::open("dbdir", Config::default())?);
    db.clear()?;

    let mut handles = vec![];

    let ops = Arc::new(AtomicU64::new(0));

    for thd in 0..num_threads {
        let db = db.clone();
        let ops = ops.clone();
        let h = std::thread::spawn(move || {
            println!("started thread {thd}");
            let typed = CandyTypedList::<String, usize, String>::new(db.clone());
            let listname = format!("mylist"); //format!("mylist{thd}");
            let mut t0 = Instant::now();
            for i in 0..num_iters {
                if i % 10000 == 0 {
                    let t1 = Instant::now();
                    println!(
                        "thread {thd} at {i} {} rate={}us",
                        db.stats(),
                        t1.duration_since(t0).as_micros() / 10_000,
                    );
                    t0 = t1;
                }

                typed.set(&listname, &(thd * num_iters + i), "xxx")?;
                ops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= tail_length {
                    typed.remove(&listname, &(thd * num_iters + i - tail_length))?;
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
