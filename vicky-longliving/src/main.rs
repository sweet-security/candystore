use std::sync::Arc;

use vicky_store::{Config, Result, VickyStore, VickyTypedList};

fn main() -> Result<()> {
    let db = Arc::new(VickyStore::open("dbdir", Config::default())?);
    db.clear()?;

    let mut handles = vec![];

    const SIZE: usize = 50_000;

    for thd in 0..10 {
        let db = db.clone();
        let h = std::thread::spawn(move || {
            println!("started thread {thd}");
            let typed = VickyTypedList::<String, usize, String>::new(db.clone());
            for i in 0..SIZE {
                if i % 10000 == 0 {
                    println!("thread {thd} at {i} {:?}", db.stats());
                }
                typed.set("mylist".into(), thd * SIZE + i, "xxx".into())?;
                if i > 1000 {
                    typed.remove("mylist".into(), &(thd * SIZE + i - 1000))?;
                }
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
