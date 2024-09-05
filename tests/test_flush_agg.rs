#![cfg(feature = "flush_aggregation")]

mod common;

use std::{
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use candystore::{CandyStore, Config, Result};

use crate::common::run_in_tempdir;

#[test]
fn test_lists() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(CandyStore::open(
            dir,
            Config {
                flush_aggregation_delay: Some(Duration::from_millis(1)),
                ..Default::default()
            },
        )?);

        let num_threads = 10;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        for i in 0..num_threads {
            let db = db.clone();
            let barrier = barrier.clone();
            let h = std::thread::spawn(move || {
                barrier.wait();
                let t0 = Instant::now();
                for j in 0..10 {
                    db.set(&format!("key{i}-{j}"), "val")?;
                }
                let dur = Instant::now().duration_since(t0);
                Result::<Duration>::Ok(dur)
            });
            handles.push(h);
        }

        for (i, h) in handles.into_iter().enumerate() {
            let dur = h.join().unwrap()?;
            println!("{i}: {dur:?}");
        }

        Ok(())
    })
}
