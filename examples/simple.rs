use core::str;

use candystore::{CandyStore, Config, Result};

fn main() -> Result<()> {
    _ = std::fs::remove_dir_all("/tmp/candy-dir");
    let db = CandyStore::open("/tmp/candy-dir", Config::default())?;

    println!("{:?}", db.get("mykey")?); // None

    db.set("mykey", "myval")?;
    println!("{:?}", db.get("mykey")?); // Some([109, 121, 118, 97, 108])

    println!("{:?}", db.remove("mykey")?); // Some([109, 121, 118, 97, 108])
    println!("{:?}", db.remove("mykey")?); // None

    println!("{:?}", db.get("mykey")?); // None

    for i in 0..10 {
        db.set(format!("mykey{i}"), format!("myval{i}"))?;
    }
    for res in db.iter_items() {
        let (k, v) = res?;
        println!(
            "{} = {}",
            str::from_utf8(&k).unwrap(),
            str::from_utf8(&v).unwrap()
        );
    }

    Ok(())
}
