use core::str;

use candystore::{Config, Result, CandyStore};

fn main() -> Result<()> {
    let db = CandyStore::open("/tmp/candy-dir", Config::default())?;

    // clear the DB just in case we has something there before. in real-life scenarios you would probably
    // not clear the DB every time
    db.clear()?;

    println!("{:?}", db.get("mykey")?); // None

    db.set("mykey", "myval")?;
    println!("{:?}", db.get("mykey")?); // Some([109, 121, 118, 97, 108])

    println!("{:?}", db.remove("mykey")?); // Some([109, 121, 118, 97, 108])
    println!("{:?}", db.remove("mykey")?); // None

    println!("{:?}", db.get("mykey")?); // None

    for i in 0..10 {
        db.set(&format!("mykey{i}"), &format!("myval{i}"))?;
    }
    for res in db.iter() {
        let (k, v) = res?;
        println!(
            "{} = {}",
            str::from_utf8(&k).unwrap(),
            str::from_utf8(&v).unwrap()
        );
    }

    Ok(())
}
