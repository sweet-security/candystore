use core::str;

use vicky_store::{Config, Result, VickyStore};

fn main() -> Result<()> {
    let db = VickyStore::open("/tmp/vicky-dir", Config::default())?;

    // clear the DB just in case we has something there before. in real-life scenarios you would probably
    // not clear the DB every time
    db.clear()?;

    println!("{:?}", db.get("mykey")?); // None

    db.insert("mykey", "myval")?;
    println!("{:?}", db.get("mykey")?); // Some([109, 121, 118, 97, 108])

    println!("{:?}", db.remove("mykey")?); // Some([109, 121, 118, 97, 108])
    println!("{:?}", db.remove("mykey")?); // None

    println!("{:?}", db.get("mykey")?); // None

    for i in 0..10 {
        db.insert(&format!("mykey{i}"), &format!("myval{i}"))?;
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
