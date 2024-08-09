use vicky_store::{Config, Result, VickyStore};

fn main() -> Result<()> {
    let db = VickyStore::open("/tmp/vicky-dir", Config::default())?;

    // clear the DB just in case we has something there before. in real-life scenarios you would probably
    // not clear the DB every time
    db.clear()?;

    db.set_in_collection("asia", "iraq", "arabic")?;
    db.set_in_collection("asia", "china", "chinese")?;
    db.set_in_collection("asia", "russia", "russian")?;

    db.set_in_collection("europe", "spain", "spanish")?;
    db.set_in_collection("europe", "italy", "italian")?;
    db.set_in_collection("europe", "greece", "greek")?;

    for res in db.iter_collection("asia") {
        let (k, v) = res?;
        println!(
            "{} => {}",
            String::from_utf8(k).unwrap(),
            String::from_utf8(v).unwrap()
        )
    }

    // iraq => arabic
    // china => chinese
    // russia => russian

    Ok(())
}