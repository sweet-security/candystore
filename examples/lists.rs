use vicky_store::{Config, Result, VickyStore};

fn main() -> Result<()> {
    let db = VickyStore::open("/tmp/vicky-dir", Config::default())?;

    // clear the DB just in case we has something there before. in real-life scenarios you would probably
    // not clear the DB every time
    db.clear()?;

    db.set_in_list("asia", "iraq", "arabic")?;
    db.set_in_list("asia", "china", "chinese")?;
    db.set_in_list("asia", "russia", "russian")?;

    db.set_in_list("europe", "spain", "spanish")?;
    db.set_in_list("europe", "italy", "italian")?;
    db.set_in_list("europe", "greece", "greek")?;

    for res in db.iter_list("asia") {
        let (k, v) = res?.unwrap();
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
