use std::sync::Arc;

use candystore::{CandyStore, CandyTypedStore, Config, Result};

fn main() -> Result<()> {
    let db = Arc::new(CandyStore::open("/tmp/candy-dir", Config::default())?);

    let typed = CandyTypedStore::<String, Vec<u32>>::new(db);
    typed.set("hello", &vec![1, 2, 3])?;

    println!("{:?}", typed.get("hello")?); // Some([1, 2, 3])

    typed.remove("hello")?;

    println!("{:?}", typed.contains("hello")?); // false

    Ok(())
}
