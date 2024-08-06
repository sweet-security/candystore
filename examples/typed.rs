use std::sync::Arc;

use vicky_store::{Config, Result, VickyStore, VickyTypedStore};

fn main() -> Result<()> {
    let db = Arc::new(VickyStore::open("/tmp/vicky-dir", Config::default())?);

    let typed = VickyTypedStore::<String, Vec<u32>>::new(db);
    typed.set("hello".into(), vec![1, 2, 3])?;

    println!("{:?}", typed.get("hello")?); // Some([1, 2, 3])

    typed.remove("hello")?;

    println!("{:?}", typed.contains("hello")?); // false

    Ok(())
}
