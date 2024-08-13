use candystore::Result;
use rand::random;

pub fn run_in_tempdir(f: impl FnOnce(&str) -> Result<()>) -> Result<()> {
    let rand: u64 = random();
    let dir = format!("/tmp/candy-{rand}");
    _ = std::fs::remove_dir_all(&dir);

    f(&dir)?;

    _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[allow(dead_code)]
pub const LONG_VAL: &str = "a very long valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
