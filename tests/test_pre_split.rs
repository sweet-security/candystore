mod common;

use vicky_store::{Config, Result, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_pre_split() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(
            dir,
            Config {
                max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
                min_compaction_threashold: 10 * 1024,
                expected_number_of_keys: 1_000_000,
                ..Default::default()
            },
        )?;

        db.set("aaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;

        let files = std::fs::read_dir(&dir)?
            .map(|res| res.unwrap().file_name().to_string_lossy().to_string())
            .filter(|filename| filename.starts_with("shard_"))
            .collect::<Vec<_>>();

        assert_eq!(files.len(), 32);

        Ok(())
    })
}
