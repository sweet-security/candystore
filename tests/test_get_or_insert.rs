mod common;

use vicky_store::{Config, Result, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_get_or_insert_default() -> Result<()> {
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

        db.insert("aaa", "1111")?;
        assert_eq!(
            db.get_or_insert_default("aaa", "2222")?,
            Some("1111".into())
        );

        assert_eq!(db.get_or_insert_default("bbbb", "2222")?, None);
        assert_eq!(
            db.get_or_insert_default("bbbb", "3333")?,
            Some("2222".into())
        );

        Ok(())
    })
}
