mod common;

use std::sync::Arc;

use vicky_store::{Config, Result, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_modify_inplace() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(
            dir,
            Config {
                max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
                min_compaction_threashold: 10 * 1024,
                ..Default::default()
            },
        )?);

        db.insert("aaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;

        assert_eq!(
            db.modify_inplace("zzz", "bbb", 7).unwrap_err().to_string(),
            "key not found"
        );

        assert_eq!(
            db.modify_inplace("aaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7)
                .unwrap_err()
                .to_string(),
            "value too long",
        );

        db.modify_inplace("aaa", "bbb", 7)?;
        assert_eq!(
            db.get("aaa")?,
            Some("aaaaaaabbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
        );

        Ok(())
    })
}