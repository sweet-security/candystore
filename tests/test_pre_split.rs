mod common;

use candystore::{CandyError, CandyStore, Config, Result};

use crate::common::run_in_tempdir;

#[test]
fn test_pre_split() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(
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

        assert_eq!(files.len(), 64);

        let stats = db.stats();
        assert_eq!(stats.num_shards, 64);
        assert_eq!(stats.num_inserted, 1);
        assert_eq!(stats.wasted_bytes, 0);

        db.set("bbb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;

        let stats = db.stats();
        assert_eq!(stats.num_inserted, 2);
        assert_eq!(stats.wasted_bytes, 0);

        db.set("aaa", "xxx")?;

        let stats = db.stats();
        assert_eq!(stats.num_inserted, 2);

        // test accounting, it's a bit of an implementation detail, but we have to account for the
        // namespace byte as well
        assert_eq!(
            stats.wasted_bytes,
            "????aaa?".len() + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".len()
        );

        db.remove("aaa")?;
        let stats = db.stats();
        assert_eq!(stats.num_inserted, 2);
        assert_eq!(stats.num_removed, 1);
        assert_eq!(
            stats.wasted_bytes,
            "????aaa?".len()
                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".len()
                + "????aaa?".len()
                + "xxx".len()
        );

        Ok(())
    })
}

#[test]
fn test_compaction() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(
            dir,
            Config {
                max_shard_size: 1000,
                min_compaction_threashold: 0,
                ..Default::default()
            },
        )?;

        // fill the shard to the rim, creating waste
        for i in 0..10 {
            db.set("aaa", &format!("1111222233334444555566667777888899990000111122223333444455556666777788889999000011112222333{:x}", i))?;

            let stats = db.stats();
            assert_eq!(stats.num_inserted, 1, "i={i}");
            assert_eq!(stats.used_bytes, 100 * (i + 1), "i={i}");
            assert_eq!(stats.wasted_bytes, 100 * i, "i={i}");
        }

        assert_eq!(db._num_compactions(), 0);

        // insert a new entry, which will cause a compaction
        db.set("bbb", "x")?;
        assert_eq!(db._num_compactions(), 1);

        let stats = db.stats();
        assert_eq!(stats.used_bytes, 100 + "????bbb?".len() + "x".len());
        assert_eq!(stats.wasted_bytes, 0);

        Ok(())
    })
}

#[test]
fn test_too_large() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(
            dir,
            Config {
                max_shard_size: 1000,
                min_compaction_threashold: 0,
                ..Default::default()
            },
        )?;

        assert!(matches!(
            db.set("yyy", &vec![7u8; 1000])
                .unwrap_err()
                .downcast::<CandyError>()
                .unwrap(),
            CandyError::EntryCannotFitInShard(_, _)
        ));

        db.set("yyy", &vec![7u8; 700])?;
        assert_eq!(db._num_splits(), 0);
        assert_eq!(db._num_compactions(), 0);

        db.set("zzz", &vec![7u8; 700])?;
        assert_eq!(db._num_compactions(), 0);
        assert_eq!(db._num_splits(), 1);

        Ok(())
    })
}
