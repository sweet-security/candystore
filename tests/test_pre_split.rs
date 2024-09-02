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
        assert_eq!(stats.num_inserts, 1);
        assert_eq!(stats.wasted_bytes, 0);

        db.set("bbb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;

        let stats = db.stats();
        assert_eq!(stats.num_inserts, 2);
        assert_eq!(stats.wasted_bytes, 0);

        db.set("aaa", "xxx")?;

        let stats = db.stats();
        assert_eq!(stats.num_inserts, 2);

        // test accounting, it's a bit of an implementation detail, but we have to account for the
        // namespace byte as well
        assert_eq!(
            stats.wasted_bytes,
            "aaa?".len() + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".len()
        );

        db.remove("aaa")?;
        let stats = db.stats();
        assert_eq!(stats.num_inserts, 2);
        assert_eq!(stats.num_removals, 1);
        assert_eq!(
            stats.wasted_bytes,
            "aaa?".len()
                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".len()
                + "aaa?".len()
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
                min_compaction_threashold: 900,
                ..Default::default()
            },
        )?;

        // fill the shard to the brim, creating waste
        for i in 0..10 {
            db.set("aaa", &format!("11112222333344445555666677778888999900001111222233334444555566667777888899990000111122223333444{:x}", i))?;

            let stats = db.stats();
            assert_eq!(stats.num_inserts, 1, "i={i}");
            assert_eq!(stats.occupied_bytes, 100 * (i + 1), "i={i}");
            assert_eq!(stats.wasted_bytes, 100 * i, "i={i}");
        }

        assert_eq!(db.stats().num_compactions, 0);

        // insert a new entry, which will cause a compaction
        db.set("bbb", "x")?;
        assert_eq!(db.stats().num_compactions, 1);

        let stats = db.stats();
        assert_eq!(stats.occupied_bytes, 100 + "bbb?".len() + "x".len());
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
                min_compaction_threashold: 1000,
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
        let stats = db.stats();
        assert_eq!(stats.num_splits, 0);
        assert_eq!(stats.num_compactions, 0);

        db.set("zzz", &vec![7u8; 700])?;
        let stats = db.stats();
        assert_eq!(stats.num_compactions, 0);
        assert_eq!(stats.num_splits, 1);

        Ok(())
    })
}

#[test]
fn test_compaction_stats() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(
            dir,
            Config {
                max_shard_size: 20_000,
                min_compaction_threashold: 10_000,
                ..Default::default()
            },
        )?;

        let stats1 = db.stats();
        assert!(stats1.last_compaction_stats.is_empty());
        assert!(stats1.last_split_stats.is_empty());

        for i in 1..500 {
            db.set(&format!("key{i}"), &format!("val{i:0200}"))?;
        }

        let stats2 = db.stats();
        println!("stats2={stats2:?}");
        assert!(stats2.last_compaction_stats.is_empty());
        assert!(stats2.last_split_stats.len() > 0);

        for i in 500..10000 {
            db.set("key", &format!("val{i:0200}"))?;
        }

        let stats3 = db.stats();
        println!("{stats3:?}");
        assert!(stats3.last_compaction_stats.len() > 1);

        for _ in 0..1000 {
            assert!(db.get("key")?.is_some());
        }

        let stats4 = db.stats();
        assert!(stats4.last_compaction_stats.is_empty());
        assert!(stats4.last_split_stats.is_empty());

        Ok(())
    })
}
