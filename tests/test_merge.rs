mod common;

use candystore::{CandyStore, Config, Result};

use crate::common::run_in_tempdir;

#[test]
fn test_merge() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        for i in 0u32..100_000 {
            db.set(&i.to_le_bytes(), "val")?;
        }
        assert_eq!(db.stats().num_entries(), 100_000);
        assert_eq!(db.stats().num_shards, 4);

        for i in 0u32..100_000 {
            if i % 16 != 0 {
                db.remove(&i.to_le_bytes())?.unwrap();
            }
        }
        assert_eq!(db.stats().num_entries(), 6250);
        db.merge_small_shards(0.25)?;
        assert_eq!(db.stats().num_shards, 1);

        for i in 0u32..100_000 {
            db.set(&i.to_le_bytes(), "val")?;
        }
        assert_eq!(db.stats().num_entries(), 100_000);
        assert_eq!(db.stats().num_shards, 4);
        for i in 0u32..100_000 {
            if i % 4 != 0 {
                db.remove(&i.to_le_bytes())?.unwrap();
            }
        }
        assert_eq!(db.stats().num_entries(), 25_000);
        db.merge_small_shards(0.25)?;
        assert_eq!(db.stats().num_shards, 2);

        for i in 0u32..100_000 {
            if (i % 4 == 0) && (i % 16 != 0) {
                db.remove(&i.to_le_bytes())?.unwrap();
            }
        }
        assert_eq!(db.stats().num_entries(), 6250);
        db.merge_small_shards(0.25)?;
        assert_eq!(db.stats().num_shards, 1);

        Ok(())
    })
}

#[test]
fn test_merge_with_expected_num_keys() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(
            dir,
            Config {
                expected_number_of_keys: 200_000,
                ..Default::default()
            },
        )?;

        assert_eq!(db.stats().num_entries(), 0);
        assert_eq!(db.stats().num_shards, 8);
        db.merge_small_shards(0.25)?;
        assert_eq!(db.stats().num_shards, 8);

        for i in 0u32..900_000 {
            db.set(&i.to_le_bytes(), "val")?;
        }
        assert_eq!(db.stats().num_entries(), 900_000);
        assert_eq!(db.stats().num_shards, 32);

        for i in 0u32..900_000 {
            if i % 16 != 0 {
                db.remove(&i.to_le_bytes())?.unwrap();
            }
        }
        assert_eq!(db.stats().num_entries(), 56250);
        db.merge_small_shards(0.25)?;
        assert_eq!(db.stats().num_shards, 8);

        Ok(())
    })
}
