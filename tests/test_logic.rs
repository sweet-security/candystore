mod common;

use std::{collections::HashSet, sync::Arc};

use vicky_store::{Config, Result, Stats, VickyStore};

use crate::common::{run_in_tempdir, LONG_VAL};

#[test]
fn test_logic() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(
            dir,
            Config {
                max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
                min_compaction_threashold: 10 * 1024,
                ..Default::default()
            },
        )?);

        assert!(db.get("my name")?.is_none());
        db.insert("my_name", "inigo montoya")?;
        db.insert("your_name", "dread pirate robert")?;

        assert_eq!(db.get("my_name")?, Some("inigo montoya".into()));
        assert_eq!(db.get("your_name")?, Some("dread pirate robert".into()));
        db.insert("your_name", "vizzini")?;
        assert_eq!(db.get("your_name")?, Some("vizzini".into()));
        assert_eq!(db.remove("my_name")?, Some("inigo montoya".into()));
        assert!(db.remove("my_name")?.is_none());
        assert!(db.get("my name")?.is_none());

        let stats = db.stats();
        assert_eq!(stats.num_entries, 1);
        assert_eq!(stats.num_compactions, 0);
        assert_eq!(stats.num_splits, 0);

        for _ in 0..1000 {
            db.insert(
                "a very long keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
                LONG_VAL,
            )?;
            assert!(db
                .remove("a very long keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")?
                .is_some());
        }

        let stats = db.stats();
        assert_eq!(stats.num_entries, 1);
        assert!(stats.num_compactions >= 2);
        assert_eq!(stats.num_splits, 0);

        for i in 0..1000 {
            db.insert(&format!("unique key {i}"), LONG_VAL)?;
        }

        let stats2 = db.stats();
        assert_eq!(stats2.num_entries, 1001);
        assert_eq!(stats2.num_compactions, stats.num_compactions);
        assert!(stats2.num_splits > stats.num_splits);

        assert_eq!(db.get("your_name")?, Some("vizzini".into()));
        db.clear()?;
        assert_eq!(db.get("your_name")?, None);

        assert_eq!(
            db.stats(),
            Stats {
                num_compactions: 0,
                num_entries: 0,
                num_splits: 0
            }
        );

        for i in 0..1000 {
            db.insert(&format!("unique key {i}"), LONG_VAL)?;
        }

        let mut all_keys = HashSet::new();

        for res in db.iter() {
            let (key, val) = res?;
            assert_eq!(val, LONG_VAL.as_bytes());
            assert!(key.starts_with(b"unique key "));
            all_keys.insert(key);
        }

        assert_eq!(all_keys.len(), 1000);

        all_keys.clear();

        let cookie = {
            let mut iter1 = db.iter();
            for _ in 0..100 {
                let res = iter1.next().unwrap();
                let (key, _) = res?;
                all_keys.insert(key);
            }
            iter1.cookie()
        };

        for res in db.iter_from_cookie(cookie) {
            let (key, _) = res?;
            all_keys.insert(key);
        }

        assert_eq!(all_keys.len(), 1000);

        Ok(())
    })
}
