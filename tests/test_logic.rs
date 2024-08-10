mod common;

use std::collections::HashSet;

use vicky_store::{Config, Result, VickyStore};

use crate::common::{run_in_tempdir, LONG_VAL};

#[test]
fn test_logic() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(
            dir,
            Config {
                max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
                min_compaction_threashold: 10 * 1024,
                ..Default::default()
            },
        )?;

        assert!(db.get("my name")?.is_none());
        db.set("my_name", "inigo montoya")?;
        db.set("your_name", "dread pirate robert")?;

        assert!(db.contains("my_name")?);
        assert!(!db.contains("My NaMe")?);

        assert_eq!(db.get("my_name")?, Some("inigo montoya".into()));
        assert_eq!(db.get("your_name")?, Some("dread pirate robert".into()));
        db.set("your_name", "vizzini")?;
        assert_eq!(db.get("your_name")?, Some("vizzini".into()));
        assert_eq!(db.remove("my_name")?, Some("inigo montoya".into()));
        assert!(db.remove("my_name")?.is_none());
        assert!(db.get("my name")?.is_none());

        assert_eq!(db._num_entries(), 1);
        assert_eq!(db._num_compactions(), 0);
        assert_eq!(db._num_splits(), 0);

        for _ in 0..1000 {
            db.set(
                "a very long keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
                LONG_VAL,
            )?;
            assert!(db
                .remove("a very long keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")?
                .is_some());
        }

        let compactions1 = db._num_compactions();
        let splits1 = db._num_splits();
        assert_eq!(db._num_entries(), 1);
        assert!(compactions1 >= 2);
        assert_eq!(splits1, 0);

        for i in 0..1000 {
            db.set(&format!("unique key {i}"), LONG_VAL)?;
        }

        assert_eq!(db._num_entries(), 1001);
        assert_eq!(db._num_compactions(), compactions1);
        assert!(db._num_splits() > splits1);

        assert_eq!(db.get("your_name")?, Some("vizzini".into()));
        db.clear()?;
        assert_eq!(db.get("your_name")?, None);

        assert_eq!(db._num_entries(), 0);
        assert_eq!(db._num_compactions(), 0);
        assert_eq!(db._num_splits(), 0);

        for i in 0..1000 {
            db.set(&format!("unique key {i}"), LONG_VAL)?;
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
