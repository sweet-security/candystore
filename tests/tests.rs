use std::{
    collections::HashSet,
    sync::{atomic::AtomicUsize, Arc},
};

use rand::random;
use vicky_store::{Config, Error, Result, SecretKey, Stats, VickyStore};

fn run_in_tempdir(f: impl FnOnce(&str) -> Result<()>) -> Result<()> {
    let rand: u64 = random();
    let dir = format!("/tmp/vicky-{rand}");
    _ = std::fs::remove_dir_all(&dir);

    f(&dir)?;

    _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

const LONG_VAL: &str = "a very long valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

#[test]
fn test_logic() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(Config {
            dir_path: dir.into(),
            max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
            min_compaction_threashold: 10 * 1024,
            secret_key: SecretKey::new("very very secret")?,
        })?);

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

#[test]
fn test_loading() -> Result<()> {
    run_in_tempdir(|dir| {
        let config = Config {
            dir_path: dir.into(),
            max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
            min_compaction_threashold: 10 * 1024,
            secret_key: SecretKey::new("very very secret")?,
        };

        {
            let db = Arc::new(VickyStore::open(config.clone())?);

            for i in 0..1000 {
                db.insert(&format!("unique key {i}"), LONG_VAL)?;
            }

            assert!(db.stats().num_splits > 1);
            assert_eq!(db.iter().count(), 1000);
        }

        {
            let db = Arc::new(VickyStore::open(config.clone())?);

            assert_eq!(db.iter().count(), 1000);

            for res in db.iter() {
                let (key, val) = res?;
                assert_eq!(val, LONG_VAL.as_bytes());
                assert!(key.starts_with(b"unique key "));
            }
        }

        {
            let existing = std::fs::read_dir(dir)?
                .map(|res| res.unwrap().file_name().to_str().unwrap().to_string())
                .filter(|name| name.starts_with("shard_"))
                .collect::<Vec<_>>();

            std::fs::write(format!("{dir}/top_1234-5678"), "xxxx")?;
            std::fs::write(format!("{dir}/bottom_1234-5678"), "xxxx")?;
            std::fs::write(format!("{dir}/compact_1234-5678"), "xxxx")?;

            let (_, span) = existing[0].split_once("_").unwrap();
            let (start, end) = span.split_once("-").unwrap();
            let start = u32::from_str_radix(start, 16).unwrap();
            let end = u32::from_str_radix(end, 16).unwrap();
            let mid = (start + end) / 2;
            std::fs::write(format!("{dir}/shard_{start:04x}-{mid:04x}"), "xxxx")?;
            std::fs::write(format!("{dir}/shard_{mid:04x}-{end:04x}"), "xxxx")?;

            let db = Arc::new(VickyStore::open(config)?);

            assert!(!std::fs::exists(format!("{dir}/top_1234-5678"))?);
            assert!(!std::fs::exists(format!("{dir}/bottom_1234-5678"))?);
            assert!(!std::fs::exists(format!("{dir}/compact_1234-5678"))?);
            assert!(!std::fs::exists(format!(
                "{dir}/shard_{start:04x}-{mid:04x}"
            ))?);
            assert!(!std::fs::exists(format!(
                "{dir}/shard_{mid:04x}-{end:04x}"
            ))?);

            assert_eq!(db.iter().count(), 1000);
        }

        Ok(())
    })
}

#[test]
fn test_multithreaded() -> Result<()> {
    run_in_tempdir(|dir| {
        for attempt in 0..10 {
            let db = Arc::new(VickyStore::open(Config {
                dir_path: dir.into(),
                max_shard_size: 20 * 1024,
                min_compaction_threashold: 10 * 1024,
                secret_key: SecretKey::new("very very secret")?,
            })?);

            const NUM_ITEMS: usize = 10_000;
            let succ_gets = Arc::new(AtomicUsize::new(0));
            let succ_removals = Arc::new(AtomicUsize::new(0));

            let mut thds = Vec::new();
            for thid in 0..50 {
                let db = db.clone();
                let succ_gets = succ_gets.clone();
                let succ_removals = succ_removals.clone();
                let handle = std::thread::spawn(move || -> Result<()> {
                    let value = format!("data{thid}");
                    for i in 0..NUM_ITEMS {
                        let key = format!("key{i}");
                        db.insert(&key, &value)?;

                        if random::<f32>() > 0.8 {
                            if db.remove(&key)?.is_some() {
                                succ_removals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        } else {
                            let val2 = db.get(&key)?;
                            if let Some(val2) = val2 {
                                assert!(val2.starts_with(b"data"));
                                succ_gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                    }
                    Ok(())
                });
                //handle.join().unwrap().unwrap();
                thds.push(handle);
            }

            for thd in thds {
                thd.join().unwrap()?;
            }

            let gets = succ_gets.load(std::sync::atomic::Ordering::SeqCst);
            let removals = succ_removals.load(std::sync::atomic::Ordering::SeqCst);

            let stats = db.stats();
            println!(
                "[{attempt}] {stats:?} gets={gets} removals={removals} diff={}",
                gets - removals
            );

            assert_eq!(db.iter().count(), stats.num_entries);
            assert!(
                stats.num_entries >= (NUM_ITEMS * 7) / 10
                    && stats.num_entries <= (NUM_ITEMS * 9) / 10
            );
            db.clear()?;
        }
        Ok(())
    })
}

#[test]
fn test_modify_inplace() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(Config {
            dir_path: dir.into(),
            max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
            min_compaction_threashold: 10 * 1024,
            secret_key: SecretKey::new("very very secret")?,
        })?);

        db.insert("aaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;

        assert!(matches!(
            db.modify_inplace("zzz", "bbb", 7),
            Err(Error::KeyNotFound)
        ));

        assert!(matches!(
            db.modify_inplace("aaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7),
            Err(Error::ValueTooLong)
        ));

        db.modify_inplace("aaa", "bbb", 7)?;
        assert_eq!(
            db.get("aaa")?,
            Some("aaaaaaabbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
        );

        Ok(())
    })
}
