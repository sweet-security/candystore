mod common;

use std::sync::{atomic::AtomicUsize, Arc};

use vicky_store::{
    Config, GetOrCreateStatus, ReplaceStatus, Result, SetStatus, VickyStore, VickyTypedCollection,
};

use crate::common::run_in_tempdir;

#[test]
fn test_collections() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(
            dir,
            Config {
                max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
                min_compaction_threashold: 10 * 1024,
                ..Default::default()
            },
        )?;

        db.set_in_collection("texas", "dallas", "500,000")?;
        db.set_in_collection("texas", "austin", "300,000")?;
        db.set_in_collection("texas", "houston", "700,000")?;
        db.set_in_collection("texas", "dallas", "450,000")?;

        assert_eq!(
            db.get_from_collection("texas", "dallas")?,
            Some("450,000".into())
        );
        assert_eq!(
            db.get_from_collection("texas", "austin")?,
            Some("300,000".into())
        );
        assert_eq!(
            db.get_from_collection("texas", "houston")?,
            Some("700,000".into())
        );

        assert_eq!(db.iter_collection("texas").count(), 3);
        assert_eq!(db.iter_collection("arkansas").count(), 0);

        let items = db
            .iter_collection("texas")
            .map(|res| res.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(items[0].0, "dallas".as_bytes());
        assert_eq!(items[2].0, "houston".as_bytes());

        db.discard_collection("texas")?;
        assert_eq!(db.get_from_collection("texas", "houston")?, None);
        assert_eq!(db.get_from_collection("texas", "dallas")?, None);
        assert_eq!(db.iter_collection("texas").count(), 0);

        db.set_in_collection("xxx", "k1", "v1")?;
        db.set_in_collection("xxx", "k2", "v2")?;
        db.set_in_collection("xxx", "k3", "v3")?;
        db.set_in_collection("xxx", "k4", "v4")?;

        // remove from the middle
        assert_eq!(db.remove_from_collection("xxx", "k3")?, Some("v3".into()));
        assert_eq!(db.iter_collection("xxx").count(), 3);
        // remove first
        assert_eq!(db.remove_from_collection("xxx", "k1")?, Some("v1".into()));
        assert_eq!(db.iter_collection("xxx").count(), 2);
        // remove last
        assert_eq!(db.remove_from_collection("xxx", "k4")?, Some("v4".into()));
        assert_eq!(db.iter_collection("xxx").count(), 1);
        // remove single
        assert_eq!(db.remove_from_collection("xxx", "k2")?, Some("v2".into()));
        assert_eq!(db.iter_collection("xxx").count(), 0);

        assert_eq!(db.collection_len("xxx")?, 0);

        assert_eq!(db.collection_len("floop")?, 0);

        for i in 0..10_000 {
            db.set_in_collection("xxx", &format!("my key {i}"), 
                "very long key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;
        }

        assert_eq!(db.collection_len("xxx")?, 10_000);

        // make sure we survive splits
        assert!(db.stats().num_splits > 1);

        for (i, res) in db.iter_collection("xxx").enumerate() {
            let (k, _) = res?;
            assert_eq!(k, format!("my key {i}").as_bytes());
            db.remove_from_collection("xxx", &k)?;
            let remaining = 10_000 - 1 - i;
            assert_eq!(db.collection_len("xxx")?, remaining);
        }

        assert_eq!(db.iter_collection("xxx").count(), 0);

        Ok(())
    })
}

#[test]
fn test_typed_collections() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(dir, Config::default())?);

        let typed = VickyTypedCollection::<String, u64, u32>::new(db);
        typed.set("texas".into(), 108, 2005)?;
        typed.set("texas".into(), 555, 2006)?;
        typed.set("texas".into(), 827, 2007)?;
        typed.set("texas".into(), 123, 2008)?;
        typed.set("texas".into(), 555, 2009)?;

        assert_eq!(typed.get("texas", &555)?, Some(2009));
        assert_eq!(typed.get("texas", &66666666)?, None);

        assert!(typed.remove("texas", &827)?.is_some());
        assert!(typed.remove("texas", &827)?.is_none());
        assert!(typed.remove("texas", &66666666)?.is_none());

        let items = typed
            .iter("texas")
            .map(|res| res.unwrap().1)
            .collect::<Vec<_>>();
        assert_eq!(items, vec![2005, 2009, 2008]);

        Ok(())
    })
}

#[test]
fn test_collections_multithreading() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(dir, Config::default())?);

        let removed = Arc::new(AtomicUsize::new(0));
        let created = Arc::new(AtomicUsize::new(0));
        let gotten = Arc::new(AtomicUsize::new(0));
        let replaced = Arc::new(AtomicUsize::new(0));

        let num_thds = 10;
        let num_iters = 1000;

        let mut handles = vec![];
        for thd in 0..num_thds {
            let db = db.clone();
            let removed = removed.clone();
            let created = created.clone();
            let replaced = replaced.clone();
            let gotten = gotten.clone();
            let h = std::thread::spawn(move || {
                for _ in 0..num_iters {
                    let idx1: u8 = rand::random();
                    if db
                        .set_in_collection("xxx", &format!("key{idx1}"), &format!("val-{thd}"))?
                        .was_created()
                    {
                        created.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    } else {
                        replaced.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }

                    std::thread::yield_now();

                    let idx2: u8 = rand::random();
                    if let Some(v) = db.get_from_collection("xxx", &format!("key{idx2}"))? {
                        assert!(v.starts_with(b"val-"));
                        gotten.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }

                    std::thread::yield_now();
                    let idx3: u8 = rand::random();
                    if db
                        .remove_from_collection("xxx", &format!("key{idx3}"))?
                        .is_some()
                    {
                        removed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    std::thread::yield_now();
                }
                Result::<()>::Ok(())
            });
            handles.push(h);
        }

        for h in handles {
            h.join().unwrap()?;
        }

        let reamining = db.iter_collection("xxx").count();
        let created = created.load(std::sync::atomic::Ordering::SeqCst);
        let replaced = replaced.load(std::sync::atomic::Ordering::SeqCst);
        let removed = removed.load(std::sync::atomic::Ordering::SeqCst);
        let gotten = gotten.load(std::sync::atomic::Ordering::SeqCst);

        assert_eq!(created - removed, reamining);
        assert_eq!(created + replaced, num_iters * num_thds);

        println!("created={created} replaced={replaced} removed={removed} gotten={gotten} reamining={reamining}");

        Ok(())
    })
}

#[test]
fn test_collections_atomics() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(dir, Config::default())?;

        assert_eq!(
            db.get_or_create_in_collection("xxx", "yyy", "1")?,
            GetOrCreateStatus::CreatedNew("1".into())
        );

        assert_eq!(
            db.get_or_create_in_collection("xxx", "yyy", "2")?,
            GetOrCreateStatus::ExistingValue("1".into())
        );

        assert_eq!(
            db.replace_in_collection("xxx", "yyy", "3")?,
            ReplaceStatus::PrevValue("1".into())
        );

        assert_eq!(
            db.replace_in_collection("xxx", "zzz", "3")?,
            ReplaceStatus::DoesNotExist
        );

        assert_eq!(
            db.get_or_create_in_collection("xxx", "yyy", "7")?,
            GetOrCreateStatus::ExistingValue("3".into())
        );

        assert_eq!(
            db.set_in_collection("xxx", "yyy", "4")?,
            SetStatus::PrevValue("3".into())
        );

        assert_eq!(db.get_from_collection("xxx", "yyy")?, Some("4".into()));

        Ok(())
    })
}
