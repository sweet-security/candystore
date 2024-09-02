mod common;

use std::sync::{atomic::AtomicUsize, Arc};

use candystore::{
    CandyStore, CandyTypedDeque, CandyTypedList, Config, GetOrCreateStatus, ListCompactionParams,
    ReplaceStatus, Result, SetStatus,
};

use crate::common::run_in_tempdir;

#[test]
fn test_lists() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(
            dir,
            Config {
                max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
                min_compaction_threashold: 10 * 1024,
                ..Default::default()
            },
        )?;

        db.set_in_list("texas", "dallas", "500,000")?;
        db.set_in_list("texas", "austin", "300,000")?;
        db.set_in_list("texas", "houston", "700,000")?;
        db.set_in_list("texas", "dallas", "450,000")?;

        assert_eq!(db.get_from_list("texas", "dallas")?, Some("450,000".into()));
        assert_eq!(db.get_from_list("texas", "austin")?, Some("300,000".into()));
        assert_eq!(
            db.get_from_list("texas", "houston")?,
            Some("700,000".into())
        );

        assert_eq!(db.iter_list("texas").count(), 3);
        assert_eq!(db.list_len("texas")?, 3);
        assert_eq!(db.iter_list("arkansas").count(), 0);
        assert_eq!(db.list_len("arkansas")?, 0);

        let items = db
            .iter_list("texas")
            .map(|res| res.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(items[0].0, "dallas".as_bytes());
        assert_eq!(items[2].0, "houston".as_bytes());

        db.discard_list("texas")?;
        assert_eq!(db.get_from_list("texas", "houston")?, None);
        assert_eq!(db.get_from_list("texas", "dallas")?, None);
        assert_eq!(db.iter_list("texas").count(), 0);

        db.set_in_list("xxx", "k1", "v1")?;
        db.set_in_list("xxx", "k2", "v2")?;
        db.set_in_list("xxx", "k3", "v3")?;
        db.set_in_list("xxx", "k4", "v4")?;

        // remove from the middle
        assert_eq!(db.remove_from_list("xxx", "k3")?, Some("v3".into()));
        assert_eq!(db.iter_list("xxx").count(), 3);
        assert_eq!(db.list_len("xxx")?, 3);
        // remove first
        assert_eq!(db.remove_from_list("xxx", "k1")?, Some("v1".into()));
        assert_eq!(db.iter_list("xxx").count(), 2);
        assert_eq!(db.list_len("xxx")?, 2);
        // remove last
        assert_eq!(db.remove_from_list("xxx", "k4")?, Some("v4".into()));
        assert_eq!(db.iter_list("xxx").count(), 1);
        assert_eq!(db.list_len("xxx")?, 1);
        // remove single
        assert_eq!(db.remove_from_list("xxx", "k2")?, Some("v2".into()));
        assert_eq!(db.iter_list("xxx").count(), 0);
        assert_eq!(db.list_len("xxx")?, 0);

        for i in 0..10_000 {
            db.set_in_list("xxx", &format!("my key {i}"), 
                "very long key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;
            assert_eq!(db.list_len("xxx")?, i + 1);
        }

        // make sure we survive splits
        assert!(db.stats().num_splits > 1);

        for (i, res) in db.iter_list("xxx").enumerate() {
            let (k, _) = res?;
            assert_eq!(k, format!("my key {i}").as_bytes());
            db.remove_from_list("xxx", &k)?;
            assert_eq!(db.list_len("xxx")?, 10_000 - i - 1);
        }

        assert_eq!(db.iter_list("xxx").count(), 0);

        Ok(())
    })
}

#[test]
fn test_typed_lists() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(CandyStore::open(dir, Config::default())?);

        let typed = CandyTypedList::<String, u64, u32>::new(db);
        typed.set("texas", &108, &2005)?;
        typed.set("texas", &555, &2006)?;
        typed.set("texas", &827, &2007)?;
        typed.set("texas", &123, &2008)?;
        typed.set("texas", &555, &2009)?;

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
fn test_lists_multithreading() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(CandyStore::open(dir, Config::default())?);

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
                        .set_in_list("xxx", &format!("key{idx1}"), &format!("val-{thd}"))?
                        .was_created()
                    {
                        created.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    } else {
                        replaced.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }

                    std::thread::yield_now();

                    let idx2: u8 = rand::random();
                    if let Some(v) = db.get_from_list("xxx", &format!("key{idx2}"))? {
                        assert!(v.starts_with(b"val-"));
                        gotten.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }

                    std::thread::yield_now();
                    let idx3: u8 = rand::random();
                    if db.remove_from_list("xxx", &format!("key{idx3}"))?.is_some() {
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

        let reamining = db.iter_list("xxx").count();
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
fn test_list_atomics() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        assert_eq!(
            db.get_or_create_in_list("xxx", "yyy", "1")?,
            GetOrCreateStatus::CreatedNew("1".into())
        );

        assert_eq!(
            db.get_or_create_in_list("xxx", "yyy", "2")?,
            GetOrCreateStatus::ExistingValue("1".into())
        );

        assert_eq!(
            db.replace_in_list("xxx", "yyy", "3", None)?,
            ReplaceStatus::PrevValue("1".into())
        );

        assert_eq!(
            db.replace_in_list("xxx", "zzz", "3", None)?,
            ReplaceStatus::DoesNotExist
        );

        assert_eq!(
            db.get_or_create_in_list("xxx", "yyy", "7")?,
            GetOrCreateStatus::ExistingValue("3".into())
        );

        assert_eq!(
            db.set_in_list("xxx", "yyy", "4")?,
            SetStatus::PrevValue("3".into())
        );

        assert_eq!(db.get_from_list("xxx", "yyy")?, Some("4".into()));

        Ok(())
    })
}

#[test]
fn test_queues() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        db.push_to_list_tail("mylist", "hello1")?;
        db.push_to_list_tail("mylist", "hello2")?;
        db.push_to_list_tail("mylist", "hello3")?;
        db.push_to_list_tail("mylist", "hello4")?;

        let mut items = vec![];
        while let Some((_uuid, v)) = db.pop_list_head("mylist")? {
            items.push(v);
        }
        assert_eq!(items, vec![b"hello1", b"hello2", b"hello3", b"hello4"]);

        db.push_to_list_tail("mylist", "hello5")?;
        db.push_to_list_tail("mylist", "hello6")?;
        db.push_to_list_tail("mylist", "hello7")?;
        db.push_to_list_tail("mylist", "hello8")?;

        let mut items = vec![];
        while let Some((_uuid, v)) = db.pop_list_tail("mylist")? {
            items.push(v);
        }
        assert_eq!(items, vec![b"hello8", b"hello7", b"hello6", b"hello5"]);

        db.push_to_list_tail("mylist", "hello9")?;
        db.push_to_list_tail("mylist", "hello10")?;
        db.push_to_list_tail("mylist", "hello11")?;
        db.push_to_list_tail("mylist", "hello12")?;
        db.push_to_list_tail("mylist", "hello13")?;
        db.push_to_list_tail("mylist", "hello14")?;

        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello9");
        assert_eq!(db.pop_list_tail("mylist")?.unwrap().1, b"hello14");
        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello10");
        assert_eq!(db.pop_list_tail("mylist")?.unwrap().1, b"hello13");
        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello11");
        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello12");
        assert_eq!(db.pop_list_head("mylist")?, None);

        db.push_to_list_head("mylist", "hello15")?;
        db.push_to_list_head("mylist", "hello16")?;
        db.push_to_list_head("mylist", "hello17")?;
        db.push_to_list_head("mylist", "hello18")?;

        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello18");
        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello17");
        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello16");
        assert_eq!(db.pop_list_head("mylist")?.unwrap().1, b"hello15");
        assert_eq!(db.pop_list_head("mylist")?, None);

        Ok(())
    })
}

#[test]
fn test_typed_queue() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(CandyStore::open(dir, Config::default())?);

        let queue = CandyTypedDeque::<String, u32>::new(db);
        assert_eq!(queue.pop_head("orders")?, None);

        for i in 10..30 {
            queue.push_tail("orders", &i)?;
        }
        for i in 10..20 {
            assert_eq!(queue.pop_head("orders")?, Some(i));
        }
        for i in (20..30).rev() {
            assert_eq!(queue.pop_tail("orders")?, Some(i));
        }

        assert_eq!(queue.pop_head("orders")?, None);

        queue.push_tail("orders", &100)?;
        queue.push_tail("orders", &101)?;
        queue.push_tail("orders", &102)?;
        queue.push_head("orders", &103)?;
        queue.push_head("orders", &104)?;
        queue.push_head("orders", &105)?;

        let items = queue
            .iter("orders")
            .map(|res| res.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(items, vec![105, 104, 103, 100, 101, 102]);

        let items = queue
            .iter_backwards("orders")
            .map(|res| res.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(items, vec![102, 101, 100, 103, 104, 105]);

        Ok(())
    })
}

#[test]
fn test_rev_iter() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        db.set_in_list("mylist", "item1", "xxx")?;
        db.set_in_list("mylist", "item2", "xxx")?;
        db.set_in_list("mylist", "item3", "xxx")?;
        db.set_in_list("mylist", "item4", "xxx")?;

        let items = db
            .iter_list("mylist")
            .map(|res| res.unwrap().0)
            .collect::<Vec<_>>();

        assert_eq!(items, vec![b"item1", b"item2", b"item3", b"item4"]);

        let items = db
            .iter_list_backwards("mylist")
            .map(|res| res.unwrap().0)
            .collect::<Vec<_>>();

        assert_eq!(items, vec![b"item4", b"item3", b"item2", b"item1"]);

        assert_eq!(
            db.peek_list_head("mylist")?,
            Some(("item1".into(), "xxx".into()))
        );

        assert_eq!(
            db.peek_list_tail("mylist")?,
            Some(("item4".into(), "xxx".into()))
        );

        Ok(())
    })
}

#[test]
fn test_promote() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        let items = || {
            db.iter_list("mylist")
                .map(|res| res.unwrap().0)
                .collect::<Vec<_>>()
        };

        db.set_in_list("mylist", "item1", "xxx")?;
        db.set_in_list("mylist", "item2", "xxx")?;
        db.set_in_list("mylist", "item3", "xxx")?;
        db.set_in_list("mylist", "item4", "xxx")?;

        assert_eq!(items(), vec![b"item1", b"item2", b"item3", b"item4"]);

        // no promotion happens
        db.set_in_list("mylist", "item2", "yyy")?;
        assert_eq!(items(), vec![b"item1", b"item2", b"item3", b"item4"]);

        // promote a middle element
        db.set_in_list_promoting("mylist", "item2", "zzz")?;
        assert_eq!(items(), vec![b"item1", b"item3", b"item4", b"item2"]);

        // promote head element
        db.set_in_list_promoting("mylist", "item1", "zzz")?;
        assert_eq!(items(), vec![b"item3", b"item4", b"item2", b"item1"]);

        // promote tail element
        db.set_in_list_promoting("mylist", "item1", "zzz")?;
        assert_eq!(items(), vec![b"item3", b"item4", b"item2", b"item1"]);

        Ok(())
    })
}

#[test]
fn test_typed_promote() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(CandyStore::open(dir, Config::default())?);
        let typed = CandyTypedList::<String, u32, String>::new(db);

        let items = || {
            typed
                .iter("mylist")
                .map(|res| res.unwrap().0)
                .collect::<Vec<_>>()
        };

        typed.set("mylist", &1, "xxx")?;
        typed.set("mylist", &2, "xxx")?;
        typed.set("mylist", &3, "xxx")?;
        typed.set("mylist", &4, "xxx")?;
        assert_eq!(items(), &[1, 2, 3, 4]);

        typed.set("mylist", &2, "yyy")?;
        assert_eq!(items(), &[1, 2, 3, 4]);

        typed.set_promoting("mylist", &2, "zzz")?;
        assert_eq!(items(), &[1, 3, 4, 2]);

        typed.set_promoting("mylist", &1, "zzz")?;
        assert_eq!(items(), &[3, 4, 2, 1]);

        typed.set_promoting("mylist", &1, "zzz")?;
        assert_eq!(items(), &[3, 4, 2, 1]);

        Ok(())
    })
}

#[test]
fn test_list_compaction() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        for i in 0u32..1000 {
            db.set_in_list("xxx", &i.to_le_bytes(), "yyy")?;
        }
        assert!(!db.compact_list_if_needed("xxx", ListCompactionParams::default())?);

        for i in 0u32..1000 {
            if i % 3 == 1 {
                assert!(db.remove_from_list("xxx", &i.to_le_bytes())?.is_some());
            }
        }

        let keys1 = db
            .iter_list("xxx")
            .map(|res| u32::from_le_bytes(res.unwrap().0.try_into().unwrap()))
            .collect::<Vec<_>>();
        for k in keys1.iter() {
            assert!(k % 3 != 1, "{k}");
        }

        assert!(db.compact_list_if_needed("xxx", ListCompactionParams::default())?);

        let keys2 = db
            .iter_list("xxx")
            .map(|res| u32::from_le_bytes(res.unwrap().0.try_into().unwrap()))
            .collect::<Vec<_>>();

        assert_eq!(keys1, keys2);

        Ok(())
    })
}
