mod common;

use std::sync::Arc;

use vicky_store::{Config, Result, VickyStore, VickyTypedCollection};

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

        for i in 0..10_000 {
            db.set_in_collection("xxx", &format!("my key {i}"), 
                "very long key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;
        }

        // make sure we survive splits
        assert!(db.stats().num_splits > 1);

        for (i, res) in db.iter_collection("xxx").enumerate() {
            let (k, _) = res?;
            assert_eq!(k, format!("my key {i}").as_bytes());
            db.remove_from_collection("xxx", &k)?;
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
