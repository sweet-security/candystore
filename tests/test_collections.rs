mod common;

use vicky_store::{Config, Result, VickyStore};

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
