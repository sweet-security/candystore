mod common;

use std::collections::HashMap;

use vicky_store::{Config, Result, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_collections() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(dir, Config::default())?;

        db.set_in_collection("class1", "john", "100")?;
        db.set_in_collection("class1", "bob", "90")?;
        db.set_in_collection("class1", "greg", "80")?;
        assert_eq!(
            db.get_from_collection("class1", "john")?,
            Some("100".into())
        );
        db.set_in_collection("class1", "john", "99")?;
        assert_eq!(db.get_from_collection("class1", "john")?, Some("99".into()));

        //assert!(db.remove_from_collection("class1", "bob")?.is_some());
        //assert!(db.remove_from_collection("class1", "bob")?.is_none());

        let items = db
            .iter_collection("class1")
            .map(|res| res.unwrap())
            .collect::<HashMap<_, _>>();
        assert_eq!(items.len(), 3);
        assert_eq!(items.get("john".as_bytes()), Some(&"99".into()));
        assert_eq!(items.get("greg".as_bytes()), Some(&"80".into()));

        for i in 0..5000 {
            db.set_in_collection("mycoll", &format!("key{i}"), &format!("xxx{i}"))?;
        }
        for i in 100..500 {
            db.set_in_collection("mycoll", &format!("key{i}"), &format!("xxx{i}"))?;
        }
        // for i in 800..500 {
        //     db._in_collection("mycoll", &format!("key{i}"), &format!("xxx{i}"))?;
        // }

        let items = db
            .iter_collection("mycoll")
            .map(|res| res.unwrap())
            .collect::<Vec<_>>();
        println!("{}", items.len());

        Ok(())
    })
}
