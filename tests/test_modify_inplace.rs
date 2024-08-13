mod common;

use candystore::{Config, ModifyStatus, Result, CandyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_modify_inplace() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        db.set("aaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;

        assert!(db.modify_inplace("zzz", "bbb", 7, None)?.is_key_missing());

        assert!(matches!(
            db.modify_inplace(
                "aaa",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                7,
                None
            )?,
            ModifyStatus::ValueTooLong(_, _, _)
        ));

        assert!(db.modify_inplace("aaa", "bbb", 7, None)?.was_replaced());
        assert_eq!(
            db.get("aaa")?,
            Some("aaaaaaabbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
        );

        assert!(db
            .modify_inplace("aaa", "ccc", 10, Some("aaa"))?
            .was_replaced());
        assert_eq!(
            db.get("aaa")?,
            Some("aaaaaaabbbcccaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
        );

        assert!(db
            .modify_inplace("aaa", "ddd", 10, Some("aaa"))?
            .is_mismatch());
        assert_eq!(
            db.get("aaa")?,
            Some("aaaaaaabbbcccaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
        );

        assert!(db.replace_inplace("bbb", "ABCD")?.is_key_missing());
        db.set("bbb", "ABCD")?;
        assert!(db.replace_inplace("bbb", "BCDE")?.was_replaced());
        assert!(db.replace_inplace("bbb", "xyz")?.is_wrong_length());
        assert!(db.replace_inplace("bbb", "wyxyz")?.is_wrong_length());
        assert_eq!(db.get("bbb")?, Some("BCDE".into()));

        Ok(())
    })
}
