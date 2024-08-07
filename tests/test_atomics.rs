mod common;

use vicky_store::{Config, GetOrCreateStatus, Result, SetStatus, VickyStore};

use crate::common::run_in_tempdir;

#[test]
fn test_atomics() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = VickyStore::open(dir, Config::default())?;

        assert!(db.get_or_create("aaa", "1111")?.was_created());

        assert!(db.replace("aaa", "2222")?.was_replaced());

        assert!(db.get_or_create("aaa", "1111")?.already_exists());

        assert!(!db.replace("bbb", "3333")?.was_replaced());

        assert!(db.set("bbb", "4444")?.was_created());
        assert_eq!(db.set("bbb", "5555")?, SetStatus::PrevValue("4444".into()));

        assert_eq!(
            db.get_or_create("bbb", "6666")?,
            GetOrCreateStatus::ExistingValue("5555".into())
        );

        assert_eq!(db.get_or_create("cccc", "6666")?.value(), b"6666");
        assert_eq!(db.get_or_create("aaa", "6666")?.value(), b"2222");

        Ok(())
    })
}
