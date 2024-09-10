mod common;

use std::sync::Arc;

use candystore::{CandyStore, CandyTypedStore, Config, Result};

use crate::common::run_in_tempdir;

#[test]
fn test_bigval() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(CandyStore::open(dir, Config::default())?);

        assert_eq!(db.set_big(b"mykey", &vec![0x99; 1_000_000])?, false);
        assert_eq!(db.get_big(b"yourkey")?, None);
        assert_eq!(db.get_big(b"mykey")?, Some(vec![0x99; 1_000_000]));
        assert_eq!(db.remove_big(b"mykey")?, true);
        assert_eq!(db.get_big(b"mykey")?, None);
        assert_eq!(db.set_big(b"mykey", &vec![0x88; 100_000])?, false);
        assert_eq!(db.set_big(b"mykey", &vec![0x77; 100_000])?, true);
        assert_eq!(db.get_big(b"mykey")?, Some(vec![0x77; 100_000]));

        let typed = CandyTypedStore::<String, Vec<u32>>::new(db);
        assert_eq!(typed.set_big("hello", &vec![123456789; 100_000])?, false);
        assert_eq!(typed.get_big("world")?, None);
        assert_eq!(typed.get_big("hello")?, Some(vec![123456789; 100_000]));
        assert_eq!(typed.remove_big("hello")?, true);
        assert_eq!(typed.remove_big("hello")?, false);

        Ok(())
    })
}
