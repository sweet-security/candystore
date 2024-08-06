mod common;

use std::sync::Arc;

use vicky_store::{Config, Result, VickyStore, VickyTypedKey, VickyTypedStore};

use crate::common::run_in_tempdir;

use databuf::{Decode, Encode};

#[derive(Debug, Encode, Decode)]
struct MyKey {
    x: u32,
    y: u64,
    z: String,
}

impl VickyTypedKey for MyKey {
    const TYPE_ID: u32 = 0x3476a551;
}

#[derive(Debug, PartialEq, Eq, Encode, Decode)]
struct MyVal {
    a: [u8; 7],
    b: i16,
    c: String,
}

#[test]
fn test_typed() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = Arc::new(VickyStore::open(dir, Config::default())?);

        let typed = VickyTypedStore::<MyKey, MyVal>::new(db.clone());
        typed.set(
            MyKey {
                x: 12,
                y: 34,
                z: "hello".into(),
            },
            MyVal {
                a: [7, 7, 7, 7, 7, 7, 7],
                b: 31415,
                c: "world".into(),
            },
        )?;

        assert_eq!(
            typed
                .get(&MyKey {
                    x: 12,
                    y: 34,
                    z: "hello".into(),
                })
                .unwrap(),
            Some(MyVal {
                a: [7, 7, 7, 7, 7, 7, 7],
                b: 31415,
                c: "world".into()
            })
        );

        assert_eq!(
            typed
                .get(&MyKey {
                    x: 12,
                    y: 34,
                    z: "ola".into(),
                })
                .unwrap(),
            None
        );

        assert_eq!(
            typed
                .remove(&MyKey {
                    x: 12,
                    y: 34,
                    z: "hello".into(),
                })
                .unwrap(),
            Some(MyVal {
                a: [7, 7, 7, 7, 7, 7, 7],
                b: 31415,
                c: "world".into()
            })
        );

        assert_eq!(
            typed
                .get(&MyKey {
                    x: 12,
                    y: 34,
                    z: "hello".into(),
                })
                .unwrap(),
            None
        );

        // two typed-stores can co-exist on the same underlying store
        let typed2 = VickyTypedStore::<String, Vec<u32>>::new(db);
        typed2.set("hello".into(), vec![1, 2, 3])?;
        typed2.set("world".into(), vec![4, 5, 6, 7])?;

        assert_eq!(typed2.get("hello").unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(typed2.get("world").unwrap(), Some(vec![4, 5, 6, 7]));

        assert_eq!(typed2.remove("hello").unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(typed2.remove("hello").unwrap(), None);

        Ok(())
    })
}
