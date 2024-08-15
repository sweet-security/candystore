use candystore::{CandyStore, Config, Result, HASH_BITS_TO_KEEP};

fn main() -> Result<()> {
    let db = CandyStore::open("/tmp/listtest", Config::default())?;
    db.clear()?;

    // force many elements to end up with the same PartedHash
    unsafe { HASH_BITS_TO_KEEP = 0xf000_000f_0000_00ff };

    for i in 0u32..10000 {
        //println!("push {i}");
        db.push_to_list_tail("xxx", &i.to_le_bytes())?;
    }

    for i in 0u32..10000 {
        //println!("pop {i}");
        assert_eq!(db.pop_list_head("xxx")?.unwrap().1, &i.to_le_bytes());
    }

    assert!(db.pop_list_head("xxx")?.is_none());

    for i in 0u32..10000 {
        //println!("push {i}");
        db.push_to_list_head("xxx", &i.to_le_bytes())?;
    }

    for i in 0u32..10000 {
        //println!("pop {i}");
        assert_eq!(db.pop_list_tail("xxx")?.unwrap().1, &i.to_le_bytes());
    }

    assert!(db.pop_list_head("xxx")?.is_none());

    for i in 0u32..1000 {
        db.set_in_list("xxx", &i.to_le_bytes(), &i.to_le_bytes())?;
    }
    for i in 400u32..600 {
        assert_eq!(
            db.remove_from_list("xxx", &i.to_le_bytes())?,
            Some(i.to_le_bytes().to_vec())
        );
    }

    for i in 0u32..100 {
        assert_eq!(
            db.remove_from_list("xxx", &i.to_le_bytes())?,
            Some(i.to_le_bytes().to_vec())
        );
    }

    for i in (900u32..1000).rev() {
        assert_eq!(
            db.remove_from_list("xxx", &i.to_le_bytes())?,
            Some(i.to_le_bytes().to_vec())
        );
    }

    let remaining = db
        .iter_list("xxx")
        .map(|res| u32::from_le_bytes(res.unwrap().unwrap().1.try_into().unwrap()))
        .collect::<Vec<_>>();

    let expectd = (100..400).chain(600..900).collect::<Vec<_>>();
    assert_eq!(remaining, expectd);

    db.discard_list("xxx")?;
    assert!(db.pop_list_head("xxx")?.is_none());

    Ok(())
}
