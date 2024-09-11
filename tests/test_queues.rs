mod common;

use candystore::{CandyStore, Config, Result};

use crate::common::run_in_tempdir;

#[test]
fn test_queues() -> Result<()> {
    run_in_tempdir(|dir| {
        let db = CandyStore::open(dir, Config::default())?;

        db.push_to_queue_tail("work", "item1")?;
        db.push_to_queue_tail("work", "item2")?;
        db.push_to_queue_tail("work", "item3")?;
        assert_eq!(db.pop_queue_head("work")?, Some("item1".into()));
        assert_eq!(db.pop_queue_head("work")?, Some("item2".into()));
        assert_eq!(db.pop_queue_head("work")?, Some("item3".into()));
        assert_eq!(db.pop_queue_head("work")?, None);

        db.push_to_queue_head("rev", "item1")?;
        db.push_to_queue_head("rev", "item2")?;
        db.push_to_queue_head("rev", "item3")?;
        assert_eq!(db.pop_queue_tail("rev")?, Some("item1".into()));
        assert_eq!(db.pop_queue_tail("rev")?, Some("item2".into()));
        assert_eq!(db.pop_queue_tail("rev")?, Some("item3".into()));
        assert_eq!(db.pop_queue_tail("rev")?, None);

        assert_eq!(db.queue_len("work")?, 0);

        for i in 1000u32..2000 {
            db.push_to_queue_tail("work", &i.to_le_bytes())?;
        }
        assert_eq!(db.queue_len("work")?, 1000);
        assert_eq!(db.queue_len("joke")?, 0);

        for (i, res) in db.iter_queue("work").enumerate() {
            let (idx, val) = res?;
            let v = u32::from_le_bytes(val.try_into().unwrap());
            assert_eq!(v, 1000 + i as u32);

            // create some holes
            if v % 5 == 0 {
                assert!(db.remove_from_queue("work", idx)?.is_some());
            }
        }

        let mut count = 0;
        for res in db.iter_queue("work") {
            let (_, val) = res?;
            let v = u32::from_le_bytes(val.try_into().unwrap());
            assert_ne!(v % 5, 0);
            count += 1;
        }
        assert!(count == 800);

        let mut count2 = 0;
        while let Some(val) = db.pop_queue_head("work")? {
            let v = u32::from_le_bytes(val.try_into().unwrap());
            assert_ne!(v % 5, 0);
            count2 += 1;
            if count2 > 400 {
                break;
            }
        }
        while let Some(val) = db.pop_queue_tail("work")? {
            let v = u32::from_le_bytes(val.try_into().unwrap());
            assert_ne!(v % 5, 0);
            count2 += 1;
        }

        assert_eq!(count, count2);
        assert_eq!(db.queue_len("work")?, 0);

        db.push_to_queue_tail("work", "item1")?;
        db.push_to_queue_tail("work", "item2")?;
        db.push_to_queue_tail("work", "item3")?;
        assert_eq!(db.queue_len("work")?, 3);
        db.extend_queue("work", ["item4", "item5"].iter())?;
        assert_eq!(db.queue_len("work")?, 5);

        let items = db
            .iter_queue("work")
            .map(|res| std::str::from_utf8(&res.unwrap().1).unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(items, ["item1", "item2", "item3", "item4", "item5"]);

        db.discard_queue("work")?;
        assert_eq!(db.queue_len("work")?, 0);

        db.extend_queue("work", (1u32..10).map(|i| i.to_le_bytes()))?;
        let items = db
            .iter_queue("work")
            .map(|res| u32::from_le_bytes(res.unwrap().1.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(items, (1u32..10).collect::<Vec<_>>());

        let items = db
            .iter_queue_backwards("work")
            .map(|res| u32::from_le_bytes(res.unwrap().1.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(items, (1u32..10).rev().collect::<Vec<_>>());

        Ok(())
    })
}
