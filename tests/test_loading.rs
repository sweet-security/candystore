mod common;

use vicky_store::{Config, Result, VickyStore};

use crate::common::{run_in_tempdir, LONG_VAL};

#[test]
fn test_loading() -> Result<()> {
    run_in_tempdir(|dir| {
        let config = Config {
            max_shard_size: 20 * 1024, // use small files to force lots of splits and compactions
            min_compaction_threashold: 10 * 1024,
            ..Default::default()
        };

        {
            let db = VickyStore::open(dir, config.clone())?;

            for i in 0..1000 {
                db.insert(&format!("unique key {i}"), LONG_VAL)?;
            }

            assert!(db.stats().num_splits > 1);
            assert_eq!(db.iter().count(), 1000);
        }

        {
            let db = VickyStore::open(dir, config.clone())?;

            assert_eq!(db.iter().count(), 1000);

            for res in db.iter() {
                let (key, val) = res?;
                assert_eq!(val, LONG_VAL.as_bytes());
                assert!(key.starts_with(b"unique key "));
            }
        }

        {
            let existing = std::fs::read_dir(dir)?
                .map(|res| res.unwrap().file_name().to_str().unwrap().to_string())
                .filter(|name| name.starts_with("shard_"))
                .collect::<Vec<_>>();

            std::fs::write(format!("{dir}/top_1234-5678"), "xxxx")?;
            std::fs::write(format!("{dir}/bottom_1234-5678"), "xxxx")?;
            std::fs::write(format!("{dir}/compact_1234-5678"), "xxxx")?;

            let (_, span) = existing[0].split_once("_").unwrap();
            let (start, end) = span.split_once("-").unwrap();
            let start = u32::from_str_radix(start, 16).unwrap();
            let end = u32::from_str_radix(end, 16).unwrap();
            let mid = (start + end) / 2;
            std::fs::write(format!("{dir}/shard_{start:04x}-{mid:04x}"), "xxxx")?;
            std::fs::write(format!("{dir}/shard_{mid:04x}-{end:04x}"), "xxxx")?;

            let db = VickyStore::open(dir, config)?;

            assert!(!std::fs::exists(format!("{dir}/top_1234-5678"))?);
            assert!(!std::fs::exists(format!("{dir}/bottom_1234-5678"))?);
            assert!(!std::fs::exists(format!("{dir}/compact_1234-5678"))?);
            assert!(!std::fs::exists(format!(
                "{dir}/shard_{start:04x}-{mid:04x}"
            ))?);
            assert!(!std::fs::exists(format!(
                "{dir}/shard_{mid:04x}-{end:04x}"
            ))?);

            assert_eq!(db.iter().count(), 1000);
        }

        Ok(())
    })
}
