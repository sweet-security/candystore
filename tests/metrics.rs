use candystore::{CandyStore, Config};
use std::time::Duration;

const ROW_WIDTH: u64 = (16 * 21) as u64;

#[test]
fn test_metrics_updates() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        initial_capacity: 1000,
        ..Default::default()
    };

    let db = CandyStore::open(dir.path(), config)?;

    let stats = db.stats();
    assert_eq!(stats.num_rows, 8);
    assert_eq!(stats.capacity, 8 * ROW_WIDTH);
    assert_eq!(stats.num_items, 0);
    assert_eq!(stats.fill_level(), 0.0);
    assert_eq!(stats.num_positive_lookups, 0);
    assert_eq!(stats.num_negative_lookups, 0);
    assert_eq!(stats.num_collisions, 0);
    assert_eq!(stats.last_remap_dur, Duration::ZERO);
    assert_eq!(stats.last_compaction_dur, Duration::ZERO);
    assert_eq!(stats.last_compaction_reclaimed_bytes, 0);
    assert_eq!(stats.last_compaction_moved_bytes, 0);
    assert_eq!(stats.num_read_ops, 0);
    assert_eq!(stats.num_read_bytes, 0);
    assert_eq!(stats.num_write_ops, 0);
    assert_eq!(stats.num_write_bytes, 0);
    assert_eq!(stats.num_created, 0);
    assert_eq!(stats.num_removed, 0);
    assert_eq!(stats.num_replaced, 0);
    assert_eq!(stats.written_bytes, 0);
    assert_eq!(stats.data_bytes, 0);
    assert_eq!(stats.waste_bytes, 0);

    db.set("key1", "val1")?;

    let stats = db.stats();
    assert_eq!(stats.num_items, 1);
    assert_eq!(stats.num_entries(), 1);
    assert_eq!(stats.num_inserts(), 1);
    assert_eq!(stats.num_updates(), 0);
    assert_eq!(stats.num_removals(), 0);
    assert_eq!(stats.num_created, 1);
    assert_eq!(stats.num_removed, 0);
    assert_eq!(stats.num_replaced, 0);
    assert!(stats.written_bytes > 0);
    assert!(stats.data_bytes > 0);
    assert_eq!(stats.waste_bytes, 0);
    assert_eq!(stats.num_write_ops, 1);
    assert!(stats.num_write_bytes > 0);
    assert!(stats.index_size_bytes > 0);
    assert_eq!(stats.num_data_files, 1);
    assert!(stats.occupied_bytes() > 0);

    db.set("key1", "val2")?;

    let stats = db.stats();
    assert_eq!(stats.num_items, 1);
    assert_eq!(stats.num_updates(), 1);
    assert_eq!(stats.num_created, 1);
    assert_eq!(stats.num_replaced, 1);
    assert_eq!(stats.num_removed, 0);
    assert!(stats.data_bytes > 0);
    assert!(stats.waste_bytes > 0);
    assert_eq!(stats.num_write_ops, 2);

    db.remove("key1")?;

    let stats = db.stats();
    assert_eq!(stats.num_items, 0);
    assert_eq!(stats.num_removals(), 1);
    assert!(stats.wasted_bytes() > 0);
    assert_eq!(stats.num_write_ops, 3);
    assert_eq!(stats.num_created, 1);
    assert_eq!(stats.num_replaced, 1);
    assert_eq!(stats.num_removed, 1);
    assert_eq!(stats.data_bytes, 0);

    assert_eq!(db.get("missing")?, None);
    assert_eq!(db.get("key1")?, None);

    let stats = db.stats();
    assert_eq!(stats.num_positive_lookups, 0);
    assert_eq!(stats.num_negative_lookups, 2);
    assert_eq!(stats.num_read_ops, 2);
    assert!(stats.num_read_bytes > 0);

    Ok(())
}

#[test]
fn test_metrics_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = Config {
        max_data_file_size: 4096,
        compaction_min_threshold: 10,
        ..Default::default()
    };

    let db = CandyStore::open(dir.path(), config)?;

    for i in 0..500 {
        db.set(
            "key",
            format!("value_that_is_long_enough_to_take_up_space_{}", i),
        )?;
    }

    for i in 0..100 {
        db.set(format!("other_key_{}", i), "val")?;
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let stats = db.stats();
    assert!(stats.written_bytes > 0);
    assert!(stats.num_replaced > 0);
    assert!(stats.data_bytes > 0);
    assert!(stats.num_items > 0);
    assert!(stats.capacity >= stats.num_items);
    assert!(stats.fill_level() > 0.0);
    assert!(stats.num_write_ops > 0);
    assert!(stats.num_write_bytes > 0);

    Ok(())
}
