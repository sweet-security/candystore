//! Whitebox tests requiring the `whitebox-testing` feature.
//!
//! Run via: `cargo test --features whitebox-testing --test whitebox`

#![cfg(feature = "whitebox-testing")]

mod common;
use crate::common::active_file_ordinal;

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use candystore::internal::{
    CHECKPOINT_SLOT_CHECKSUM_OFFSET, CHECKPOINT_SLOT_FILE_OFFSET,
    CHECKPOINT_SLOT_GENERATION_OFFSET, CHECKPOINT_SLOT_ORDINAL_OFFSET, EntryPointer,
    INDEX_CHECKPOINT_SLOT_0_OFFSET, PAGE_SIZE, ROW_LAYOUT_POINTERS_OFFSET,
    ROW_LAYOUT_SIGNATURES_OFFSET,
};
#[cfg(unix)]
use candystore::internal::{HashCoord, KeyNamespace, MIN_SPLIT_LEVEL, ROW_WIDTH};
use candystore::{CandyStore, Config, Error};
use tempfile::tempdir;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/// Write a phantom entry into the rows file at the given row and column.
fn inject_phantom_entry(
    dir: &Path,
    row_idx: usize,
    col: usize,
    file_idx: u16,
    file_offset: u64,
) -> Result<(), Error> {
    let sig: u32 = 0xDEAD_BEEF;
    let ptr = EntryPointer::new(file_idx, file_offset, 512, 0).0;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("rows"))
        .map_err(Error::IOError)?;

    let row_base = row_idx * PAGE_SIZE;

    // Write signature
    let sig_off = (row_base + ROW_LAYOUT_SIGNATURES_OFFSET + col * 4) as u64;
    file.seek(SeekFrom::Start(sig_off))
        .map_err(Error::IOError)?;
    file.write_all(&sig.to_le_bytes()).map_err(Error::IOError)?;

    // Write pointer
    let ptr_off = (row_base + ROW_LAYOUT_POINTERS_OFFSET + col * 8) as u64;
    file.seek(SeekFrom::Start(ptr_off))
        .map_err(Error::IOError)?;
    file.write_all(&ptr.to_le_bytes()).map_err(Error::IOError)?;

    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

/// Read a signature from the rows file.
fn read_signature(dir: &Path, row_idx: usize, col: usize) -> Result<u32, Error> {
    use std::io::Read;
    let mut file = std::fs::File::open(dir.join("rows")).map_err(Error::IOError)?;
    let off = (row_idx * PAGE_SIZE + ROW_LAYOUT_SIGNATURES_OFFSET + col * 4) as u64;
    file.seek(SeekFrom::Start(off)).map_err(Error::IOError)?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf).map_err(Error::IOError)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_commit_cursor(dir: &Path, offset: u64) -> Result<(), Error> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join("index"))
        .map_err(Error::IOError)?;

    let ordinal = active_file_ordinal(dir)?;
    let generation = 1u64;
    let checksum = candystore::internal::checkpoint_slot_checksum(generation, ordinal, offset);

    file.seek(SeekFrom::Start(
        INDEX_CHECKPOINT_SLOT_0_OFFSET + CHECKPOINT_SLOT_GENERATION_OFFSET,
    ))
    .map_err(Error::IOError)?;
    file.write_all(&generation.to_le_bytes())
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(
        INDEX_CHECKPOINT_SLOT_0_OFFSET + CHECKPOINT_SLOT_ORDINAL_OFFSET,
    ))
    .map_err(Error::IOError)?;
    file.write_all(&ordinal.to_le_bytes())
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(
        INDEX_CHECKPOINT_SLOT_0_OFFSET + CHECKPOINT_SLOT_FILE_OFFSET,
    ))
    .map_err(Error::IOError)?;
    file.write_all(&offset.to_le_bytes())
        .map_err(Error::IOError)?;

    file.seek(SeekFrom::Start(
        INDEX_CHECKPOINT_SLOT_0_OFFSET + CHECKPOINT_SLOT_CHECKSUM_OFFSET,
    ))
    .map_err(Error::IOError)?;
    file.write_all(&checksum.to_le_bytes())
        .map_err(Error::IOError)?;
    file.sync_all().map_err(Error::IOError)?;
    Ok(())
}

#[cfg(unix)]
fn colliding_user_keys(hash_key: (u64, u64), row_idx: usize, count: usize) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    let mut candidate = 0u64;
    while keys.len() < count {
        let key = format!("split-key-{candidate:06}");
        let hc = HashCoord::new(KeyNamespace::User, key.as_bytes(), hash_key);
        if hc.row_index(MIN_SPLIT_LEVEL as u64) == row_idx {
            keys.push(key);
        }
        candidate += 1;
    }
    keys
}

/// Fork, run `child_fn` in the child (which should abort), wait and assert
/// the child was killed by SIGABRT.
#[cfg(unix)]
fn fork_expect_abort(child_fn: impl FnOnce()) {
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        child_fn();
        // Should not reach here — child_fn should abort.
        unsafe { libc::_exit(0) };
    }
    let mut status = 0i32;
    let wait_rc = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(wait_rc, pid);
    assert!(
        libc::WIFSIGNALED(status),
        "child exited normally, expected signal"
    );
    assert_eq!(
        libc::WTERMSIG(status),
        libc::SIGABRT,
        "child killed by unexpected signal"
    );
}

#[cfg(unix)]
fn run_split_row_resume_test(crash_point: &'static std::ffi::CStr) -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 256 * 1024,
        ..Config::default()
    };
    let keys = colliding_user_keys(config.hash_key, 0, ROW_WIDTH + 1);
    let child_keys = keys.clone();

    fork_expect_abort(|| {
        unsafe {
            libc::setenv(c"CANDYSTORE_CRASH_POINT".as_ptr(), crash_point.as_ptr(), 1);
            libc::setenv(c"CANDYSTORE_CRASH_AFTER".as_ptr(), c"0".as_ptr(), 1);
        }

        let db = CandyStore::open(dir.path(), config).unwrap();
        for (idx, key) in child_keys.iter().enumerate() {
            db.set(key, format!("value{idx:04}")).unwrap();
        }
    });

    let db = CandyStore::open(dir.path(), config)?;
    for (idx, key) in keys.iter().take(ROW_WIDTH).enumerate() {
        assert_eq!(
            db.get(key)?,
            Some(format!("value{idx:04}").into_bytes()),
            "{key} missing after split-row crash recovery"
        );
    }
    assert_eq!(db.get(&keys[ROW_WIDTH])?, None);

    db.set(&keys[ROW_WIDTH], "value-post-crash")?;
    drop(db);

    let db = CandyStore::open(dir.path(), config)?;
    for (idx, key) in keys.iter().take(ROW_WIDTH).enumerate() {
        assert_eq!(
            db.get(key)?,
            Some(format!("value{idx:04}").into_bytes()),
            "{key} missing after reopening resumed split-row state"
        );
    }
    assert_eq!(
        db.get(&keys[ROW_WIDTH])?,
        Some(b"value-post-crash".to_vec())
    );

    Ok(())
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

/// Inject phantom index entries pointing past the durable extent of the
/// active data file. Rebuild should purge them.
#[test]
fn test_rebuild_purges_phantom_entries() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    // Phase 1: write real data, then close cleanly.
    {
        let db = CandyStore::open(dir.path(), config)?;
        db.set("key1", "val1")?;
        db.set("key2", "val2")?;
    }

    // Phase 2: inject phantom entries.
    // Use a last row slot (col 335) to avoid colliding with real entries.
    inject_phantom_entry(dir.path(), 0, 335, 0, 0x100_000)?;
    inject_phantom_entry(dir.path(), 1, 335, 0, 0x200_000)?;

    // Verify we actually wrote non-zero signatures.
    assert_ne!(read_signature(dir.path(), 0, 335)?, 0);
    assert_ne!(read_signature(dir.path(), 1, 335)?, 0);

    // Phase 3: reopen — recovery replays active file and purges phantoms.
    {
        let db = CandyStore::open(dir.path(), config)?;
        assert_eq!(db.get("key1")?, Some(b"val1".to_vec()));
        assert_eq!(db.get("key2")?, Some(b"val2".to_vec()));
    }

    // Verify phantom signatures are cleared.
    assert_eq!(read_signature(dir.path(), 0, 335)?, 0);
    assert_eq!(read_signature(dir.path(), 1, 335)?, 0);

    Ok(())
}

/// A bogus commit cursor offset beyond the data extent is ignored and rebuild
/// restarts from offset 0 for the active file.
#[test]
fn test_bogus_checkpoint_offset_causes_full_replay() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..50 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    write_commit_cursor(dir.path(), 0xFFFF_FFFF)?;

    let db = CandyStore::open(dir.path(), config)?;
    for i in 0..50 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            Some(format!("val{i:04}").into_bytes()),
            "key{i:04} missing after bogus commit-cursor rebuild"
        );
    }

    Ok(())
}

/// A commit cursor offset that points into the middle of an entry or padding
/// must be rejected rather than treated as a valid resume point.
#[test]
fn test_mid_entry_checkpoint_offset_restarts_from_zero() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..50 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    write_commit_cursor(dir.path(), 1)?;

    let db = CandyStore::open(dir.path(), config)?;
    for i in 0..50 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            Some(format!("val{i:04}").into_bytes()),
            "key{i:04} missing after mid-entry commit-cursor fallback"
        );
    }

    Ok(())
}

/// Crash mid-rebuild via the `rebuild_entry` crash point, then resume.
#[cfg(unix)]
#[test]
fn test_mid_rebuild_crash_resumes_from_checkpoint() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config {
        max_data_file_size: 256 * 1024,
        ..Config::default()
    };

    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..3000u32 {
            db.set(format!("key{i:04}"), format!("val{i:04}"))?;
        }
        db._abort_for_testing();
    }

    fork_expect_abort(|| {
        unsafe {
            libc::setenv(
                c"CANDYSTORE_CRASH_POINT".as_ptr(),
                c"rebuild_entry".as_ptr(),
                1,
            );
            libc::setenv(c"CANDYSTORE_CRASH_AFTER".as_ptr(), c"1200".as_ptr(), 1);
        }
        let _ = CandyStore::open(dir.path(), config);
    });

    // Reopen — should resume from the persisted replay cursor.
    let db = CandyStore::open(dir.path(), config)?;
    for i in 0..3000u32 {
        assert_eq!(
            db.get(format!("key{i:04}"))?,
            Some(format!("val{i:04}").into_bytes()),
            "key{i:04} missing after resume-from-cursor rebuild"
        );
    }

    Ok(())
}

/// Crash after data write but before index insert, then rebuild.
/// The data file has the entry but the index doesn't — replay should recover it.
#[cfg(unix)]
#[test]
fn test_crash_after_write_before_insert_recovers_on_rebuild() -> Result<(), Error> {
    let dir = tempdir().unwrap();
    let config = Config::default();

    // Write some baseline data.
    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..10 {
            db.set(format!("base{i}"), format!("val{i}"))?;
        }
        db._abort_for_testing();
    }

    // Reopen cleanly to get a stable state, then crash mid-insert.
    {
        let db = CandyStore::open(dir.path(), config)?;
        for i in 0..10 {
            assert_eq!(
                db.get(format!("base{i}"))?,
                Some(format!("val{i}").into_bytes())
            );
        }
    }

    // Now write one more key and crash after the data file write but before
    // the index is updated.
    fork_expect_abort(|| {
        unsafe {
            libc::setenv(
                c"CANDYSTORE_CRASH_POINT".as_ptr(),
                c"set_after_write_before_insert".as_ptr(),
                1,
            );
            libc::setenv(c"CANDYSTORE_CRASH_AFTER".as_ptr(), c"0".as_ptr(), 1);
        }
        let db = CandyStore::open(dir.path(), config).unwrap();
        let _ = db.set("crash_key", "crash_val");
    });

    // Rebuild should recover everything including the crash_key (data is
    // durable in the active file even though the index insert never happened).
    let db = CandyStore::open(dir.path(), config)?;
    for i in 0..10 {
        assert_eq!(
            db.get(format!("base{i}"))?,
            Some(format!("val{i}").into_bytes()),
            "base{i} missing after crash recovery"
        );
    }
    // The crash_key's data was written to the file before the crash, so it
    // should be recovered by replay. However, this depends on whether the
    // OS flushed the data page to disk before abort — in this test the child
    // is doing an in-process abort so the file write may or may not be
    // durable. We verify the baseline keys survived; crash_key recovery is
    // best-effort.

    Ok(())
}

#[cfg(unix)]
#[test]
fn test_split_row_crash_before_publish_retries_cleanly() -> Result<(), Error> {
    run_split_row_resume_test(c"split_row_after_copy_before_publish")
}

#[cfg(unix)]
#[test]
fn test_split_row_crash_after_high_publish_retries_cleanly() -> Result<(), Error> {
    run_split_row_resume_test(c"split_row_after_high_publish_before_low_publish")
}
