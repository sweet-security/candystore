# candystore

`candystore` is an embedded persistent key-value store for Rust with:

- append-only data files
- a mutable in-place index for fast lookups
- rebuild-from-data-files recovery
- list and queue collection APIs
- typed wrappers built on `databuf`
- background compaction

The index is an acceleration structure. The data files are the durable event log that recovery replays when the index is dirty.

## Highlights

- Fast point lookups through a memory-mapped index
- Crash recovery by rebuilding from append-only data files
- Ordered list API keyed by `(list, item)`
- Queue and large-value APIs
- Thread-safe shared access through `Arc<CandyStore>`
- Typed APIs for encoded keys and values

## Quick Start

```rust
use candystore::{CandyStore, Config, Result};

fn main() -> Result<()> {
    let db = CandyStore::open("/tmp/candy-dir", Config::default())?;

    db.set("user:1", "alice")?;
    assert_eq!(db.get("user:1")?, Some(b"alice".to_vec()));

    let status = db.replace("user:1", "alice-v2", Some("alice"))?;
    assert!(status.was_replaced());

    db.remove("user:1")?;
    assert_eq!(db.get("user:1")?, None);
    Ok(())
}
```

## Collections

Lists are ordered maps scoped by a list key.

```rust
use candystore::{CandyStore, Config, Result};

fn main() -> Result<()> {
    let db = CandyStore::open("/tmp/candy-lists", Config::default())?;

    db.set_in_list("langs", "rust", "systems")?;
    db.set_in_list("langs", "python", "scripting")?;

    let items = db.iter_list("langs").collect::<Result<Vec<_>, _>>()?;
    assert_eq!(items.len(), 2);
    Ok(())
}
```

Queues store ordered values under a queue key.

```rust
use candystore::{CandyStore, Config, Result};

fn main() -> Result<()> {
    let db = CandyStore::open("/tmp/candy-queue", Config::default())?;

    db.push_to_queue_tail("jobs", "job-1")?;
    db.push_to_queue_tail("jobs", "job-2")?;

    assert_eq!(db.pop_queue_head("jobs")?, Some(b"job-1".to_vec()));
    Ok(())
}
```

## Typed API

Typed wrappers encode keys and values with `databuf` and separate each typed key space with a per-type id.

```rust
use std::sync::Arc;

use candystore::{CandyStore, CandyTypedStore, Config, Result};

fn main() -> Result<()> {
    let db = Arc::new(CandyStore::open("/tmp/candy-typed", Config::default())?);
    let users = CandyTypedStore::<String, Vec<u32>>::new(db);

    users.set("scores", &vec![1, 2, 3])?;
    assert_eq!(users.get("scores")?, Some(vec![1, 2, 3]));
    Ok(())
}
```

## Large Values

`set_big` / `get_big` / `remove_big` store values larger than the inline value limit by chunking them across queue-backed entries.

## Recovery Model

On open, the store marks the index dirty before doing work. On clean drop it syncs data files, clears the dirty flag, and flushes the index header.

If the store is reopened while dirty, behavior depends on `Config::rebuild_strategy`:

- `FailIfDirty`: reject open
- `RebuildIfDirty`: rebuild the index from data files
- `ResetDBIfDirty`: clear the directory and recreate an empty store
- `TrustDirtyIndexIfChecksumCorrectOrFail`: accept the dirty index only if row checksums match
- `TrustDirtyIndexIfChecksumCorrectOrRebuild`: trust valid checksums, otherwise rebuild
- `TrustDirtyIndexIfChecksumCorrectOrReset`: trust valid checksums, otherwise reset the database

## Operational Notes

- `Config::hash_key` is part of on-disk compatibility. A store must be reopened with the same hash key.
- Data files are append-only. Background compaction rewrites live entries into the active file and deletes old files.
- Rebuild now fails closed on unknown entry types and unknown namespaces in data files rather than silently skipping them.
- The index format and data-file format are internal implementation details until a `1.0` compatibility policy is explicitly documented.

## Examples

See:

- `examples/simple.rs`
- `examples/typed.rs`
- `examples/lists.rs`
- `examples/multithreaded.rs`

## Design

See `DESIGN.md` for the storage layout, locking model, rebuild path, and collection semantics.