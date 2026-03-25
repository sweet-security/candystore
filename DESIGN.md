# Design

This document describes the storage model, concurrency strategy, recovery rules, and collection semantics used by `candystore`.

## Goals

- Fast point lookups for embedded workloads
- Durable append-only writes to data files
- Recoverability by replaying data files when the index is dirty
- Support for ordered collections and large values without a separate database layer
- Simple operational model for single-process ownership with multi-threaded access

## High-Level Architecture

The store has two durable layers:

1. `data_XXXX` files: append-only log-structured storage
2. `index` + `rows`: mutable lookup structure used for fast reads

ASCII overview:

```text
                   +-------------------+
 set/get/remove -> |   CandyStore API  |
                   +---------+---------+
                             |
                             v
                  +----------------------+
                  | in-memory coordination|
                  | row locks + counters  |
                  +-----+------------+----+
                        |            |
                        |            |
                        v            v
                 +-----------+   +----------------+
                 | index/rows |   | data_0000..N  |
                 | mmap index |   | append-only   |
                 +-----------+   +----------------+
                        ^            |
                        |            |
                        +------------+
                          rebuild / compaction
```

The important design choice is that the index is rebuildable. The data files are the durable source of truth for recovery.

## Index Layout

The index is stored in two files:

- `index`: header, counters, global split level, dirty flag, hash key, waste accounting
- `rows`: fixed-size hash rows stored in page-sized records

Each row contains:

- a split level
- a checksum
- fixed-width signature slots
- fixed-width entry pointers

ASCII row model:

```text
+--------------------------------------------------------------+
| split_level | checksum | signatures[ROW_WIDTH] | pointers[] |
+--------------------------------------------------------------+
```

Pointers are compact and encode:

- data file index
- aligned file offset
- size hint
- masked row selector bits

The index is optimized for lookup speed, not for being the primary source of truth.

## Data Files

Each data file starts with a fixed header page containing:

- file signature
- file format version
- file ordinal

After the header page, entries are appended at 16-byte alignment.

Two entry kinds exist today:

- `Data`: key + value
- `Tombstone`: key only

ASCII entry layout:

```text
Data entry
----------
u32 header
u16 key_len
u16 value_len
value bytes
key bytes
u16 checksum
padding to 16-byte alignment

Tombstone entry
---------------
u32 header
u16 key_len
key bytes
u16 checksum
padding to 16-byte alignment
```

The `header` packs:

- an entry-offset-derived magic value
- namespace bits
- entry type bits

Checksums cover the logical entry bytes before alignment padding.

## Namespaces

Namespaces partition the key space inside the same physical store:

- user KV entries
- queue metadata and queue data
- list metadata, list index, and list data
- large-value metadata and chunks
- typed variants of the above

This lets all features share the same physical storage while keeping their internal keys distinct.

## Write Path

Normal writes are append-and-swing-pointer operations.

ASCII write flow:

```text
client write
   |
   v
hash key -> lock logical shard -> find existing row slot
   |
   +--> append new entry to active data file
   |
   +--> update row pointer in index
   |
   +--> account old entry as waste when replacing/removing
```

Important consequences:

- updates never overwrite old data in place
- remove operations append tombstones
- old versions remain in old data files until compaction removes them

## Recovery Path

On open, the store marks the index dirty immediately. This makes an interrupted open conservative.

If the previous shutdown was unclean and the configured strategy rebuilds, recovery does:

```text
reset index state
sort data files by ordinal
for each data file in order:
    scan aligned entries
    validate checksum and entry shape
    replay data/tombstone into the index
```

Recovery invariants:

- later entries win over earlier ones
- tombstones remove prior live values
- file order is determined only by file ordinal
- duplicate data-file ordinals are invalid because they make replay order ambiguous
- unknown entry types and unknown namespaces are treated as invalid data and fail rebuild
- recovered entries are validated against current key/value size limits before indexing

This is the key reason data files can act as the long-term ground truth for the current format.

## Compaction

Compaction rewrites live entries from an old file into the active file, then deletes the old file.

ASCII compaction flow:

```text
old data file
    |
    v
scan entries in order
    |
    +--> if entry is still the current live version:
    |        append to active file
    |        replace pointer
    |
    +--> otherwise skip
    |
    v
delete compacted file
```

Compaction is rate-limited by a token-bucket pacer using `compaction_throughput_bytes_per_sec`.

## Locking Model

There are two main locking layers:

1. per-row / per-shard locking around index mutation
2. logical key locks to serialize conflicting higher-level operations

ASCII concurrency view:

```text
thread A            thread B
   |                   |
   +--> logical lock ---+
            |
            v
       row/shard lock
            |
            v
        mutate row
```

This design allows unrelated keys to proceed concurrently while keeping conflicting operations consistent.

The store also uses an on-disk `.lockfile` so only one process owns a store directory at a time.

## Collection Semantics

### Lists

Lists are ordered maps keyed by `(list_key, item_key)`.

- each list has metadata with `head`, `tail`, and `count`
- list order is stored through an index from logical position to item key
- item data stores the user value plus the logical index suffix
- updates can preserve position or promote the item to the tail, depending on the API
- retain/compaction can rewrite sparse lists into compact spans

### Queues

Queues are ordered sequences under a queue key.

- each queue has metadata with `head`, `tail`, and `count`
- queue entries are stored by synthetic logical index
- head/tail peeks and pops skip holes caused by removals
- `queue_range` exposes the current logical span, not a dense ordinal count

### Large Values

Large values use queue-backed chunk storage.

- metadata records identify the chunk queue
- data is split into chunk entries
- reads concatenate chunks in queue order

## Typed API Design

Typed wrappers are thin adapters over the untyped APIs.

- keys and values are encoded with `databuf`
- type-specific IDs are appended to typed root keys
- typed collections reuse the same lower-level storage semantics

This means typed and untyped APIs share the same durability and recovery model.

## Dirty Shutdown Semantics

Clean shutdown requires:

1. background compaction thread stopped
2. data files synced
3. index header dirty flag cleared and flushed

If any of those steps do not happen, the next open treats the index as dirty.

## Format and Compatibility Notes

Current compatibility assumptions:

- the index stores the effective hash key, and reopen reuses the persisted key for existing stores
- data-file entry types and namespaces are intentionally strict during rebuild
- the current code does not define a stable cross-version migration policy yet

For a future `1.0`, the minimum compatibility policy should define:

- whether old data-file versions remain readable
- whether hash keys are user-managed forever or migrated differently
- what entry types and namespaces are reserved for future expansion
- how recovery should behave when a newer writer introduces unknown on-disk constructs

## Why the Data Files Are the Source of Truth

The index can be reset and replayed from the data files.

That is only true if the data files remain:

- append-only
- checksummed
- strictly parseable
- replayable in deterministic order

The recent hardening work in this repository specifically enforces that rebuild fails closed on unknown entry metadata instead of silently discarding it.

## Practical Limits

- maximum user key length: `MAX_USER_KEY_SIZE`
- maximum inline value length: `MAX_USER_VALUE_SIZE`
- large values use chunked storage instead of a single inline entry
- maximum data-file size is bounded by pointer encoding limits

## Suggested Future 1.0 Checklist

- write a formal on-disk format spec for data files
- define the compatibility promise for `DATA_FILE_VERSION`
- decide whether hash-key compatibility remains config-managed
- add targeted corruption tests for truncated, unknown-type, and unknown-namespace entries
- document operational upgrade expectations explicitly