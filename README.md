# CandyStore

A pure Rust implementation of a fast (*blazingly* :tm:, of course), persistent, in-process
key-value store that relies on a hash-based sharding algorithm. All operations — lookup,
insert, and removal — are O(1).

| Operation | Time*  |
|-----------|--------|
| Lookup    | < 1us  |
| Insert    | < 2us  |
| Removal   | < 1us  |

The algorithm can be thought of as a "zero-overhead" extension to a hash table stored over
files, designed to minimize IO operations. See [how to interpret the results\*](#how-to-interpret-the-performance-results).

## Overview


## How to Interpret the Performance Results

While the numbers above are incredible, it is obvious that any file-backed store will be
limited by the filesystem's latency and bandwidth. For example, you can expect a read
latency of 20-100us from SSDs (NVMe), so that's the lower bound on reading a random
location in the file.

What the numbers above measure is the performance of the *algorithm*, not the *storage*:
given you can spare an overhead of 0.6% mapped into memory, lookup/insert/removal require
a single disk IO. Replacing (updating) an existing element requires two IOs, since it needs
to compare the key before writing it anew. These IOs may return from the kernel's page
cache, in which case it's practically immediate, or from disk, in which case you can expect
it to take 1-2 round-trip times of your device.

Inserting to/removing from lists require 2-3 IOs, since these operations need to update
the list's head or tail, as well as a "chain" element. Such operations should really be done
with a "large enough page cache". Updating/fetching an existing element in a list is a
single IO as above.

If your memory is too constrained for keeping the lookup tables mapped-in (i.e., they get
evicted to disk), you'll incur one more unit of "IO latency" for fetching the row from the
table. Since the row spans 4KB, it should behave nicely with 4K IOs.

## Design

See `DESIGN.md` for the full storage layout, locking model, rebuild path, collection
semantics, and compaction strategy.