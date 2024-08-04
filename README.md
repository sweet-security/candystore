# VickyStore
A pure rust implementation of a fast, persistent, in-process key-value store, that relies on a novel sharding 
mechanism. 

The algorithm is straight forward: 
* A key is hashed, producing 64 bits of hash. The most significant 16 bits are taken to be "shard selector", followed
  by 16 bits of "row selector", followed by 32 bits of "signature".
* The shard selector selects a shard, which maps to a file in a directory.
* At first, we have a shard that covers the range `[0..65535]`, so all shard selectors map to the same file.
* When the file grows too big, or contains too many keys, it undergoes a split operation, where the keys are 
  split into a bottom-half and a top-half: shard `[0..65535]` gets split into `[0..32767]` and `[32768..65535]`, and 
  the keys are divided according to their shard selector. This process repeats as needed.
* Inside a shard, we have a header table made of rows, each being an array of signatures. The row selector selects 
  the key's row, and within the row we use SIMD operations for matching the signature very quickly. This 
  part of the file is kept mmap'ed.
* Once we find the correct entry, we get its data offset in the file and read it. 
  
The default parameters (chosen by simulations) are shards with 64 rows, each with 512 entries. The chances 
of collisions with these parameters are minimal, and they allow for ~90% utilization of the shard, while
having relatively small header tables (32K entries, taking up 384KB). With the expected 90% utilization, it means
you should be able to hold 29K keys per shard.

The concept can be extended to a distributed database, by adding a layer of master-shards that select a 
server, followed by the normal sharding mechanism described above.

## Notes
* The file format is not yet stable

## Design Goals
* Fast and efficient
* Low memory footprint
* No heavy/unbounded merges
* No Write-Ahead Log (WAL) or journalling of any kind
* Splitting/compaction happens per-shard, so there's no global locking
* Suitable for both write-heavy/read-heavy workloads
* Concurrent by design (multiple threads getting/setting/removing keys at the same time)
* The backing store is taken to be an SSD, thus it's not optimized for HDDs

## Roadmap
* Add TTL to keys (easy, but will screw up accounting)
* Add key-prefixes which can be used to implement grouping of keys into "families", i.e. lightweight indexing
  by storing their parted-hash with an "anti collision" in a modifiable entry
* Distributed protocol based on file locks (meant to run on a shared network folder)

