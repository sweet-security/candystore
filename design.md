# CandyStore V2

CandyStore V2 is a persistent, consistent, O(1) Key-Value store modeled as a hash-table-on-file. 

CandyStore consists of an index file and multiple data files, which live in a single filesystem directory. The index file is mmap'ed and the data files
are append-only and serve both as the data storage and as a journal; the index file can always be 
reconstructed from the data files. We trust the operating system to flush the mmap properly on exit, but we cannot
rely on this in case of power failure, kernel panic, etc. In such cases we will reconstruct the index from the data
files.

Another change from CandyStore v1 is being truly O(1). V1 used a tree of shards, and walking this tree was 
naturally `O(log(N))`. It also brought on several complications in the implementation (locking, splitting, 
compacting, etc.) that V2 strives to solve.

## Two-Dimensional Hash

We use a strong hash function such as `SipHash` to produce two 32 bit hashes:
* Row selector (`u32`)
* Signature (`u32`); must never be zero

These hash values will help us navigate the persistent hash table. We can think of them as an Cartesian coordinates:
the row selector indexes a row, and the signature selects an entry in that row. Due to the nature of hashing, it's
possible that we have collisions, so the same signature may appear more than once in a given row.

# Index File

The index file starts with a 4096 header, followed by the rows table. This table is always a power-of-two in size,
but may contain "holes", e.g., unpopulated rows. On the one hand, assuming a good hash function, we can expect 
rows will quickly "even out". However, allowing the table to have unpopulated holes means we can grow the 
table without a stop-the-world operation to redistribute everything. 


## Row Layout

Rows are 2 pages (8192 bytes) with the following structure:

| Offset | Field      | Type                            | Size |
|--------|------------|---------------------------------|------|
| 0      | Split      | AtomicU64 (only lowest u8 used) | 8    |
| 8      | Reserved   | -                               | 120  |
| 128    | Signatures | array of 576 u32                | 2304 |
| 2432   | Pointers   | array of 576 file pointers      | 5760 |
| 8192   |            |                                 |      |

The file pointers are 10-bytes long, and have the following structure:

```
     63                                                           0
     +--------------------+---------------+-----------------------+
     | Row selector bits  |  Entry size*  |       File offset     |
     |        (24)        |      (8)      |         (32)          |
     +------------+-------+---------------+-----------------------+

    16           0
    +------------+
    | File index |
    |    (16)    |
    +------------+

(*) entry size in multiples of 512 bytes, rounded up
```

As rows can hold up to 576 (64*9) entries, the overhead per entry is 14.2 bytes (8192/576).
The probability of collisions (according to the birthday paradox) is `(576^2)/(2*2^32)`, or ~1:26000. This means
the table will point us to the right location with 99.996% accuracy, making this algorithm is O(1)
in terms of disk IO.

The file-pointer size limits enforce some theoretical limits:
* Up to 2^24 rows (so only 24 bits from the row selector are viable)
* Data file sizes is limited to 4GB (32-bit file offset), but in practice it's better to keep it lower.
* Keys are limited to 16KB
* Values are limited to 64KB

## Row Table

The index is a table of rows. Each row holds (up to) 255 items and starts with a control word. The table starts
with split level 1, which means we use a mask of `(1<<split)-1` when taking the row selector from the hash.
In this case the mask is just `0x1` which means we use the LSB only to decide between rows 0 or 1 (the table always
holds `(1<<split)` rows). A split of 0 is invalid.

Initially, we have

|Row Idx | Split | Signatures   | Pointers |
|--------|-------|--------------|----------|
|    0   |   1   | ...          | ...      |
|    1   |   1   | ...          | ...      |
 
Now suppose row index 1 fils up, i.e., all the signature slots are full, and we need to insert another item whose 
`row_selector & split_mask = 1`. This means we have to split the row. We increment the global split level to 2, which
means we now consider two bits of the row selector (`split_mask = 0x3`). We then go over all the items in row index 1,
inspect their row selectors and then distribute them according to `row_selector & split_mask`. 
We know the LSB is always 1 (because that's who we got 
the items in row index 1 in the first place), and the second bit could be either 0 or 1, which means we now distribute 
the items to row index 1 (0b01) or 3 (0b11). Assuming a good hash function, we assume half of the keys will move 
to row 3. Theoretically we'll need to repeat this process if it so happens that all keys have the same second bit.

The table now looks like this

|Row Idx | Split | Signatures   | Pointers |
|--------|-------|--------------|----------|
|    0   |   1   | ...          | ...      |
|    1   |   2   | ...          | ...      |
|    2   |   0   | ...          | ...      |
|    3   |   2   | ...          | ...      |

Now let's consider lookup: we take a key and compute the hash, splitting it into a row selector and a signature.
We use the global split level, 2, thus taking the 2 LSBs. There are 3 options:

* Suppose we got 3 (0b11), so we go to row 3 and see that it matches the split level we used, so we know we've 
  found the correct row. We proceed to perform a SIMD lookup of the signature.
* Suppose the row index we got was 0 (0b00). We'll go to row 0 and see it has a split level less that what we 
  computed with. This is okay, because it means the row hasn't split yet, thus it contains all the keys whose
  LSB is 0. We know our row_selector has an MSB of 0, thus we can safely look it up in this row.
* Suppose the row index we got was 2 (0b10). We'll go to row 2 and see it has an invalid split (0). This means
  we've reached a "future row", so we take the global split, decrement 1, compute a new mask and use the correct row.
  The new mask in this case will be 0x1 and `2 & 0x1 = 0`, so we'll end up in row 0 and proceed from there. 
  Theoretically this is a `O(global_split_level)` operation, but in practice, assuming a good hash function,
  we will rarely have an imbalance of more than +/- 1, so in the average case this is an O(1) operation.

## Row Locking

Operations on rows use a `RWLock`; get locks for `read` while `set`/`remove` lock for `write`. We lock the 
row index (after masking), using an array of RWLocks of some reasonable size (e.g., 64 locks). Note that this 
locks the "physical row", which is important because different logical row selectors can end up in the same 
physical row due to masking.

When a `set` operation needs to split a row (holding a lock on its index), it does not need to hold a lock for 
to-be-created row as well, since any operation that would reach that row would bail (seeing a split of 0 and trying 
with a lower split level, which will surely hit the row being held by the currently-splitting `set` operation).
This simplifies our reasoning as to proving we don't have deadlocks: each operation holds a single lock.

However, there is a race condition where a reader waits on a row lock while a writer splits that row. By the time
the reader acquires the lock, the key it was looking for might have moved to the new row. Therefore, after acquiring
a row lock, a thread **must** re-validate that the row is still the correct row for the requested key (i.e., check 
`row_selector & row_mask == row_index`). If the row's depth has increased such that the key belongs elsewhere, 
the thread must release the lock and retry the lookup from the top.

## Table Expansion

Once we grow the global split level, we need to expand the file (truncate up) and double the mmap. We require 
holding a RWLock on the mmap for that, so expansion will not take place while operations use the existing mmap
(invalidating pointers).

To avoid deadlocks, if a thread holding a row lock determines that expansion is needed, it cannot simply wait for 
the mmap write lock (as other readers might hold the mmap read lock and wait for the row lock). Instead, the thread 
must release the row lock and the mmap read lock, acquire the mmap write lock, perform the expansion, and then 
restart the operation.

## Table Shrinking

If many (over 75%) items were removed, it may be beneficial to merge rows and shrink the table.
This is a simple process, but may involve a complex locking order. Also, it would only make sense in a 
global context, e.g., merging the bottom half of the rows with the top half of the rows, so memory can be
released to the operating system, which makes it a stop-the-world operation. Therefore, shrinking is an
explicit operation which locks the mmap, merges all row pairs, and resizes the mmap and the underlying file.

# Data Files

Data files start with a 4096 header, followed by entries. The header contains a 64-bit monotonic serial number for ordering and a "checkpointed offset" indicating how much data has been safely flushed. The data files are append-only, and immutable once rotated.
They have a maximum theoretical size of 4GB due to file offsets being stored as 32 bits, and we can have up to 64K
such files (again, file pointer limitation). They are numbered 0..N, and they need not be contiguous. For example,
it's possible we have `data.00008` and `data.00010` but `data.00009` has been removed.

However, for practical reasons, it's best to limit data files to a few hundred MBs, say 128MB.

## Entry Layout

An entry in the file is made of:
* Entry header (`u32`)
* Key bytes
* Value bytes
* Checksum (`u32`): CRC32 of (Entry Header + Key + Value)

The entry header is structured like so:
```
     31                                0
     +-------+---------+---------------+
     | Type  | Key Len |    Val Len    |
     |  (2)  |  (14)   |      (16)     |
     +-------+---------+---------------+

Type:
* 0 - Set
* 1 - Tombstone (removed)
* 2 - Reserved
* 3 - Reserved
```

The overhead of an entry in the data file is thus 8 bytes, bringing the total overhead to ~22 bytes.

## Rotation

Once the active data file (most recent one) reaches a configurable size, a new data file will be created
and become the new active data file; the previous one is then "fully flushed" (headers updated) and becomes
immutable.

## Compaction

Once a file is rotated, compaction may kick in. Each file maintains a "waste level", and if that level crosses
some threshold (say, over 30%), we would compact the file. This is a background process that simply reads all 
entries from the file, validates if they are up-to-date with the index, and if they are, copies them to the active
file simply by re-setting them in the key-value store.

It locks the key during that operation (which locks the physical row in turn), so we are sure no modifications 
could race with our compaction. Once compaction has finished going over all the keys in the file it will delete the
file since we can be sure this file ID is no longer being pointed to in the index.

Note that concurrent readers might still hold references to the old file (having resolved the file ID before 
compaction finished). To handle this safely, the implementation must use reference counting (e.g., `Arc<DataFile>`) 
for the file map. Compaction removes the file from the active map (preventing new readers), but the physical file 
deletion is deferred until all current readers holding references to that file have dropped them.

## Recovery

Upon crashing, it's possible we've lost some unflushed entries in the active data file. The data file's header
contains a "checkpointed offset", up to which all data has surely been flushed (the system flushes every few seconds). 
The recovery process starts at this offset and reads the remaining entries. We validate each entry using its CRC32 checksum.
Once we find an entry with a checksum mismatch, we stop and truncate the file to the last valid offset.

Note that several concurrent writes may have been taking place when we crashed, so it's possible we lose 
multiple entries. This may happen even on "regular process crash", e.g., the process was writing a large value
and hit a SEGFAULT or OOM and only part of the data was sent to the kernel. The kernel will surely flush the data,
but it means data files can have "broken tails" even without a kernel panic/power failure.

The rows table is only updated once the write was ack'ed by the kernel, meaning it will always reflect the most
up-to-date state, so long as we didn't have a kernel panic or a power failure. However, it is possible for the OS 
to flush the memory-mapped index page (containing a new pointer) to disk *before* flushing the appended data in 
the data file. On restart, this could result in the index pointing to valid offsets that contain garbage.
To mitigate this, the `get` path must be robust: if it follows a pointer and finds a signature mismatch or invalid 
entry header, it must treat this as a "Not Found" (or a specific "Corruption" error) rather than panicking or 
returning bad data.

This means we would rarely have to rebuild the rows table. The index file holds an "aggregated checksum" of all keys seen so far (using XOR, 
so order doesn't matter), and so do all of the data files. When the KV is opened, we go over all data files and 
aggregate their checksum, followed by aggregating the checksums of the active file's tail (until an valid entry is seen). 
If this aggregated checksum matches the one held by the index file, we don't need any futher recovery. 

If they differ, however, we need to rebuild the index. This requires scanning all data files in order of their 
64-bit serial numbers (stored in the file header). We replay all entries to reconstruct the in-memory index state.
Since we process files in serial order, Tombstones correctly overwrite previous Sets, ensuring the final state is consistent.

