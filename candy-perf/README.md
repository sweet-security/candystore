Performance results from my machine

* Ubuntu 24.04 LTS
* Lenovo ThinkPad X1 Carbon Gen 10 (12th Gen Intel® Core™ i7-1260P × 16)
* RAM: 32.0 GiB
* SSD: 512 GB
* Built with `cargo build --release`
* Running on a local filesystem

### Smallish entries (4 byte keys, 3 byte values)
```
1000000 small entries with pre-split
  Small entries insert: 1.347us
  Small entries get 100% existing: 0.477us
  Small entries get 50% existing: 0.474us
  Small entries removal: 0.493us
  Small entries mixed: 1.822us

1000000 small entries without pre-split
  Small entries insert: 4.151us
  Small entries get 100% existing: 0.517us
  Small entries get 50% existing: 0.515us
  Small entries removal: 0.535us
  Small entries mixed: 4.633us
```

### Largish entries (100 byte keys, 300 byte values)
```
500000 large entries with pre-split
  Large entries insert: 1.624us
  Large entries get 100% existing: 0.618us
  Large entries removal: 0.128us

500000 large entries without pre-split
  Large entries insert: 5.422us
  Large entries get 100% existing: 0.731us
  Large entries removal: 0.139us
```

### Lists
```
10 collections with 100000 items in each
  Inserts: 8.104us
  Updates: 2.593us
  Gets: 0.612us
  Iterations: 0.556us
  Removal of 50% items: 7.945us
  Discards: 0.972us
```

### Threads without contention (different keys)
```
No-contention: 10 threads accessing 100000 different keys - with pre-split
  Inserts: 3.238us
  Gets: 1.004us
  Removals: 0.929us

No-contention: 10 threads accessing 100000 different keys - without pre-split
  Inserts: 19.497us
  Gets: 1.119us
  Removals: 1.001us
```

### Threads with contention (same keys)
```
Contention: 10 threads accessing 1000000 same keys - with pre-split
  Inserts: 4.556us
  Gets: 1.204us
  Removals: 1.334us

Contention: 10 threads accessing 1000000 same keys - without pre-split
  Inserts: 12.167us
  Gets: 2.195us
  Removals: 2.257us
```
