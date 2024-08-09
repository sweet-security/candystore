Performance results from my machine

```
Ubuntu 24.04 LTS
Lenovo ThinkPad X1 Carbon Gen 10 (12th Gen Intel® Core™ i7-1260P × 16)
RAM: 32.0 GiB
SSD: 512 GB
```
* Built with `cargo build --release`
* Running on a local filesystem

```
1000000 small entries with pre-split
  Small entries insert: 1.388us
  Small entries get 100% existing: 0.486us
  Small entries get 50% existing: 0.483us
  Small entries removal: 0.514us
  Small entries mixed: 1.837us

1000000 small entries without pre-split
  Small entries insert: 4.332us
  Small entries get 100% existing: 0.524us
  Small entries get 50% existing: 0.527us
  Small entries removal: 0.543us
  Small entries mixed: 4.777us

500000 large entries with pre-split
  Large entries insert: 1.703us
  Large entries get 100% existing: 0.634us
  Large entries removal: 0.134us

500000 large entries without pre-split
  Large entries insert: 5.557us
  Large entries get 100% existing: 0.782us
  Large entries removal: 0.145us

10 collections with 100000 items in each
  Inserts: 8.356us
  Updates: 2.704us
  Gets: 0.632us
  Iterations: 0.576us
  Removal 50% of items: 4.192us
  Discards: 0.536us

10 threads accessing 100000 different keys - with pre-split
  Inserts: 3.283us
  Gets: 0.976us
  Removals: 0.886us

10 threads accessing 100000 different keys - without pre-split
  Inserts: 19.353us
  Gets: 1.027us
  Removals: 0.927us

10 threads accessing 1000000 same keys - with pre-split
  Inserts: 12.029us
  Gets: 2.333us
  Removals: 2.989us

10 threads accessing 1000000 same keys - without pre-split
  Inserts: 10.777us
  Gets: 2.586us
  Removals: 2.818us
```