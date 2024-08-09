## VickiStore Crasher

Fork a child process to insert 1M keys into the DB, while the parent kills it repeatedly. The test
makes sure the child is able to make progress as well as making sure the DB remains consistent.

Note: the store is not meant to be used by multiple processes concurrently -- it uses thread syncrhonization,
not inter-process synchronization. The test uses the store only from a single process at a time.


```
$ cargo run
child starting at 0
[0] killing child
child starting at 20445
[1] killing child
child starting at 31656
[2] killing child
child starting at 55500
.
.
.
child starting at 978418
[219] killing child
child starting at 982138
[220] killing child
child starting at 991255
child finished
child finished in 221 iterations
Parent starts validating the DB...
DB validated successfully
```
