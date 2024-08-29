use std::{
    fmt::Display,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;

use crate::shard::HEADER_SIZE;

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub num_shards: usize,
    pub num_splits: usize,
    pub num_compactions: usize,
    pub last_split_stats: Vec<(Duration, u64, u64)>,
    pub last_compaction_stats: Vec<(Duration, u64, u64)>,

    pub occupied_bytes: usize,
    pub wasted_bytes: usize,

    pub num_inserts: usize,
    pub num_updates: usize,
    pub num_positive_lookups: usize,
    pub num_negative_lookups: usize,
    pub num_removals: usize,
    pub num_collisions: usize,

    pub num_read_ops: usize,
    pub num_read_bytes: usize,
    pub num_write_ops: usize,
    pub num_write_bytes: usize,

    pub entries_under_128: usize,
    pub entries_under_1k: usize,
    pub entries_under_8k: usize,
    pub entries_under_32k: usize,
    pub entries_over_32k: usize,
}

impl Stats {
    pub const FILE_HEADER_SIZE: usize = HEADER_SIZE as usize;

    pub fn data_bytes(&self) -> usize {
        self.occupied_bytes - self.wasted_bytes
    }
    pub fn total_occupied_bytes(&self) -> usize {
        self.num_shards * Self::FILE_HEADER_SIZE + self.occupied_bytes
    }
    pub fn num_entries(&self) -> usize {
        self.num_inserts - self.num_removals
    }
    pub fn average_entry_size(&self) -> usize {
        self.data_bytes()
            .checked_div(self.num_entries())
            .unwrap_or(0)
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, 
            "shards={} [splits={} compacts={}] [occupied={} wasted={}] [ins={} updt={} +lkup={} -lkup={} rem={} coll={}] reads=[{}, {}b] writes=[{}, {}b]",
            self.num_shards, self.num_splits, self.num_compactions, self.occupied_bytes, self.wasted_bytes, 
            self.num_inserts, self.num_updates, self.num_positive_lookups, self.num_negative_lookups, 
            self.num_removals, self.num_collisions, self.num_read_ops, self.num_read_bytes, self.num_write_ops, 
            self.num_write_bytes)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CyclicArr<T, const N: usize> {
    idx: usize,
    arr: [T; N],
}
impl<T: Default + Copy, const N: usize> Default for CyclicArr<T, N> {
    fn default() -> Self {
        Self {
            idx: 0,
            arr: [T::default(); N],
        }
    }
}
impl<T: Default + Clone + Copy, const N: usize> CyclicArr<T, N> {
    pub(crate) fn push(&mut self, val: T) {
        self.arr[self.idx % N] = val;
        self.idx += 1;
    }
    pub(crate) fn clear(&mut self) {
        self.idx = 0;
        for i in 0..N {
            self.arr[i] = T::default();
        }
    }
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a T> {
        (self.idx.checked_sub(N).unwrap_or(0)..self.idx).map(|idx| &self.arr[idx % N])
    }
}

#[test]
fn test_cyclic_arr() {
    let mut arr = CyclicArr::<u32, 8>::default();
    assert!(arr.iter().collect::<Vec<_>>().is_empty());
    arr.push(1);
    arr.push(2);
    arr.push(3);
    assert_eq!(arr.iter().collect::<Vec<_>>(), vec![&1,&2,&3]);
    arr.push(4);
    arr.push(5);
    arr.push(6);
    arr.push(7);
    arr.push(8);
    assert_eq!(arr.iter().collect::<Vec<_>>(), vec![&1,&2,&3,&4,&5,&6,&7,&8]);
    arr.push(9);
    arr.push(10);
    arr.push(11);
    assert_eq!(arr.iter().collect::<Vec<_>>(), vec![&4,&5,&6,&7,&8,&9,&10,&11]);
    arr.clear();
    arr.push(12);
    arr.push(13);
    arr.push(14);
    assert_eq!(arr.iter().collect::<Vec<_>>(), vec![&12,&13,&14]);
    for i in 15u32..1000 {
        arr.push(i);
    }
    assert_eq!(arr.iter().collect::<Vec<_>>(), vec![&992,&993,&994,&995,&996,&997,&998,&999]);
}

#[derive(Debug, Default)]
pub struct InternalStats {
    pub(crate) num_splits: AtomicUsize,
    pub(crate) num_compactions: AtomicUsize,
    pub(crate) last_compaction_stats: Mutex<CyclicArr<(Duration, u64, u64), 8>>,
    pub(crate) last_split_stats: Mutex<CyclicArr<(Duration, u64, u64), 8>>,

    pub(crate) num_inserts: AtomicUsize,
    pub(crate) num_updates: AtomicUsize,
    pub(crate) num_positive_lookups: AtomicUsize,
    pub(crate) num_negative_lookups: AtomicUsize,
    pub(crate) num_removals: AtomicUsize,
    pub(crate) num_collisions: AtomicUsize,

    pub(crate) num_read_ops: AtomicUsize,
    pub(crate) num_read_bytes: AtomicUsize,
    pub(crate) num_write_ops: AtomicUsize,
    pub(crate) num_write_bytes: AtomicUsize,

    pub(crate) entries_under_128: AtomicUsize,
    pub(crate) entries_under_1k: AtomicUsize,
    pub(crate) entries_under_8k: AtomicUsize,
    pub(crate) entries_under_32k: AtomicUsize,
    pub(crate) entries_over_32k: AtomicUsize,
}

impl InternalStats {
    pub(crate) fn add_entry(&self, sz: usize) {
        self.num_write_bytes.fetch_add(sz, Ordering::Relaxed);
        self.num_write_ops.fetch_add(1, Ordering::Relaxed);
        match sz {
            0..128 => self.entries_under_128.fetch_add(1, Ordering::Relaxed),
            128..1024 => self.entries_under_1k.fetch_add(1, Ordering::Relaxed),
            1024..8192 => self.entries_under_8k.fetch_add(1, Ordering::Relaxed),
            8192..32768 => self.entries_under_32k.fetch_add(1, Ordering::Relaxed),
            _ => self.entries_over_32k.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub(crate) fn report_split(&self, t0: Instant, bottom_size: u64, top_size: u64) {
        let dur = Instant::now().duration_since(t0);
        self.num_splits.fetch_add(1, Ordering::Relaxed);
        self.last_split_stats
            .lock()
            .push((dur, bottom_size, top_size));
    }

    pub(crate) fn report_compaction(&self, t0: Instant, prev_size: u64, new_size: u64) {
        let dur = Instant::now().duration_since(t0);
        self.num_compactions.fetch_add(1, Ordering::Relaxed);
        self.last_compaction_stats
            .lock()
            .push((dur, prev_size, new_size));
    }

    pub(crate) fn clear(&self) {
        // store 0 in every stats...

        self.num_splits.store(0, Ordering::SeqCst);
        self.num_compactions.store(0, Ordering::SeqCst);
        self.last_split_stats.lock().clear();
        self.last_compaction_stats.lock().clear();

        self.num_inserts.store(0, Ordering::SeqCst);
        self.num_updates.store(0, Ordering::SeqCst);
        self.num_positive_lookups.store(0, Ordering::SeqCst);
        self.num_negative_lookups.store(0, Ordering::SeqCst);
        self.num_removals.store(0, Ordering::SeqCst);
        self.num_collisions.store(0, Ordering::SeqCst);

        self.num_read_ops.store(0, Ordering::SeqCst);
        self.num_read_bytes.store(0, Ordering::SeqCst);
        self.num_write_ops.store(0, Ordering::SeqCst);
        self.num_write_bytes.store(0, Ordering::SeqCst);

        self.entries_under_128.store(0, Ordering::SeqCst);
        self.entries_under_1k.store(0, Ordering::SeqCst);
        self.entries_under_8k.store(0, Ordering::SeqCst);
        self.entries_under_32k.store(0, Ordering::SeqCst);
        self.entries_over_32k.store(0, Ordering::SeqCst);
    }

    pub(crate) fn fill_stats(&self, stats: &mut Stats) {
        stats.num_splits = self.num_splits.load(Ordering::Relaxed);
        stats.num_compactions = self.num_compactions.load(Ordering::Relaxed);

        {
            let mut guard = self.last_split_stats.lock();
            stats.last_split_stats = guard.iter().copied().collect::<Vec<_>>();
            guard.clear();
        }
        {
            let mut guard = self.last_compaction_stats.lock();
            stats.last_compaction_stats = guard.iter().copied().collect::<Vec<_>>();
            guard.clear();
        }

        stats.num_inserts = self.num_inserts.load(Ordering::Relaxed);
        stats.num_updates = self.num_updates.load(Ordering::Relaxed);
        stats.num_positive_lookups = self.num_positive_lookups.load(Ordering::Relaxed);
        stats.num_negative_lookups = self.num_negative_lookups.load(Ordering::Relaxed);
        stats.num_removals = self.num_removals.load(Ordering::Relaxed);
        stats.num_collisions = self.num_collisions.load(Ordering::Relaxed);

        stats.num_read_ops = self.num_read_ops.load(Ordering::Relaxed);
        stats.num_read_bytes = self.num_read_bytes.load(Ordering::Relaxed);
        stats.num_write_ops = self.num_write_ops.load(Ordering::Relaxed);
        stats.num_write_bytes = self.num_write_bytes.load(Ordering::Relaxed);

        stats.entries_under_128 = self.entries_under_128.load(Ordering::Relaxed);
        stats.entries_under_1k = self.entries_under_1k.load(Ordering::Relaxed);
        stats.entries_under_8k = self.entries_under_8k.load(Ordering::Relaxed);
        stats.entries_under_32k = self.entries_under_32k.load(Ordering::Relaxed);
        stats.entries_over_32k = self.entries_over_32k.load(Ordering::Relaxed);
    }
}
