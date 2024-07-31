#![feature(btree_cursors)]
use std::{collections::BTreeMap, sync::atomic::AtomicUsize, time::Instant, u32};

use simd_itertools::PositionSimd;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PartedHash {
    shard_idx: u32,
    row_idx: u32,
    signature: u32,
}

impl PartedHash {
    fn new_random() -> Self {
        Self {
            shard_idx: rand::random(),
            row_idx: rand::random(),
            signature: rand::random(),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct ShardRow {
    entries: Vec<PartedHash>,
}

static TOTAL_COLLISIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct Shard {
    row_width: usize,
    total: usize,
    rows: Vec<ShardRow>,
}
impl Shard {
    fn new(num_rows: usize, row_width: usize) -> Self {
        Self {
            row_width,
            total: 0,
            rows: vec![ShardRow::default(); num_rows],
        }
    }
    fn add(&mut self, h: PartedHash) -> bool {
        let len = self.rows.len();
        let row = &mut self.rows[(h.row_idx as usize) % len];
        if row.entries.len() >= self.row_width {
            false
        } else {
            if row
                .entries
                .iter()
                .find(|h2| h2.signature == h.signature)
                .is_some()
            {
                TOTAL_COLLISIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            row.entries.push(h);
            self.total += 1;
            true
        }
    }
}

struct DB {
    num_rows: usize,
    row_width: usize,
    total: usize,
    num_splits: usize,
    fill_level_on_split: usize,
    fill_levels: Vec<f64>,
    shards: BTreeMap<u64, Shard>,
}
impl DB {
    fn new(num_rows: usize, row_width: usize) -> Self {
        let mut bt = BTreeMap::new();
        bt.insert(1 << 32, Shard::new(num_rows, row_width));
        Self {
            num_rows,
            row_width,
            total: 0,
            num_splits: 0,
            fill_level_on_split: 0,
            fill_levels: vec![],
            shards: bt,
        }
    }
    fn add(&mut self, to_add: PartedHash) {
        let (key_before, key_after) = {
            let shard_idx = to_add.shard_idx as u64;
            let mut cursor = self
                .shards
                .lower_bound_mut(std::ops::Bound::Excluded(&shard_idx));
            let key_before = cursor.peek_prev().map(|(k, _)| *k).unwrap_or(0);
            let Some((key_after, shard)) = cursor.peek_next() else {
                panic!("no key_after for 0x{:x}", to_add.shard_idx);
            };

            if shard.add(to_add) {
                self.total += 1;
                return;
            }
            (key_before, *key_after)
        };

        let prev_shard = self.shards.remove(&key_after).unwrap();
        let midpoint = (key_before / 2) + (key_after / 2);
        self.shards
            .insert(midpoint, Shard::new(self.num_rows, self.row_width));
        self.shards
            .insert(key_after, Shard::new(self.num_rows, self.row_width));

        self.num_splits += 1;
        self.fill_level_on_split += prev_shard.total;

        /*println!(
            "split ({:3}) 0x{key_before:08x}..0x{midpoint:08x}..0x{key_after:09x}  [total: {:8}, shard avg fill: {:.4}, shard size: {}]",
            self.num_splits,
            self.total,
            ((self.fill_level_on_split as f64) / (self.num_splits as f64))
                / ((self.num_rows * self.row_width) as f64),
            self.num_rows * self.row_width
        );*/
        self.fill_levels.push(
            ((self.fill_level_on_split as f64) / (self.num_splits as f64))
                / ((self.num_rows * self.row_width) as f64),
        );
        self.total -= prev_shard.total;

        for row in prev_shard.rows.iter() {
            for h in row.entries.iter() {
                self.add(*h);
            }
        }
        self.add(to_add);
    }
}

fn main() {
    for rows in [32, 64, 128, 256] {
        for width in [32, 64, 128, 256, 512, 1024] {
            let mut db = DB::new(rows, width);
            let mut added = 0;
            TOTAL_COLLISIONS.store(0, std::sync::atomic::Ordering::SeqCst);
            for _ in 0..100 {
                for _ in 0..db.num_rows * db.row_width {
                    db.add(PartedHash::new_random());
                    added += 1;
                }
            }

            let mut summed = 0;
            for (_, sh) in db.shards.iter() {
                summed += sh.total;
            }

            let mut summed_last_fills = 0.0;
            for lf in db.fill_levels.iter() {
                summed_last_fills += lf;
            }

            assert_eq!(db.total, summed);
            assert_eq!(db.total, added);
            let avg = summed_last_fills / (db.fill_levels.len() as f64);
            let sz = (db.num_rows * db.row_width * 12) / 1024;
            println!(
                "r={rows:4} w={width:4} avg={:.6} elems={:7} sz={:4}KB collisions={} collisions-probability={:.015} {} {}",
                avg,
                db.num_rows * db.row_width,
                sz,
                TOTAL_COLLISIONS.load(std::sync::atomic::Ordering::SeqCst),
                1.0 - (-(width as f64) * (width as f64 - 1.0) / ((1u64 << 33) as f64)).exp(),
                if avg > 0.8 {"GOOD"} else {""},
                if sz > 800 {"BIG"} else {""},
            );
        }
    }

    let reps = 10_000_000usize;
    for width in [32, 64, 128, 256, 512, 1024] {
        let mut v = vec![0u32; width];
        for i in 0..width {
            v[i] = i as u32;
        }
        v[width - 1] = 80808080;
        assert_eq!(v.iter().position_simd(80808080), Some(width - 1));
        assert_eq!(v.iter().position_simd(80808081), None);
        let mut pos: usize = 0;

        let t0 = Instant::now();
        for _ in 0..reps {
            pos += v.iter().position_simd(80808080).unwrap_or(0);
            pos += v.iter().position_simd(80808081).unwrap_or(0);
        }

        println!(
            "width={width:4} time per simd={:4}ns",
            Instant::now().duration_since(t0).as_nanos() as usize / reps,
        );

        assert_eq!(pos, (width - 1) * reps);
    }

    let reps = 10_000_000usize;
    for width in [32, 64, 128, 256, 512, 1024] {
        let mut v = vec![0u32; width];
        for i in 0..width {
            v[i] = i as u32;
        }
        v[width - 1] = 80808080;
        assert_eq!(v.iter().position_simd(80808080), Some(width - 1));
        assert_eq!(v.iter().position_simd(80808081), None);
        let mut pos: usize = 0;

        let t0 = Instant::now();
        for _ in 0..reps {
            pos += v.iter().position(|x| *x == 80808080).unwrap_or(0);
            pos += v.iter().position(|x| *x == 80808081).unwrap_or(0);
        }

        println!(
            "width={width:4} time per non-simd={:4}ns",
            Instant::now().duration_since(t0).as_nanos() as usize / reps,
        );

        assert_eq!(pos, (width - 1) * reps);
    }
}
