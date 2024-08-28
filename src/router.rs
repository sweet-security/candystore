use anyhow::ensure;
use parking_lot::RwLock;
use std::time::Instant;
use std::{ops::Range, sync::Arc};

use crate::shard::{InsertMode, InsertStatus, Shard};
use crate::stats::InternalStats;
use crate::Result;
use crate::{hashing::PartedHash, store::InternalConfig};

fn consolidate_ranges(mut ranges: Vec<Range<u32>>) -> (Vec<Range<u32>>, Vec<Range<u32>>) {
    // we may encounter unfinished splits, where we have any combination of the bottom half, top half and
    // original shard existing. in this case, we want to keep the largest of them, e.g, suppose we find
    // [0..16, 0..32], we want to remove 0..16 and keep only 0..32. to do that, we sort by `start`
    // followed by sorting by end, so [0..16, 16..32, 0..32] is sorted as [0..32, 0..16, 16..32], which means
    // we'll encounter all over-arching ranges before smaller ones
    ranges.sort_by(|a, b| {
        if a.start == b.start {
            b.end.cmp(&a.end)
        } else {
            a.start.cmp(&b.start)
        }
    });

    let mut removed = vec![];
    let mut i = 1;
    while i < ranges.len() {
        if ranges[i].start >= ranges[i - 1].start && ranges[i].end <= ranges[i - 1].end {
            removed.push(ranges.remove(i));
        } else {
            i += 1;
        }
    }
    (ranges, removed)
}

#[test]
fn test_consolidate_ranges() {
    assert_eq!(consolidate_ranges(vec![0..16]), (vec![0..16], vec![]));
    assert_eq!(
        consolidate_ranges(vec![16..32, 0..16]),
        (vec![0..16, 16..32], vec![])
    );
    assert_eq!(
        consolidate_ranges(vec![16..32, 0..16, 0..32]),
        (vec![0..32], vec![0..16, 16..32])
    );
    assert_eq!(
        consolidate_ranges(vec![16..32, 0..16, 0..32, 48..64, 32..48, 50..60]),
        (vec![0..32, 32..48, 48..64], vec![0..16, 16..32, 50..60])
    );
}

enum ShardNode {
    Leaf(Shard),
    Vertex(Arc<ShardRouter>, Arc<ShardRouter>),
}

impl ShardNode {
    fn span(&self) -> Range<u32> {
        match self {
            Self::Leaf(sh) => sh.span.clone(),
            Self::Vertex(bottom, top) => bottom.span.start..top.span.end,
        }
    }
    fn len(&self) -> u32 {
        self.span().end - self.span().start
    }
}

pub(crate) struct ShardRouter {
    span: Range<u32>,
    config: Arc<InternalConfig>,
    node: RwLock<ShardNode>,
    stats: Arc<InternalStats>,
}

impl ShardRouter {
    pub(crate) const END_OF_SHARDS: u32 = 1u32 << 16;

    pub(crate) fn new(config: Arc<InternalConfig>, stats: Arc<InternalStats>) -> Result<Self> {
        let mut shards = Self::load(&config, &stats)?;
        if shards.is_empty() {
            shards = Self::create_initial_shards(&config, &stats)?;
        }
        let root = Self::treeify(shards, stats.clone());
        Ok(Self {
            span: root.span(),
            config,
            node: RwLock::new(root),
            stats,
        })
    }

    fn load(config: &Arc<InternalConfig>, stats: &Arc<InternalStats>) -> Result<Vec<Shard>> {
        let mut found_shards = vec![];
        for res in std::fs::read_dir(&config.dir_path)? {
            let entry = res?;
            let filename = entry.file_name();
            let Some(filename) = filename.to_str() else {
                continue;
            };
            let Ok(filetype) = entry.file_type() else {
                continue;
            };
            if !filetype.is_file() {
                continue;
            }
            if filename.starts_with("compact_")
                || filename.starts_with("bottom_")
                || filename.starts_with("top_")
            {
                std::fs::remove_file(entry.path())?;
                continue;
            } else if !filename.starts_with("shard_") {
                continue;
            }
            let Some((_, span)) = filename.split_once("_") else {
                continue;
            };
            let Some((start, end)) = span.split_once("-") else {
                continue;
            };
            let start = u32::from_str_radix(start, 16).expect(filename);
            let end = u32::from_str_radix(end, 16).expect(filename);

            ensure!(
                start < end && end <= Self::END_OF_SHARDS,
                "Bad span for {filename}"
            );

            found_shards.push(start..end);
        }

        let (shards_to_keep, shards_to_remove) = consolidate_ranges(found_shards);
        for span in shards_to_remove {
            std::fs::remove_file(
                config
                    .dir_path
                    .join(format!("shard_{:04x}-{:04x}", span.start, span.end)),
            )?;
        }

        let mut shards = vec![];
        for span in shards_to_keep {
            shards.push(Shard::open(
                config
                    .dir_path
                    .join(format!("shard_{:04x}-{:04x}", span.start, span.end)),
                span,
                false,
                config.clone(),
                stats.clone(),
            )?);
        }

        Ok(shards)
    }

    fn create_initial_shards(
        config: &Arc<InternalConfig>,
        stats: &Arc<InternalStats>,
    ) -> Result<Vec<Shard>> {
        let step = (Self::END_OF_SHARDS as f64)
            / (config.expected_number_of_keys as f64 / Shard::EXPECTED_CAPACITY as f64).max(1.0);
        let step = 1 << (step as u32).ilog2();

        let mut shards = vec![];
        let mut start = 0;
        while start < Self::END_OF_SHARDS {
            let end = start + step;
            shards.push(Shard::open(
                config
                    .dir_path
                    .join(format!("shard_{:04x}-{:04x}", start, end)),
                start..end,
                true,
                config.clone(),
                stats.clone(),
            )?);
            start = end;
        }

        Ok(shards)
    }

    fn from_shardnode(n: ShardNode, stats: Arc<InternalStats>) -> Self {
        let config = match n {
            ShardNode::Leaf(ref sh) => sh.config.clone(),
            ShardNode::Vertex(ref bottom, _) => bottom.config.clone(),
        };
        Self {
            config,
            span: n.span(),
            node: RwLock::new(n),
            stats,
        }
    }

    fn treeify(shards: Vec<Shard>, stats: Arc<InternalStats>) -> ShardNode {
        // algorithm: first find the smallest span, and let that be our base unit, say it's 1K. then go over
        // 0..64K in 1K increments and pair up every consecutive pairs whose size is 1K. we count on the spans to be
        // sorted, so we'll merge 0..1K with 1K..2K, and not 1K..3K with 2K..3K.
        // then we double our base unit and repeat, until base unit = 64K.

        let mut nodes = vec![];
        let mut unit: u32 = Self::END_OF_SHARDS;
        {
            let mut spans_debug: Vec<Range<u32>> = vec![];
            for sh in shards {
                assert!(
                    spans_debug.is_empty() || spans_debug.last().unwrap().start != sh.span.start,
                    "two elements with the same start {spans_debug:?} {:?}",
                    sh.span
                );
                spans_debug.push(sh.span.clone());
                let n = ShardNode::Leaf(sh);
                if unit > n.len() {
                    unit = n.len();
                }
                nodes.push(n);
            }
            assert!(
                spans_debug.is_sorted_by(|a, b| a.start < b.start),
                "not sorted {spans_debug:?}"
            );

            assert!(unit >= 1 && unit.is_power_of_two(), "unit={unit}");
            assert!(nodes.len() > 0, "No shards to merge");
            assert!(nodes.len() > 1 || unit == Self::END_OF_SHARDS);
        }

        while unit < Self::END_OF_SHARDS {
            let mut i = 0;
            while i < nodes.len() - 1 {
                if nodes[i].len() == unit && nodes[i + 1].len() == unit {
                    let n0 = nodes.remove(i);
                    let n1 = nodes.remove(i);
                    nodes.insert(
                        i,
                        ShardNode::Vertex(
                            Arc::new(Self::from_shardnode(n0, stats.clone())),
                            Arc::new(Self::from_shardnode(n1, stats.clone())),
                        ),
                    );
                } else {
                    i += 1;
                }
            }

            unit *= 2;
        }

        assert_eq!(nodes.len(), 1);
        nodes.remove(0)
    }

    pub(crate) fn shared_op<T>(
        &self,
        shard_selector: u32,
        func: impl FnOnce(&Shard) -> Result<T>,
    ) -> Result<T> {
        match &*self.node.read() {
            ShardNode::Leaf(sh) => func(sh),
            ShardNode::Vertex(bottom, top) => {
                if shard_selector < bottom.span.end {
                    bottom.shared_op(shard_selector, func)
                } else {
                    top.shared_op(shard_selector, func)
                }
            }
        }
    }

    pub(crate) fn clear(&self) -> Result<()> {
        let mut guard = self.node.write();

        for res in std::fs::read_dir(&self.config.dir_path)? {
            let entry = res?;
            let filename = entry.file_name();
            let Some(filename) = filename.to_str() else {
                continue;
            };
            let Ok(filetype) = entry.file_type() else {
                continue;
            };
            if !filetype.is_file() {
                continue;
            }
            if filename.starts_with("shard_")
                || filename.starts_with("compact_")
                || filename.starts_with("bottom_")
                || filename.starts_with("top_")
            {
                std::fs::remove_file(entry.path())?;
            }
        }

        let shards = Self::create_initial_shards(&self.config, &self.stats)?;
        *guard = Self::treeify(shards, self.stats.clone());

        Ok(())
    }

    pub(crate) fn call_on_all_shards<T>(
        &self,
        mut func: impl FnMut(&Shard) -> Result<T> + Copy,
    ) -> Result<Vec<T>> {
        match &*self.node.read() {
            ShardNode::Leaf(sh) => Ok(vec![func(sh)?]),
            ShardNode::Vertex(bottom, top) => {
                let mut v = bottom.call_on_all_shards(func)?;
                v.extend(top.call_on_all_shards(func)?);
                Ok(v)
            }
        }
    }

    fn split_shard(&self) -> Result<()> {
        let mut guard = self.node.write();
        let ShardNode::Leaf(sh) = &*guard else {
            // already split
            return Ok(());
        };
        let mid = (sh.span.start + sh.span.end) / 2;

        let t0 = Instant::now();

        let bottomfile = self
            .config
            .dir_path
            .join(format!("bottom_{:04x}-{:04x}", sh.span.start, mid));
        let topfile = self
            .config
            .dir_path
            .join(format!("top_{:04x}-{:04x}", mid, sh.span.end));

        let bottom_shard = Shard::open(
            bottomfile.clone(),
            sh.span.start..mid,
            true,
            self.config.clone(),
            self.stats.clone(),
        )?;
        let top_shard = Shard::open(
            topfile.clone(),
            mid..sh.span.end,
            true,
            self.config.clone(),
            self.stats.clone(),
        )?;

        sh.split_into(&bottom_shard, &top_shard)?;

        std::fs::rename(
            bottomfile,
            self.config.dir_path.join(format!(
                "shard_{:04x}-{:04x}",
                bottom_shard.span.start, bottom_shard.span.end
            )),
        )?;
        std::fs::rename(
            topfile,
            self.config.dir_path.join(format!(
                "shard_{:04x}-{:04x}",
                top_shard.span.start, top_shard.span.end
            )),
        )?;
        std::fs::remove_file(
            self.config
                .dir_path
                .join(format!("shard_{:04x}-{:04x}", sh.span.start, sh.span.end)),
        )?;

        self.stats.report_split(
            t0,
            bottom_shard.get_write_offset(),
            top_shard.get_write_offset(),
        );

        *guard = ShardNode::Vertex(
            Arc::new(ShardRouter {
                span: bottom_shard.span.clone(),
                config: self.config.clone(),
                node: RwLock::new(ShardNode::Leaf(bottom_shard)),
                stats: self.stats.clone(),
            }),
            Arc::new(ShardRouter {
                span: top_shard.span.clone(),
                config: self.config.clone(),
                node: RwLock::new(ShardNode::Leaf(top_shard)),
                stats: self.stats.clone(),
            }),
        );

        Ok(())
    }

    fn compact_shard(&self, write_offset: u64) -> Result<()> {
        let mut guard = self.node.write();
        let ShardNode::Leaf(sh) = &*guard else {
            // was split
            return Ok(());
        };
        if sh.get_write_offset() < write_offset {
            // already compacted
            return Ok(());
        };

        let t0 = Instant::now();
        let orig_filename = self
            .config
            .dir_path
            .join(format!("shard_{:04x}-{:04x}", sh.span.start, sh.span.end));
        let tmpfile = self
            .config
            .dir_path
            .join(format!("compact_{:04x}-{:04x}", sh.span.start, sh.span.end));

        let mut compacted_shard = Shard::open(
            tmpfile.clone(),
            sh.span.clone(),
            true,
            self.config.clone(),
            self.stats.clone(),
        )?;

        // XXX: this can be done in a background thread, holding a read lock until we're done, and then wrap it
        // all up under a write lock
        sh.compact_into(&mut compacted_shard)?;

        std::fs::rename(tmpfile, orig_filename)?;

        self.stats.report_compaction(
            t0,
            sh.get_write_offset(),
            compacted_shard.get_write_offset(),
        );

        *guard = ShardNode::Leaf(compacted_shard);

        Ok(())
    }

    pub(crate) fn insert(
        &self,
        ph: PartedHash,
        full_key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<InsertStatus> {
        loop {
            let res = match &*self.node.read() {
                ShardNode::Leaf(sh) => sh.insert(ph, full_key, val, mode, true)?,
                ShardNode::Vertex(bottom, top) => {
                    if (ph.shard_selector() as u32) < bottom.span.end {
                        bottom.insert(ph, full_key, val, mode)?
                    } else {
                        top.insert(ph, full_key, val, mode)?
                    }
                }
            };

            match res {
                InsertStatus::SplitNeeded => {
                    self.split_shard()?;
                    // retry
                }
                InsertStatus::CompactionNeeded(write_offset) => {
                    self.compact_shard(write_offset)?;
                    // retry
                }
                _ => {
                    return Ok(res);
                }
            }
        }
    }
}
