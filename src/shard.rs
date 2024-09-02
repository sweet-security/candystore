use anyhow::bail;
use bytemuck::{bytes_of_mut, Pod, Zeroable};
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use simd_itertools::PositionSimd;
use std::{
    fs::{File, OpenOptions},
    io::Read,
    ops::Range,
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Instant,
};

use memmap::{MmapMut, MmapOptions};

use crate::Result;
use crate::{
    hashing::{PartedHash, INVALID_SIG},
    stats::InternalStats,
    store::InternalConfig,
};

//
// these numbers were chosen according to the simulation, as they allow for 90% utilization of the shard with
// virtually zero chance of in-row collisions and "smallish" shard size: shards start at 384KB and
// can hold 32K entries, and since we're limited at 4GB file sizes, we can key-value pairs of up to 128KB
// (keys and values are limited to 64KB each anyway)
//
// other good combinations are 32/512, 32/1024, 64/256, 64/1024, 128/512, 256/256
//
pub(crate) const NUM_ROWS: usize = 64;
pub(crate) const ROW_WIDTH: usize = 512;

#[repr(C)]
struct ShardRow {
    signatures: [u32; ROW_WIDTH],
    offsets_and_sizes: [u64; ROW_WIDTH], // | key_size: 16 | val_size: 16 | file_offset: 32 |
}

impl ShardRow {
    #[inline]
    fn lookup(&self, sig: u32, start_idx: &mut usize) -> Option<usize> {
        if let Some(rel_idx) = self.signatures[*start_idx..].iter().position_simd(sig) {
            let abs_idx = rel_idx + *start_idx;
            *start_idx = abs_idx + 1;
            Some(abs_idx)
        } else {
            None
        }
    }
}

#[test]
fn test_row_lookup() -> Result<()> {
    let mut row = ShardRow {
        signatures: [0; ROW_WIDTH],
        offsets_and_sizes: [0; ROW_WIDTH],
    };

    row.signatures[7] = 123;
    row.signatures[8] = 123;
    row.signatures[9] = 123;
    row.signatures[90] = 123;
    row.signatures[ROW_WIDTH - 1] = 999;

    let mut start = 0;
    assert_eq!(row.lookup(123, &mut start), Some(7));
    assert_eq!(start, 8);
    assert_eq!(row.lookup(123, &mut start), Some(8));
    assert_eq!(start, 9);
    assert_eq!(row.lookup(123, &mut start), Some(9));
    assert_eq!(start, 10);
    assert_eq!(row.lookup(123, &mut start), Some(90));
    assert_eq!(start, 91);
    assert_eq!(row.lookup(123, &mut start), None);
    assert_eq!(start, 91);

    start = 0;
    assert_eq!(row.lookup(0, &mut start), Some(0));
    assert_eq!(start, 1);

    start = 0;
    assert_eq!(row.lookup(999, &mut start), Some(ROW_WIDTH - 1));
    assert_eq!(start, ROW_WIDTH);

    assert_eq!(row.lookup(999, &mut start), None);
    assert_eq!(start, ROW_WIDTH);

    Ok(())
}

#[repr(C, align(4096))]
struct PageAligned<T>(T);

pub(crate) const SHARD_FILE_MAGIC: [u8; 8] = *b"CandyStr";
pub(crate) const SHARD_FILE_VERSION: u64 = 10;

#[derive(Clone, Copy, Default, Debug, Pod, Zeroable)]
#[repr(C)]
struct MetaHeader {
    magic: [u8; 8],
    version: u64,
}

#[repr(C)]
struct ShardHeader {
    metadata: MetaHeader,
    wasted_bytes: AtomicU64,
    write_offset: AtomicU64,
    num_inserts: AtomicU64,
    num_removals: AtomicU64,
    compacted_up_to: AtomicUsize,
    rows: PageAligned<[ShardRow; NUM_ROWS]>,
}

pub(crate) const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;
const _: () = assert!(HEADER_SIZE % 4096 == 0);

#[derive(Debug)]
pub(crate) enum InsertStatus {
    Added,
    Replaced(Vec<u8>),
    KeyDoesNotExist,
    SplitNeeded,
    AlreadyExists(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum InsertMode<'a> {
    Set,
    Replace(Option<&'a [u8]>),
    GetOrCreate,
}

enum TryReplaceStatus<'a> {
    KeyDoesNotExist(RwLockWriteGuard<'a, ()>, bool),
    KeyExistsNotReplaced(Vec<u8>),
    KeyExistsReplaced(Vec<u8>),
}

pub(crate) type KVPair = (Vec<u8>, Vec<u8>);

struct MmapFile {
    file: File,
    mmap: MmapMut,
}

impl MmapFile {
    fn new(file: File, mlock_headers: bool) -> Result<Self> {
        let mmap = unsafe { MmapOptions::new().len(HEADER_SIZE as usize).map_mut(&file) }?;

        #[cfg(target_family = "unix")]
        if mlock_headers {
            unsafe { libc::mlock(mmap.as_ptr() as *const _, mmap.len()) };
        }

        // optimization, we don't care about the return code
        #[cfg(target_family = "unix")]
        unsafe {
            libc::posix_fallocate(file.as_raw_fd(), 0, HEADER_SIZE as i64)
        };

        let header = unsafe { &mut *(mmap.as_ptr() as *mut ShardHeader) };
        header.metadata.magic = SHARD_FILE_MAGIC;
        header.metadata.version = SHARD_FILE_VERSION;

        Ok(Self { file, mmap })
    }

    fn create(filename: impl AsRef<Path>, config: &InternalConfig) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;
        file.set_len(
            HEADER_SIZE
                + if config.truncate_up {
                    config.max_shard_size as u64
                } else {
                    0
                },
        )?;
        Self::new(file, config.mlock_headers)
    }

    #[inline(always)]
    fn header(&self) -> &ShardHeader {
        unsafe { &*(self.mmap.as_ptr() as *const ShardHeader) }
    }
    #[inline(always)]
    fn header_mut(&self) -> &mut ShardHeader {
        unsafe { &mut *(self.mmap.as_ptr() as *mut ShardHeader) }
    }
    #[inline(always)]
    fn row(&self, row_idx: usize) -> &ShardRow {
        &self.header().rows.0[row_idx]
    }
    #[inline(always)]
    fn row_mut(&self, row_idx: usize) -> &mut ShardRow {
        &mut self.header_mut().rows.0[row_idx]
    }

    // reading doesn't require holding any locks - we only ever extend the file, never overwrite data
    fn read_kv(&self, stats: &InternalStats, offset_and_size: u64) -> Result<KVPair> {
        let klen = (offset_and_size >> 48) as usize;
        debug_assert_eq!(klen >> 14, 0, "attempting to read a special key");
        let vlen = ((offset_and_size >> 32) & 0xffff) as usize;
        let offset = (offset_and_size as u32) as u64;
        let mut buf = vec![0u8; klen + vlen];
        self.file.read_exact_at(&mut buf, HEADER_SIZE + offset)?;

        stats.num_read_bytes.fetch_add(buf.len(), Ordering::Relaxed);
        stats.num_read_ops.fetch_add(1, Ordering::Relaxed);

        let val = buf[klen..klen + vlen].to_owned();
        buf.truncate(klen);

        Ok((buf, val))
    }

    // writing doesn't require holding any locks since we write with an offset
    fn write_kv(&self, stats: &InternalStats, key: &[u8], val: &[u8]) -> Result<u64> {
        let entry_size = key.len() + val.len();
        let mut buf = vec![0u8; entry_size];
        buf[..key.len()].copy_from_slice(key);
        buf[key.len()..].copy_from_slice(val);

        // atomically allocate some area. it may leak if the IO below fails or if we crash before updating the
        // offsets_and_size array, but we're okay with leaks
        let write_offset = self
            .header()
            .write_offset
            .fetch_add(buf.len() as u64, Ordering::SeqCst) as u64;

        // now writing can be non-atomic (pwrite)
        self.file.write_all_at(&buf, HEADER_SIZE + write_offset)?;
        stats.add_entry(entry_size);

        Ok(((key.len() as u64) << 48) | ((val.len() as u64) << 32) | write_offset)
    }
}

struct TPHandle {
    rx: crossbeam_channel::Receiver<Result<()>>,
}
impl TPHandle {
    fn wait(&self) -> Result<()> {
        self.rx.recv()?
    }
    fn finished(&self) -> bool {
        !self.rx.is_empty()
    }
}

struct CompactionInfo {
    config: Arc<InternalConfig>,
    stats: Arc<InternalStats>,
    files: Arc<RwLock<(MmapFile, Option<MmapFile>)>>,
    row_locks: Arc<[RwLock<()>; NUM_ROWS]>,
    t0: Instant,
    src_filename: PathBuf,
    target_filename: PathBuf,
}

pub(crate) struct CompactionThreadPool {
    tx: crossbeam_channel::Sender<Option<(CompactionInfo, crossbeam_channel::Sender<Result<()>>)>>,
    threads: Vec<JoinHandle<Result<()>>>,
}

impl CompactionThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded::<
            Option<(CompactionInfo, crossbeam_channel::Sender<Result<()>>)>,
        >();
        let mut threads = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let rx = rx.clone();
            let handle = std::thread::spawn(move || {
                for elem in rx.iter() {
                    let Some((info, handle_tx)) = elem else {
                        break;
                    };
                    let res = Shard::background_compact(info);
                    handle_tx.send(res)?;
                }
                Ok(())
            });
            threads.push(handle);
        }

        Self { tx, threads }
    }

    fn submit(&self, info: CompactionInfo) -> Result<TPHandle> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.tx.send(Some((info, tx)))?;
        Ok(TPHandle { rx })
    }

    #[allow(dead_code)]
    pub fn terminate(self) -> Result<()> {
        for _ in self.threads.iter() {
            self.tx.send(None)?;
        }

        for th in self.threads {
            match th.join() {
                Err(e) => std::panic::resume_unwind(e),
                Ok(res) => res?,
            }
        }
        Ok(())
    }
}

pub(crate) struct Shard {
    pub(crate) span: Range<u32>,
    pub(crate) config: Arc<InternalConfig>,
    stats: Arc<InternalStats>,
    files: Arc<RwLock<(MmapFile, Option<MmapFile>)>>,
    row_locks: Arc<[RwLock<()>; NUM_ROWS]>,
    threadpool: Arc<CompactionThreadPool>,
    compaction_handle: Arc<Mutex<Option<TPHandle>>>,
    #[cfg(feature = "flush_aggregation")]
    sync_agg_mutex: parking_lot::Mutex<()>,
    #[cfg(feature = "flush_aggregation")]
    in_sync_agg_delay: std::sync::atomic::AtomicBool,
}

impl Shard {
    pub(crate) const EXPECTED_CAPACITY: usize = (NUM_ROWS * ROW_WIDTH * 9) / 10; // ~ 29,500

    pub(crate) fn open(
        span: Range<u32>,
        truncate: bool,
        config: Arc<InternalConfig>,
        stats: Arc<InternalStats>,
        threadpool: Arc<CompactionThreadPool>,
    ) -> Result<Self> {
        let filename = config
            .dir_path
            .join(format!("shard_{:04x}-{:04x}", span.start, span.end));
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(&filename)?;

        let mut file_size = file.metadata()?.len();
        if file_size != 0 {
            let mut meta_header = MetaHeader::default();
            let sz = file.read(bytes_of_mut(&mut meta_header))?;
            if sz != size_of::<MetaHeader>()
                || meta_header.magic != SHARD_FILE_MAGIC
                || meta_header.version != SHARD_FILE_VERSION
            {
                if config.clear_on_unsupported_version {
                    file.set_len(0)?;
                    file_size = 0;
                } else {
                    bail!(
                        "{filename:?} unsupported magic={:?} version=0x{:016x} size={}",
                        meta_header.magic,
                        meta_header.version,
                        file_size,
                    );
                }
            }

            if file_size != 0 && file_size < HEADER_SIZE {
                if config.clear_on_unsupported_version {
                    file.set_len(0)?;
                    file_size = 0;
                } else {
                    bail!("corrupt shard file (size={})", file_size);
                }
            }
        }

        if file_size == 0 {
            if config.truncate_up {
                // when creating, set the file's length so that we won't need to extend it every time we write
                // (saves on file metadata updates)
                file.set_len(HEADER_SIZE + config.max_shard_size as u64)?;
            } else {
                file.set_len(HEADER_SIZE)?;
            }
        }

        let mut row_locks = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS {
            row_locks.push(RwLock::new(()));
        }
        let row_locks: [RwLock<()>; NUM_ROWS] = row_locks.try_into().unwrap();

        let mut mmap_file = MmapFile::new(file, config.mlock_headers)?;

        let compacted_filename = config
            .dir_path
            .join(format!("compact_{:04x}-{:04x}", span.start, span.end));
        if truncate {
            _ = std::fs::remove_file(compacted_filename);
        } else {
            if let Ok(compacted_file) = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&compacted_filename)
            {
                let target = MmapFile::new(compacted_file, config.mlock_headers)?;
                Self::do_compaction(&row_locks, &mmap_file, &target, &stats, &config)?;
                std::fs::rename(compacted_filename, filename)?;
                mmap_file = target;
            }
        }

        Ok(Self {
            span,
            config,
            stats,
            files: Arc::new(RwLock::new((mmap_file, None))),
            row_locks: Arc::new(row_locks),
            threadpool,
            compaction_handle: Arc::new(Mutex::new(None)),
            #[cfg(feature = "flush_aggregation")]
            sync_agg_mutex: parking_lot::Mutex::new(()),
            #[cfg(feature = "flush_aggregation")]
            in_sync_agg_delay: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn new(
        span: Range<u32>,
        mmap_file: MmapFile,
        config: Arc<InternalConfig>,
        stats: Arc<InternalStats>,
        threadpool: Arc<CompactionThreadPool>,
    ) -> Result<Self> {
        let mut row_locks = Vec::with_capacity(NUM_ROWS);
        for _ in 0..NUM_ROWS {
            row_locks.push(RwLock::new(()));
        }
        let row_locks: [RwLock<()>; NUM_ROWS] = row_locks.try_into().unwrap();

        Ok(Self {
            span,
            config,
            stats,
            files: Arc::new(RwLock::new((mmap_file, None))),
            row_locks: Arc::new(row_locks),
            threadpool,
            compaction_handle: Arc::new(Mutex::new(None)),
            #[cfg(feature = "flush_aggregation")]
            sync_agg_mutex: parking_lot::Mutex::new(()),
            #[cfg(feature = "flush_aggregation")]
            in_sync_agg_delay: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn do_compaction(
        row_locks: &[RwLock<()>; NUM_ROWS],
        src: &MmapFile,
        target: &MmapFile,
        stats: &InternalStats,
        config: &InternalConfig,
    ) -> Result<()> {
        let mut first_row = true;
        loop {
            let row_idx = target.header().compacted_up_to.load(Ordering::Acquire);
            if row_idx >= NUM_ROWS {
                break;
            }

            let _row_guard = row_locks[row_idx].write();
            let src_row = src.row(row_idx);
            let target_row = target.row_mut(row_idx);
            let mut target_col = 0;

            for (src_col, &sig) in src_row.signatures.iter().enumerate() {
                if sig == INVALID_SIG {
                    continue;
                }
                let (k, v) = src.read_kv(&stats, src_row.offsets_and_sizes[src_col])?;

                assert!(
                    first_row || target_row.signatures[target_col] == INVALID_SIG,
                    "row={row_idx} col={target_col} sig={}",
                    target_row.signatures[target_col]
                );
                let ph = PartedHash::new(&config.hash_seed, &k);
                assert_eq!(ph.row_selector(), row_idx);
                target_row.offsets_and_sizes[target_col] = target.write_kv(&stats, &k, &v)?;
                std::sync::atomic::fence(Ordering::SeqCst);
                target_row.signatures[target_col] = ph.signature();
                target.header().num_inserts.fetch_add(1, Ordering::Relaxed);
                target_col += 1;
            }

            target
                .header()
                .compacted_up_to
                .fetch_add(1, Ordering::Release);
            first_row = false;
        }

        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<()> {
        //self.mmap.flush()? -- fdatasync should take care of that as well
        self.files.read().0.file.sync_data()?;
        Ok(())
    }

    pub(crate) fn split(&self) -> Result<(Shard, Shard)> {
        let mut handle_guard = self.compaction_handle.lock();
        if let Some(handle) = handle_guard.take() {
            handle.wait()?;
        }

        let files_guard = self.files.write();

        let mid = (self.span.start + self.span.end) / 2;

        let t0 = Instant::now();

        let bottom_filename = self
            .config
            .dir_path
            .join(format!("bottom_{:04x}-{:04x}", self.span.start, mid));
        let top_filename = self
            .config
            .dir_path
            .join(format!("top_{:04x}-{:04x}", mid, self.span.end));

        let bottom_file = MmapFile::create(&bottom_filename, &self.config)?;
        let top_file = MmapFile::create(&top_filename, &self.config)?;

        for (row_idx, src_row) in files_guard.0.header().rows.0.iter().enumerate() {
            let mut bottom_col = 0;
            let mut top_col = 0;
            for (col, &sig) in src_row.signatures.iter().enumerate() {
                if sig == INVALID_SIG {
                    continue;
                }
                let (k, v) = files_guard
                    .0
                    .read_kv(&self.stats, src_row.offsets_and_sizes[col])?;
                let ph = PartedHash::new(&self.config.hash_seed, &k);
                assert_eq!(row_idx, ph.row_selector());

                let (file, col) = if ph.shard_selector() < mid {
                    (&bottom_file, &mut bottom_col)
                } else {
                    (&top_file, &mut top_col)
                };

                let target_row = file.row_mut(ph.row_selector());
                assert_eq!(
                    target_row.signatures[*col], INVALID_SIG,
                    "row={} col={} sig={}",
                    row_idx, *col, target_row.signatures[*col]
                );
                target_row.offsets_and_sizes[*col] = file.write_kv(&self.stats, &k, &v)?;
                std::sync::atomic::fence(Ordering::SeqCst);
                target_row.signatures[*col] = ph.signature();
                file.header().num_inserts.fetch_add(1, Ordering::Relaxed);
                *col += 1;
            }
        }

        std::fs::rename(
            bottom_filename,
            self.config
                .dir_path
                .join(format!("shard_{:04x}-{:04x}", self.span.start, mid,)),
        )?;
        std::fs::rename(
            top_filename,
            self.config
                .dir_path
                .join(format!("shard_{:04x}-{:04x}", mid, self.span.end)),
        )?;
        std::fs::remove_file(self.config.dir_path.join(format!(
            "shard_{:04x}-{:04x}",
            self.span.start, self.span.end
        )))?;

        self.stats.report_split(
            t0,
            bottom_file.header().write_offset.load(Ordering::Relaxed),
            top_file.header().write_offset.load(Ordering::Relaxed),
        );

        let bottom = Self::new(
            self.span.start..mid,
            bottom_file,
            self.config.clone(),
            self.stats.clone(),
            self.threadpool.clone(),
        )?;
        let top = Self::new(
            mid..self.span.end,
            top_file,
            self.config.clone(),
            self.stats.clone(),
            self.threadpool.clone(),
        )?;

        Ok((bottom, top))
    }

    fn operate_on_row<T>(
        &self,
        row_idx: usize,
        func: impl FnOnce(&MmapFile, &ShardRow) -> Result<T>,
    ) -> Result<T> {
        let files_guard = self.files.read();
        let _row_guard = self.row_locks[row_idx].read();
        let file = if let Some(ref target) = files_guard.1 {
            if row_idx < target.header().compacted_up_to.load(Ordering::Acquire) {
                target
            } else {
                &files_guard.0
            }
        } else {
            &files_guard.0
        };

        func(file, file.row(row_idx))
    }

    fn operate_on_row_mut<T>(
        &self,
        row_idx: usize,
        func: impl FnOnce(&MmapFile, bool, RwLockWriteGuard<()>, &mut ShardRow) -> Result<T>,
    ) -> Result<T> {
        let files_guard = self.files.read();
        let row_guard = self.row_locks[row_idx].write();
        let file = if let Some(ref target) = files_guard.1 {
            if row_idx < target.header().compacted_up_to.load(Ordering::Acquire) {
                target
            } else {
                &files_guard.0
            }
        } else {
            &files_guard.0
        };

        func(
            &file,
            files_guard.1.is_some(),
            row_guard,
            file.row_mut(row_idx),
        )
    }

    pub(crate) fn read_at(&self, row_idx: usize, entry_idx: usize) -> Result<Option<KVPair>> {
        self.operate_on_row(row_idx, |file, row| {
            if row.signatures[entry_idx] != INVALID_SIG {
                Ok(Some(
                    file.read_kv(&self.stats, row.offsets_and_sizes[entry_idx])?,
                ))
            } else {
                Ok(None)
            }
        })
    }

    pub(crate) fn get_by_hash(&self, ph: PartedHash) -> Result<Vec<KVPair>> {
        self.operate_on_row(ph.row_selector(), |file, row| {
            let mut first_time = true;
            let mut kvs = Vec::with_capacity(1);
            let mut start = 0;
            while let Some(idx) = row.lookup(ph.signature(), &mut start) {
                kvs.push(file.read_kv(&self.stats, row.offsets_and_sizes[idx])?);
                if first_time {
                    self.stats
                        .num_positive_lookups
                        .fetch_add(1, Ordering::Relaxed);
                    first_time = false;
                }
            }
            if kvs.is_empty() {
                self.stats
                    .num_negative_lookups
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(kvs)
        })
    }

    pub(crate) fn get(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.operate_on_row(ph.row_selector(), |file, row| {
            let mut start = 0;
            while let Some(idx) = row.lookup(ph.signature(), &mut start) {
                let (k, v) = file.read_kv(&self.stats, row.offsets_and_sizes[idx])?;
                if key == k {
                    self.stats
                        .num_positive_lookups
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(v));
                }
            }
            self.stats
                .num_negative_lookups
                .fetch_add(1, Ordering::Relaxed);
            Ok(None)
        })
    }

    #[cfg(feature = "flush_aggregation")]
    fn flush_aggregation(&self) -> Result<()> {
        let Some(delay) = self.config.flush_aggregation_delay else {
            return Ok(());
        };

        let do_sync = || {
            self.in_sync_agg_delay.store(true, Ordering::SeqCst);
            std::thread::sleep(delay);
            self.in_sync_agg_delay.store(false, Ordering::SeqCst);
            self.flush()
        };

        if let Some(_guard) = self.sync_agg_mutex.try_lock() {
            // we're the first ones here. wait for the aggregation duration and sync the file
            do_sync()?;
        } else {
            // another thread is currently sync'ing, we're waiting in line. if the holder of the lock is in the
            // sleep (aggregation) phase, we can just wait for it to finish and return -- the other thread will
            // have sync'ed us by the time we got the lock. otherwise, we'll need to sync as well
            let was_in_delay = self.in_sync_agg_delay.load(Ordering::Relaxed);
            let _guard = self.sync_agg_mutex.lock();
            if !was_in_delay {
                do_sync()?;
            }
        }
        Ok(())
    }

    fn try_replace<'a>(
        &'a self,
        file: &MmapFile,
        row_guard: RwLockWriteGuard<'a, ()>,
        row: &mut ShardRow,
        ph: PartedHash,
        key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<TryReplaceStatus> {
        let mut start = 0;
        let mut had_collision = false;
        while let Some(idx) = row.lookup(ph.signature(), &mut start) {
            let (k, existing_val) = file.read_kv(&self.stats, row.offsets_and_sizes[idx])?;
            if key != k {
                had_collision = true;
                continue;
            }
            match mode {
                InsertMode::GetOrCreate => {
                    // no-op, key already exists
                    self.stats
                        .num_positive_lookups
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(TryReplaceStatus::KeyExistsNotReplaced(existing_val));
                }
                InsertMode::Set => {
                    // fall through
                }
                InsertMode::Replace(expected_val) => {
                    if expected_val.is_some_and(|expected_val| expected_val != existing_val) {
                        return Ok(TryReplaceStatus::KeyExistsNotReplaced(existing_val));
                    }
                }
            }

            // optimization
            if val != existing_val {
                row.offsets_and_sizes[idx] = file.write_kv(&self.stats, key, val)?;
                file.header()
                    .wasted_bytes
                    .fetch_add((k.len() + existing_val.len()) as u64, Ordering::Relaxed);
                self.stats.num_updates.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "flush_aggregation")]
                {
                    drop(row_guard);
                    self.flush_aggregation()?;
                }
            }
            return Ok(TryReplaceStatus::KeyExistsReplaced(existing_val));
        }

        Ok(TryReplaceStatus::KeyDoesNotExist(row_guard, had_collision))
    }

    fn wait_for_compaction(&self) -> Result<()> {
        let mut handle_guard = self.compaction_handle.lock();
        if let Some(handle) = handle_guard.take() {
            handle.wait()?;
        }
        Ok(())
    }

    fn begin_compaction(&self, min_write_offset: u64) -> Result<()> {
        let mut handle_guard = self.compaction_handle.lock();
        let mut files_guard = self.files.write();

        if files_guard.0.header().write_offset.load(Ordering::Relaxed) < min_write_offset {
            // already compacted by someone else
            return Ok(());
        }

        if files_guard.1.is_some() {
            // if the compaction target exists and the thread is still running -- all good
            if let Some(ref handle) = *handle_guard {
                if !handle.finished() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }

        // the thread could've crashed in the middle of a compaction, and here's the place to extract the error
        if let Some(handle) = handle_guard.take() {
            handle.wait()?;
        }

        assert!(files_guard.1.is_none());

        let t0 = Instant::now();
        let src_filename = self.config.dir_path.join(format!(
            "shard_{:04x}-{:04x}",
            self.span.start, self.span.end
        ));
        let target_filename = self.config.dir_path.join(format!(
            "compact_{:04x}-{:04x}",
            self.span.start, self.span.end
        ));
        let target = MmapFile::create(&target_filename, &self.config)?;
        target.header().compacted_up_to.store(0, Ordering::Release);
        files_guard.1 = Some(target);

        let handle = self.threadpool.submit(CompactionInfo {
            files: self.files.clone(),
            stats: self.stats.clone(),
            row_locks: self.row_locks.clone(),
            config: self.config.clone(),
            t0,
            src_filename,
            target_filename,
        })?;
        *handle_guard = Some(handle);

        Ok(())
    }

    fn background_compact(info: CompactionInfo) -> Result<()> {
        let mut files_guard = info.files.upgradable_read();
        let src = &files_guard.0;
        let target = files_guard.1.as_ref().unwrap();

        Self::do_compaction(&info.row_locks, src, target, &info.stats, &info.config)?;

        std::fs::rename(&info.target_filename, &info.src_filename)?;

        info.stats.report_compaction(
            info.t0,
            src.header().write_offset.load(Ordering::Relaxed),
            target.header().write_offset.load(Ordering::Relaxed),
        );

        files_guard.with_upgraded(|files| {
            files.0 = files.1.take().unwrap();
        });
        Ok(())
    }

    pub(crate) fn insert(
        &self,
        ph: PartedHash,
        full_key: &[u8],
        val: &[u8],
        mode: InsertMode,
    ) -> Result<InsertStatus> {
        let mut should_compact = None;

        let status =
            self.operate_on_row_mut(ph.row_selector(), |file, is_compacting, row_guard, row| {
                if !is_compacting {
                    if file.header().wasted_bytes.load(Ordering::Relaxed)
                        >= self.config.min_compaction_threashold as u64
                    {
                        should_compact = Some(file.header().write_offset.load(Ordering::Relaxed));
                    } else if file.header().write_offset.load(Ordering::Relaxed)
                        + (full_key.len() + val.len()) as u64
                        > self.config.max_shard_size as u64
                    {
                        return Ok(InsertStatus::SplitNeeded);
                    }
                }

                let status = self.try_replace(file, row_guard, row, ph, &full_key, val, mode)?;
                match status {
                    TryReplaceStatus::KeyDoesNotExist(_guard, had_collision) => {
                        if matches!(mode, InsertMode::Replace(_)) {
                            return Ok(InsertStatus::KeyDoesNotExist);
                        }

                        // find an empty slot
                        let mut start = 0;
                        if let Some(idx) = row.lookup(INVALID_SIG, &mut start) {
                            let new_off = file.write_kv(&self.stats, &full_key, val)?;

                            // we don't want a reorder to happen here - first write the offset, then the signature
                            row.offsets_and_sizes[idx] = new_off;
                            std::sync::atomic::fence(Ordering::SeqCst);
                            row.signatures[idx] = ph.signature();
                            if had_collision {
                                self.stats.num_collisions.fetch_add(1, Ordering::Relaxed);
                            }
                            file.header().num_inserts.fetch_add(1, Ordering::Relaxed);
                            #[cfg(feature = "flush_aggregation")]
                            {
                                drop(_guard);
                                self.flush_aggregation()?;
                            }
                            Ok(InsertStatus::Added)
                        } else {
                            // no room in this row, must split
                            Ok(InsertStatus::SplitNeeded)
                        }
                    }
                    TryReplaceStatus::KeyExistsNotReplaced(existing) => {
                        Ok(InsertStatus::AlreadyExists(existing))
                    }
                    TryReplaceStatus::KeyExistsReplaced(existing) => {
                        Ok(InsertStatus::Replaced(existing))
                    }
                }
            })?;

        if let Some(min_write_offset) = should_compact {
            self.begin_compaction(min_write_offset)?;
        }
        Ok(status)
    }

    pub(crate) fn remove(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.operate_on_row_mut(ph.row_selector(), |file, _, _guard, row| {
            let mut start = 0;

            while let Some(idx) = row.lookup(ph.signature(), &mut start) {
                let (k, v) = file.read_kv(&self.stats, row.offsets_and_sizes[idx])?;
                if key == k {
                    row.signatures[idx] = INVALID_SIG;
                    // we managed to remove this key
                    file.header().num_removals.fetch_add(1, Ordering::Relaxed);
                    file.header()
                        .wasted_bytes
                        .fetch_add((k.len() + v.len()) as u64, Ordering::Relaxed);
                    #[cfg(feature = "flush_aggregation")]
                    {
                        drop(_guard);
                        self.flush_aggregation()?;
                    }
                    return Ok(Some(v));
                }
            }

            Ok(None)
        })
    }

    pub(crate) fn get_stats(&self) -> Result<(u64, u64, u64, u64)> {
        self.wait_for_compaction()?;
        let files_guard = self.files.read();
        let hdr = files_guard.0.header();
        Ok((
            hdr.write_offset.load(Ordering::Relaxed),
            hdr.wasted_bytes.load(Ordering::Relaxed),
            hdr.num_inserts.load(Ordering::Relaxed),
            hdr.num_removals.load(Ordering::Relaxed),
        ))
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        _ = self.wait_for_compaction();
    }
}
