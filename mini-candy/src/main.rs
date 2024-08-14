//! a very minimal implementation of CandyStore, for educational purposes. handles single-threaded get/set/remove/iter
//!
use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{Seek, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use memmap::{MmapMut, MmapOptions};
use siphasher::sip::SipHasher24;

type Result<T> = std::io::Result<T>;
const WIDTH: usize = 512;
const ROWS: usize = 64;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PartedHash(u64);

impl PartedHash {
    const INVALID_SIG: u32 = 0;
    fn new(buf: &[u8]) -> Self {
        Self(SipHasher24::new().hash(buf))
    }
    fn sig(&self) -> u32 {
        if self.0 as u32 == Self::INVALID_SIG {
            0x12345678 // can't return INVALID_SIG
        } else {
            self.0 as u32
        }
    }
    fn row(&self) -> usize {
        (self.0 as usize >> 32) % ROWS
    }
    fn shard(&self) -> u32 {
        (self.0 >> 48) as u32
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Descriptor {
    offset: u32,
    klen: u16,
    vlen: u16,
}

#[repr(C)]
struct ShardRow {
    sigs: [u32; WIDTH],
    descs: [Descriptor; WIDTH],
}

#[repr(C)]
struct ShardHeader {
    rows: [ShardRow; ROWS],
}

struct ShardFile {
    start: u32,
    end: u32,
    file: RefCell<File>,
    mmap: MmapMut,
}

type Buf = Vec<u8>;
type KV = (Buf, Buf);

impl ShardFile {
    const HEADER_SIZE: u64 = size_of::<ShardHeader>() as u64;

    fn open(dirpath: impl AsRef<Path>, start: u32, end: u32) -> Result<Self> {
        let filepath = dirpath.as_ref().join(format!("{start}-{end}"));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(filepath)?;
        file.set_len(Self::HEADER_SIZE)?;
        file.seek(std::io::SeekFrom::End(0))?;
        let mmap = unsafe {
            MmapOptions::new()
                .len(Self::HEADER_SIZE as usize)
                .map_mut(&file)
        }?;
        Ok(Self {
            start,
            end,
            file: RefCell::new(file),
            mmap,
        })
    }

    fn header_row(&self, r: usize) -> &mut ShardRow {
        &mut unsafe { &mut *(self.mmap.as_ptr() as *const ShardHeader as *mut ShardHeader) }.rows[r]
    }

    fn read(&self, desc: Descriptor) -> Result<KV> {
        let mut k = vec![0; desc.klen as usize];
        let mut v = vec![0; desc.vlen as usize];
        let f = self.file.borrow();
        f.read_exact_at(&mut k, desc.offset as u64)?;
        f.read_exact_at(&mut v, desc.offset as u64 + desc.klen as u64)?;
        Ok((k, v))
    }
    fn write(&self, key: &[u8], val: &[u8]) -> Result<Descriptor> {
        let mut f = self.file.borrow_mut();
        let offset = f.stream_position()?;
        f.write_all(key)?;
        f.write_all(val)?;
        Ok(Descriptor {
            offset: offset as u32,
            klen: key.len() as u16,
            vlen: val.len() as u16,
        })
    }

    fn get(&self, ph: PartedHash, key: &[u8]) -> Result<Option<Buf>> {
        let row = self.header_row(ph.row());
        for (i, s) in row.sigs.iter().enumerate() {
            if *s == ph.sig() {
                let desc = row.descs[i];
                let (k, v) = self.read(desc)?;
                if k == key {
                    return Ok(Some(v));
                }
            }
        }
        Ok(None)
    }

    fn set(&mut self, ph: PartedHash, key: &[u8], val: &[u8]) -> Result<bool> {
        let row = self.header_row(ph.row());
        for (i, s) in row.sigs.iter().enumerate() {
            if *s == ph.sig() {
                let desc = row.descs[i];
                let (k, _) = self.read(desc)?;
                if k == key {
                    row.descs[i] = self.write(key, val)?;
                    return Ok(true);
                }
            }
        }

        for (i, s) in row.sigs.iter_mut().enumerate() {
            if *s == PartedHash::INVALID_SIG {
                // insert new
                *s = ph.sig();
                row.descs[i] = self.write(key, val)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn remove(&mut self, ph: PartedHash, key: &[u8]) -> Result<bool> {
        let row = self.header_row(ph.row());
        for (i, s) in row.sigs.iter_mut().enumerate() {
            if *s == ph.sig() {
                let desc = row.descs[i];
                let (k, _) = self.read(desc)?;
                if k == key {
                    *s = PartedHash::INVALID_SIG;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = Result<KV>> + 'a {
        (0..ROWS).map(|r| self.header_row(r)).flat_map(|row| {
            row.sigs.iter().enumerate().filter_map(|(i, sig)| {
                if *sig == PartedHash::INVALID_SIG {
                    return None;
                }
                Some(self.read(row.descs[i]))
            })
        })
    }
}

struct Store {
    dirpath: PathBuf,
    shards: Vec<ShardFile>,
}

impl Store {
    const MAX_SHARD: u32 = u16::MAX as u32 + 1;

    fn open(dirpath: impl AsRef<Path>) -> Result<Self> {
        let dirpath = dirpath.as_ref().to_path_buf();
        std::fs::create_dir_all(&dirpath)?;
        let first_shard = ShardFile::open(&dirpath, 0, Self::MAX_SHARD)?;
        Ok(Self {
            dirpath,
            shards: vec![first_shard],
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Buf>> {
        let ph = PartedHash::new(key);
        for shard in self.shards.iter() {
            if ph.shard() < shard.end {
                return shard.get(ph, key);
            }
        }
        unreachable!();
    }

    fn remove(&mut self, key: &[u8]) -> Result<bool> {
        let ph = PartedHash::new(key);
        for shard in self.shards.iter_mut() {
            if ph.shard() < shard.end {
                return shard.remove(ph, key);
            }
        }
        unreachable!();
    }

    fn split(&mut self, shard_idx: usize) -> Result<()> {
        let removed_shard = self.shards.remove(shard_idx);

        let start = removed_shard.start;
        let end = removed_shard.end;
        let mid = (removed_shard.start + removed_shard.end) / 2;
        println!("splitting [{start}, {end}) to [{start}, {mid}) and [{mid}, {end})");

        let mut bottom = ShardFile::open(&self.dirpath, start, mid)?;
        let mut top = ShardFile::open(&self.dirpath, mid, end)?;

        for res in removed_shard.iter() {
            let (key, val) = res?;
            let ph = PartedHash::new(&key);
            if ph.shard() < mid {
                bottom.set(ph, &key, &val)?;
            } else {
                top.set(ph, &key, &val)?;
            }
        }

        std::fs::remove_file(self.dirpath.join(format!("{start}-{end}")))?;

        self.shards.push(bottom);
        self.shards.push(top);
        self.shards.sort_by(|x, y| x.end.cmp(&y.end));
        Ok(())
    }

    fn set(&mut self, key: &[u8], val: &[u8]) -> Result<bool> {
        let ph = PartedHash::new(key);
        loop {
            let mut shard_to_split = None;
            for (i, shard) in self.shards.iter_mut().enumerate() {
                if ph.shard() < shard.end {
                    if shard.set(ph, key, val)? {
                        return Ok(true);
                    }
                    shard_to_split = Some(i);
                    break;
                }
            }
            self.split(shard_to_split.unwrap())?;
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = Result<KV>> + 'a {
        self.shards.iter().flat_map(|shard| shard.iter())
    }
}

fn main() -> Result<()> {
    let mut db = Store::open("/tmp/mini-dbdir")?;
    db.set(b"hello", b"world")?;

    println!("{:?}", db.get(b"hello")?);
    println!("{:?}", db.get(b"nonexistent")?);

    db.remove(b"hello")?;
    println!("{:?}", db.get(b"hello")?);

    println!("{}", db.iter().count());

    for i in 0..100_000u32 {
        db.set(&i.to_le_bytes(), &(i * 2).to_le_bytes())?;
    }

    println!("{}", db.iter().count());

    Ok(())
}
