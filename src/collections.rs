use simd_itertools::PositionSimd;

use crate::{
    hashing::{PartedHash, INVALID_SIG},
    shard::{KVPair, Shard},
    store::{COLL_NAMESPACE, ITEM_NAMESPACE},
    GetOrCreateStatus, ReplaceStatus, Result, SetStatus, VickyStore,
};

enum SetCollStatus {
    Added,
    BlockFull,
    BlockMissing,
}

const NUM_HASHES_IN_BLOCK: usize = 256;
const COLLECTION_BLOCK: &[u8] = &[0u8; NUM_HASHES_IN_BLOCK * PartedHash::LEN];

pub struct CollectionIterator<'a> {
    store: &'a VickyStore,
    suffix: [u8; PartedHash::LEN + ITEM_NAMESPACE.len()],
    block_idx: u32,
    coll_key: Vec<u8>,
    curr_buf: Option<Vec<u8>>,
    entry_idx: usize,
}

impl<'a> Iterator for CollectionIterator<'a> {
    type Item = Result<KVPair>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.curr_buf.is_none() {
                self.curr_buf = match self.store.get_raw(&self.coll_key) {
                    Err(e) => return Some(Err(e)),
                    Ok(buf) => buf,
                }
            }
            let Some(ref curr_buf) = self.curr_buf else {
                return None;
            };

            let entries = unsafe {
                std::slice::from_raw_parts(curr_buf.as_ptr() as *const u64, NUM_HASHES_IN_BLOCK)
            };
            while self.entry_idx < NUM_HASHES_IN_BLOCK {
                let item_ph = PartedHash::from_u64(entries[self.entry_idx]);
                self.entry_idx += 1;
                if item_ph.signature() == INVALID_SIG {
                    break;
                }

                for res in self.store.get_by_hash(item_ph) {
                    let (mut k, v) = match res {
                        Err(e) => return Some(Err(e)),
                        Ok(kv) => kv,
                    };
                    if k.ends_with(&self.suffix) {
                        k.truncate(k.len() - self.suffix.len());
                        return Some(Ok((k, v)));
                    }
                }
            }

            // move to next block
            self.entry_idx = 0;
            self.curr_buf = None;
            self.block_idx += 1;
            let block_idx_offset = self.coll_key.len() - ITEM_NAMESPACE.len() - size_of::<u32>();
            self.coll_key[block_idx_offset..block_idx_offset + size_of::<u32>()]
                .copy_from_slice(&self.block_idx.to_le_bytes());
        }
    }
}

// XXX:
// * hold number of added entries, so we could start at the right block
// * add number removed entries, and trigger compaction when this number gets to 0.5 of added entries
// * maybe find a way to store these counters in an mmap?
// * may represent the block as "row" that covers a range of hashes. we begin with just one row and when
//   it gets full, we split it into [0..2^31), [2^31..2^32)
// * alternatively, hold a back-pointer from the entry to the block

impl VickyStore {
    fn make_coll_key(&self, coll_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_key = coll_key.to_owned();
        full_key.extend_from_slice(&0u32.to_le_bytes());
        full_key.extend_from_slice(COLL_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &full_key), full_key)
    }

    fn make_item_key(&self, coll_ph: PartedHash, item_key: &[u8]) -> (PartedHash, Vec<u8>) {
        let mut full_key = item_key.to_owned();
        full_key.extend_from_slice(&coll_ph.to_bytes());
        full_key.extend_from_slice(ITEM_NAMESPACE);
        (PartedHash::new(&self.config.hash_seed, &full_key), full_key)
    }

    fn make_item_suffix(
        &self,
        coll_ph: PartedHash,
    ) -> [u8; PartedHash::LEN + ITEM_NAMESPACE.len()] {
        let mut suffix = [0u8; PartedHash::LEN + ITEM_NAMESPACE.len()];
        suffix[..PartedHash::LEN].copy_from_slice(&coll_ph.to_bytes());
        suffix[PartedHash::LEN..].copy_from_slice(ITEM_NAMESPACE);
        suffix
    }

    fn _add_to_collection(&self, mut coll_key: Vec<u8>, item_ph: PartedHash) -> Result<u32> {
        // the first block ("master block") is special:
        // * the first entry holds the number of blocks in this collection
        // * the second entry is made of two u32's, the lower one specified the number of items added
        //   and the higher one, the number of items removed. the collection's len = added - removed.
        //   when `removed > added / 2` we should trigger compaction

        let master_coll_key = coll_key.clone();

        let block_idx_offset = coll_key.len() - (size_of::<u32>() + ITEM_NAMESPACE.len());
        let mut block_idx = 0u32;
        loop {
            coll_key[block_idx_offset..block_idx_offset + size_of::<u32>()]
                .copy_from_slice(&block_idx.to_le_bytes());

            let status = self.operate_on_key_mut(&coll_key, |shard, row, _, idx_kv| {
                if let Some((row_idx, _, v)) = idx_kv {
                    assert_eq!(v.len(), COLLECTION_BLOCK.len());
                    let entries = unsafe {
                        std::slice::from_raw_parts(v.as_ptr() as *const u64, NUM_HASHES_IN_BLOCK)
                    };

                    let mut start = 0;
                    if block_idx == 0 {
                        block_idx = entries[0] as u32;
                        start = 2;
                    }

                    let start = if block_idx == 0 { 1 } else { 0 };
                    if let Some(free_idx) = entries[start..].iter().position_simd(0u64) {
                        let (klen, vlen, offset) =
                            Shard::extract_offset_and_size(row.offsets_and_sizes[row_idx]);
                        assert!(free_idx * PartedHash::LEN < vlen, "free_idx={free_idx}");
                        shard.write_raw(
                            &item_ph.to_bytes(),
                            offset + klen as u64 + (free_idx * PartedHash::LEN) as u64,
                        )?;
                        Ok(SetCollStatus::Added)
                    } else {
                        Ok(SetCollStatus::BlockFull)
                    }
                } else {
                    Ok(SetCollStatus::BlockMissing)
                }
            })?;

            match status {
                SetCollStatus::Added => {
                    break;
                }
                SetCollStatus::BlockFull => {
                    block_idx += 1;
                }
                SetCollStatus::BlockMissing => {
                    self.get_or_create_raw(&coll_key, COLLECTION_BLOCK)?;
                    //self.modify_inplace_raw(&master_coll_key, patch, 0, Some(&0u32.to_le_bytes()))?;
                }
            }
        }

        Ok(0u32)
    }

    pub fn set_in_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<SetStatus> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());
        let (item_ph, item_key) = self.make_item_key(coll_ph, item_key.as_ref());

        // XXX: add a lock table so two threads won't insert the same item at the same time

        let mut val = val.as_ref().to_owned();

        if let Some(curr_buf) = self.get(&item_key)? {
            val.extend_from_slice(&curr_buf[curr_buf.len() - size_of::<u32>()..]);
        } else {
            let block_idx = self._add_to_collection(coll_key, item_ph)?;
            val.extend_from_slice(&block_idx.to_le_bytes());
        }
        self.set_raw(&item_key, val.as_ref())
    }

    pub fn replace_in_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        val: &B3,
    ) -> Result<ReplaceStatus> {
        let (coll_ph, _) = self.make_coll_key(coll_key.as_ref());
        let (_, item_key) = self.make_item_key(coll_ph, item_key.as_ref());

        self.replace_raw(&item_key, val.as_ref())
    }

    pub fn get_or_create_in_collection<
        B1: AsRef<[u8]> + ?Sized,
        B2: AsRef<[u8]> + ?Sized,
        B3: AsRef<[u8]> + ?Sized,
    >(
        &self,
        coll_key: &B1,
        item_key: &B2,
        default_val: &B3,
    ) -> Result<GetOrCreateStatus> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());
        let (item_ph, item_key) = self.make_item_key(coll_ph, item_key.as_ref());

        let res = self.get_or_create_raw(&item_key, default_val.as_ref())?;
        if res.was_created() {
            self._add_to_collection(coll_key, item_ph)?;
        }
        Ok(res)
    }

    pub fn get_from_collection<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        coll_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        let (coll_ph, _) = self.make_coll_key(coll_key.as_ref());
        let (_, item_key) = self.make_item_key(coll_ph, item_key.as_ref());
        self.get_raw(&item_key)
    }

    pub fn remove_from_collection<B1: AsRef<[u8]> + ?Sized, B2: AsRef<[u8]> + ?Sized>(
        &self,
        coll_key: &B1,
        item_key: &B2,
    ) -> Result<Option<Vec<u8>>> {
        /*let (coll_ph, mut coll_key) = self.make_coll_key(coll_key.as_ref());
        let (item_ph, item_key) = self.make_item_key(coll_ph, item_key.as_ref());

        let Some(res) = self.remove_raw(&item_key)? else {
            return Ok(None);
        };

        let block_idx_offset = coll_key.len() - ITEM_NAMESPACE.len() - size_of::<u32>();
        for block_idx in 0u32.. {
            coll_key[block_idx_offset..block_idx_offset + size_of::<u32>()]
                .copy_from_slice(&block_idx.to_le_bytes());

            let found = self.operate_on_key_mut(&coll_key, |shard, row, _, idx_kv| {
                let Some((row_idx, _, v)) = idx_kv else {
                    // block does not exist - end of chain
                    return Ok(true);
                };
                let entries = unsafe {
                    std::slice::from_raw_parts(v.as_ptr() as *const u64, NUM_HASHES_IN_BLOCK)
                };
                if let Some(item_idx) = entries.iter().position_simd(item_ph.as_u64()) {
                    let (klen, vlen, offset) =
                        Shard::extract_offset_and_size(row.offsets_and_sizes[row_idx]);
                    assert!(item_idx * PartedHash::LEN < vlen);
                    shard.write_raw(
                        &[0u8; PartedHash::LEN],
                        offset + klen as u64 + (item_idx * PartedHash::LEN) as u64,
                    )?;
                    Ok(true)
                } else {
                    // try next block
                    Ok(false)
                }
            })?;
            if found {
                break;
            }
        }

        Ok(Some(res))*/
        Ok(None)
    }

    pub fn iter_collection<'a, B: AsRef<[u8]> + ?Sized>(
        &'a self,
        coll_key: &B,
    ) -> CollectionIterator<'a> {
        let (coll_ph, coll_key) = self.make_coll_key(coll_key.as_ref());

        CollectionIterator {
            coll_key,
            block_idx: 0,
            suffix: self.make_item_suffix(coll_ph),
            curr_buf: None,
            store: &self,
            entry_idx: 0,
        }
    }
}
