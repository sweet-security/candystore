use siphasher::sip128::{Hash128, SipHasher24};

use crate::{Result, VickyError};

#[derive(Debug, Clone, Copy)]
pub struct HashSeed([u8; 16]);

/// A struct that represents a "nonce" for seeding the hash function (keyed hash).
/// Keeping it secret is only meaningful if you're concerned with DoS attacks
impl HashSeed {
    pub const LEN: usize = size_of::<Self>();

    /// Construct a HashSeed from the given byte buffer (must be 16 bytes in length)
    pub fn new<B: AsRef<[u8]> + ?Sized>(key: &B) -> Result<Self> {
        let key = key.as_ref();
        if key.len() != Self::LEN {
            return Err(Box::new(VickyError::WrongHashSeedLength));
        }
        let mut bytes = [0u8; Self::LEN];
        bytes.copy_from_slice(&key);
        Ok(Self(bytes))
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartedHash {
    pub shard_selector: u16,
    pub row_selector: u16,
    pub signature: u32,
}

pub(crate) const INVALID_SIG: u32 = 0;

pub(crate) const USER_NAMESPACE: &[u8] = &[1];
pub(crate) const TYPED_NAMESPACE: &[u8] = &[2];

impl PartedHash {
    #[allow(dead_code)]
    pub const LEN: usize = size_of::<u64>();

    pub fn new(seed: &HashSeed, buf: &[u8]) -> Self {
        Self::from_hash(SipHasher24::new_with_key(&seed.0).hash(buf))
    }

    fn from_hash(h: Hash128) -> Self {
        let mut signature = h.h1 as u32;
        if signature == INVALID_SIG {
            signature = h.h2 as u32;
            if signature == INVALID_SIG {
                signature = (h.h2 >> 32) as u32;
                if signature == INVALID_SIG {
                    signature = 0x6052_c9b7; // this is so unlikely that it doesn't really matter
                }
            }
        }
        Self {
            shard_selector: (h.h1 >> 48) as u16,
            row_selector: (h.h1 >> 32) as u16,
            signature,
        }
    }

    #[cfg(test)]
    pub fn to_u64(&self) -> u64 {
        ((self.shard_selector as u64) << 48)
            | ((self.row_selector as u64) << 32)
            | (self.signature as u64)
    }

    // pub fn from_u64(val: u64) -> Self {
    //     Self {
    //         shard_selector: (val >> 48) as u16,
    //         row_selector: (val >> 32) as u16,
    //         signature: val as u32,
    //     }
    // }
    // pub fn as_bytes(&self) -> [u8; Self::LEN] {
    //     self.to_u64().to_le_bytes()
    // }
    // pub fn from_bytes(b: &[u8]) -> Self {
    //     assert_eq!(b.len(), Self::LEN);
    //     let buf: [u8; 8] = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
    //     Self::from_u64(u64::from_le_bytes(buf))
    // }
}

#[test]
fn test_parted_hash() -> Result<()> {
    HashSeed::new("1234").expect_err("shouldn't work");
    HashSeed::new("12341234123412341").expect_err("shouldn't work");

    let seed = HashSeed::new("aaaabbbbccccdddd")?;

    assert_eq!(
        PartedHash::new(&seed, b"hello world").to_u64(),
        13445180190757400308,
    );

    Ok(())
}
