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
pub(crate) struct PartedHash(u64);

pub(crate) const INVALID_SIG: u32 = 0;

impl PartedHash {
    pub const LEN: usize = size_of::<u64>();

    pub fn new(seed: &HashSeed, buf: &[u8]) -> Self {
        Self::from_hash(SipHasher24::new_with_key(&seed.0).hash(buf))
    }

    #[inline]
    pub fn shard_selector(&self) -> u16 {
        (self.0 >> 48) as u16
    }
    #[inline]
    pub fn row_selector(&self) -> u16 {
        (self.0 >> 32) as u16
    }
    #[inline]
    pub fn signature(&self) -> u32 {
        self.0 as u32
    }

    fn from_hash(h: Hash128) -> Self {
        let mut sig = h.h1 as u32;
        if sig == INVALID_SIG {
            sig = h.h2 as u32;
            if sig == INVALID_SIG {
                sig = (h.h2 >> 32) as u32;
                if sig == INVALID_SIG {
                    sig = 0x6052_c9b7; // this is so unlikely that it doesn't really matter
                }
            }
        }
        let shard = h.h1 & 0xffff_0000_0000_0000;
        let row = h.h1 & 0x0000_ffff_0000_0000;
        Self(shard | row | sig as u64)
    }

    pub fn to_bytes(&self) -> [u8; Self::LEN] {
        self.0.to_le_bytes()
    }
    pub fn as_u64(&self) -> u64 {
        self.0
    }
    pub fn from_u64(val: u64) -> Self {
        Self(val)
    }
}

#[test]
fn test_parted_hash() -> Result<()> {
    HashSeed::new("12341234123412341").expect_err("shouldn't work");

    let seed = HashSeed::new("aaaabbbbccccdddd")?;

    let h1 = PartedHash::new(&seed, b"hello world");
    assert_eq!(h1.as_u64(), 13445180190757400308,);
    let h2 = PartedHash::from_u64(13445180190757400308);
    assert_eq!(PartedHash::new(&seed, b"hello world"), h2);

    let h3 = PartedHash::from_u64(0x1020304050607080);
    assert_eq!(
        h3.to_bytes(),
        [0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10]
    );

    Ok(())
}
