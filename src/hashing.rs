use siphasher::sip128::{Hash128, SipHasher24};

use crate::Result;

#[derive(Debug, Clone, Copy)]
pub struct HashSeed([u8; 16]);

/// A struct that represents a "nonce" for seeding the hash function (keyed hash).
/// Keeping it secret is only meaningful if you're concerned with DoS attacks
impl HashSeed {
    pub const LEN: usize = size_of::<Self>();

    pub fn new(bytes: [u8; Self::LEN]) -> Self {
        Self(bytes)
    }

    /// Construct a HashSeed from the given byte buffer (must be 16 bytes in length)
    pub fn from_buf<B: AsRef<[u8]> + ?Sized>(key: &B) -> Result<Self> {
        Ok(Self(key.as_ref().try_into()?))
    }
}

use bytemuck::{Pod, Zeroable};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Hash)]
#[repr(transparent)]
pub(crate) struct PartedHash(u64);

impl std::fmt::Display for PartedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04x}.{:04x}.{:08x}",
            self.shard_selector(),
            self.row_selector(),
            self.signature()
        )
    }
}

pub(crate) const INVALID_SIG: u32 = 0;

#[cfg(feature = "whitebox_testing")]
pub static mut HASH_BITS_TO_KEEP: u64 = u64::MAX; // which bits to keep from the hash - for testing collisions

impl PartedHash {
    pub fn new(seed: &HashSeed, buf: &[u8]) -> Self {
        Self::from_hash(SipHasher24::new_with_key(&seed.0).hash(buf))
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.signature() != INVALID_SIG
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

    pub fn as_u64(&self) -> u64 {
        self.0
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
        let val = shard | row | (sig as u64);

        #[cfg(feature = "whitebox_testing")]
        let val = (val & unsafe { HASH_BITS_TO_KEEP }) | 1 /* make sure sig != 0 */;

        Self(val)
    }
}

#[test]
fn test_parted_hash() -> Result<()> {
    use bytemuck::{bytes_of, from_bytes};

    HashSeed::from_buf("12341234123412341").expect_err("shouldn't work");

    let seed = HashSeed::from_buf("aaaabbbbccccdddd")?;

    let h1 = PartedHash::new(&seed, b"hello world");
    assert_eq!(h1.0, 13445180190757400308,);
    let h2 = PartedHash(13445180190757400308);
    assert_eq!(PartedHash::new(&seed, b"hello world"), h2);

    let h3 = PartedHash(0x1020304050607080);
    assert_eq!(
        bytes_of(&h3),
        [0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10]
    );
    let h4: PartedHash = *from_bytes(&[0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10]);
    assert_eq!(h4, h3);

    Ok(())
}
