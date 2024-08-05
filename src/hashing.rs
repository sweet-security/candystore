use std::hash::Hasher;

use siphasher::sip128::{Hash128, Hasher128, SipHasher24};

use crate::{Result, VickyError};

#[derive(Debug, Clone, Copy)]
pub struct SecretKey([u8; 16]);

/// A struct that represents a "nonce" for seeding the hash function (keyed hash).
/// Keeping it secret is only meaningful if you're concerned with DoS attacks
impl SecretKey {
    pub const LEN: usize = size_of::<Self>();

    /// Construct a SecretKey from the given byte buffer (must be 16 bytes in length)
    ///
    pub fn new<B: AsRef<[u8]> + ?Sized>(key: &B) -> Result<Self> {
        let key = key.as_ref();
        if key.len() != Self::LEN {
            return Err(Box::new(VickyError::WrongSecretKeyLength));
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
pub(crate) const USER_NAMESPACE: u8 = 1;
//pub(crate) const TYPED_NAMESPACE: u8 = 2;

impl PartedHash {
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
    pub fn from_buffer(namespace: u8, key: &SecretKey, buf: &[u8]) -> Self {
        // maybe use blake3?
        let mut hasher = SipHasher24::new_with_key(&key.0);
        hasher.write_u8(namespace);
        hasher.write(buf);
        Self::from_hash(hasher.finish128())
    }

    #[allow(dead_code)]
    pub fn builder(key: &SecretKey) -> PartedHashBuilder {
        PartedHashBuilder(SipHasher24::new_with_key(&key.0))
    }

    #[allow(dead_code)]
    pub fn to_u64(&self) -> u64 {
        ((self.shard_selector as u64) << 48)
            | ((self.row_selector as u64) << 32)
            | (self.signature as u64)
    }
}

#[allow(dead_code)]
pub(crate) struct PartedHashBuilder(SipHasher24);

impl PartedHashBuilder {
    #[allow(dead_code)]
    pub fn write(mut self, bytes: &[u8]) -> Self {
        self.0.write(bytes);
        self
    }
    #[allow(dead_code)]
    pub fn write_u32(mut self, v: u32) -> Self {
        self.0.write_u32(v);
        self
    }
    #[allow(dead_code)]
    pub fn write_u8(mut self, v: u8) -> Self {
        self.0.write_u8(v);
        self
    }
    #[allow(dead_code)]
    pub fn finish(self) -> PartedHash {
        PartedHash::from_hash(self.0.finish128())
    }
}

#[test]
fn test_parted_hash() -> Result<()> {
    SecretKey::new("1234").expect_err("shouldn't work");
    SecretKey::new("12341234123412341").expect_err("shouldn't work");

    let key = SecretKey::new("aaaabbbbccccdddd")?;

    assert_eq!(
        PartedHash::from_buffer(USER_NAMESPACE, &key, b"hello world").to_u64(),
        12143172433256666175,
    );

    assert_eq!(
        PartedHash::builder(&key)
            .write_u8(USER_NAMESPACE)
            .write(b"hello world")
            .finish()
            .to_u64(),
        12143172433256666175,
    );

    Ok(())
}
