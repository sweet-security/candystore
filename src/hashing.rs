use std::hash::Hasher;

use siphasher::sip128::SipHasher24;

use crate::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub struct SecretKey([u8; 16]);

impl SecretKey {
    pub const LEN: usize = size_of::<Self>();

    pub fn new<B: AsRef<[u8]> + ?Sized>(key: &B) -> Result<Self> {
        let key = key.as_ref();
        if key.len() != Self::LEN {
            return Err(Error::WrongSecretKeyLength);
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

impl PartedHash {
    pub fn from_buffer(namespace: u8, key: &SecretKey, buf: &[u8]) -> Self {
        // maybe use blake3?
        let mut hasher = SipHasher24::new_with_key(&key.0);
        hasher.write_u8(namespace);
        hasher.write(buf);
        let h = hasher.hash(buf);
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
}

#[test]
fn test_parted_hash() -> Result<()> {
    assert!(matches!(
        SecretKey::new("1234"),
        Err(Error::WrongSecretKeyLength)
    ));
    assert!(matches!(
        SecretKey::new("12341234123412341"),
        Err(Error::WrongSecretKeyLength)
    ));
    let key = SecretKey::new("aaaabbbbccccdddd")?;

    assert_eq!(
        PartedHash::from_buffer(USER_NAMESPACE, &key, b"hello world"),
        PartedHash {
            shard_selector: 62379,
            row_selector: 17802,
            signature: 3217405680
        }
    );
    Ok(())
}
