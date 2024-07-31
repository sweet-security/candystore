#![feature(btree_cursors)]

mod hashing;
mod shard;
mod store;

pub use hashing::SecretKey;
pub use shard::Config;
pub use store::{Stats, VickyStore};

#[derive(Debug)]
pub enum Error {
    WrongSecretKeyLength,
    KeyTooLong,
    ValueTooLong,
    IOError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
