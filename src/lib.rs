#![feature(btree_cursors)]

mod hashing;
mod shard;
mod store;

pub use hashing::SecretKey;
pub use shard::Config;
use std::fmt::{Display, Formatter};
pub use store::{Stats, VickyStore};

#[derive(Debug)]
pub enum Error {
    WrongSecretKeyLength,
    KeyTooLong,
    ValueTooLong,
    KeyNotFound,
    IOError(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Error::WrongSecretKeyLength => write!(f, "wrong secret length"),
            Error::KeyTooLong => write!(f, "key too long"),
            Error::KeyNotFound => write!(f, "key not found"),
            Error::ValueTooLong => write!(f, "value too long"),
            Error::IOError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
