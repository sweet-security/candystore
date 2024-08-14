use databuf::{Decode, Encode};
use std::fmt::{Display, Formatter};
use std::io::Write;
use uuid::{Bytes, Uuid};

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EncodableUuid(Uuid);

impl From<Uuid> for EncodableUuid {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<EncodableUuid> for Uuid {
    fn from(value: EncodableUuid) -> Self {
        value.0
    }
}

impl Encode for EncodableUuid {
    fn encode<const CONFIG: u16>(&self, c: &mut (impl Write + ?Sized)) -> std::io::Result<()> {
        self.0.as_bytes().encode::<CONFIG>(c)
    }
}

impl Decode<'_> for EncodableUuid {
    fn decode<const CONFIG: u16>(c: &mut &'_ [u8]) -> databuf::Result<Self> {
        Ok(Self(Uuid::from_bytes(Bytes::decode::<CONFIG>(c)?)))
    }
}

impl Display for EncodableUuid {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}
