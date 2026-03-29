use std::hash::Hasher;

use candystore::Config;

#[allow(dead_code)]
pub fn small_file_config() -> Config {
    Config {
        max_data_file_size: 16 * 1024,
        ..Config::default()
    }
}

#[allow(dead_code)]
pub fn checkpoint_slot_checksum(generation: u64, ordinal: u64, offset: u64) -> u64 {
    let mut hasher = siphasher::sip::SipHasher13::new();
    hasher.write_u64(generation);
    hasher.write_u64(ordinal);
    hasher.write_u64(offset);
    hasher.finish()
}
