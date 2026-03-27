use candystore::Config;

#[allow(dead_code)]
pub fn small_file_config() -> Config {
    Config {
        max_data_file_size: 16 * 1024,
        ..Config::default()
    }
}
