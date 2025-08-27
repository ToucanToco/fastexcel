pub fn path_for_fixture(fixture_file: &str) -> String {
    format!(
        "{}/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        fixture_file
    )
}
