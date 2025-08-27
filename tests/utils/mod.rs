pub fn path_for_fixture(fixture_file: &str) -> String {
    format!(
        "{}/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        fixture_file
    )
}

#[macro_export]
macro_rules! fe_column {
    ($name:expr, $vec:expr) => {
        fastexcel::FastExcelColumn::try_new($name.into(), $vec.into(), None)
            .context("Failed to create column")
    };
}
