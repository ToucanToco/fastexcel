pub fn path_for_fixture(fixture_file: &str) -> String {
    format!(
        "{}/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        fixture_file
    )
}

macro_rules! fe_column {
    ($name:expr, $vec_or_arr:expr) => {
        fastexcel::FastExcelColumn::try_new($name.into(), $vec_or_arr.into(), None)
            .context("Failed to create column")
    };
}

macro_rules! fe_columns {
    // (name => []) Any number of times but at least once, optionally followed by a comma
    ($($name:expr => $vec_or_arr:expr),+ $(,)?) => {
        vec![
            $(fe_column!($name, $vec_or_arr)?),+
        ]
    };
}
