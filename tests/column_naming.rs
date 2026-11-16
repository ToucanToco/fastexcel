#[allow(unused_macros)]
mod utils;

use anyhow::{Context, Result};
use fastexcel::{LoadSheetOrTableOptions, SelectedColumns};
use pretty_assertions::assert_eq;
use rstest::rstest;

use crate::utils::path_for_fixture;

fn sheet_column_names(opts: LoadSheetOrTableOptions) -> Result<Vec<String>> {
    let mut reader = fastexcel::read_excel(path_for_fixture(
        "fixture-single-sheet-duplicated-columns.xlsx",
    ))
    .context("could not read fixture")?;
    let mut sheet = reader
        .load_sheet(0.into(), opts)
        .context("could not load sheet")?;
    Ok(sheet
        .available_columns()
        .context("could not obtain available columns")?
        .into_iter()
        .map(|col| col.name)
        .collect())
}

fn table_column_names(opts: LoadSheetOrTableOptions) -> Result<Vec<String>> {
    let mut reader = fastexcel::read_excel(path_for_fixture("sheet-with-tables.xlsx"))
        .context("could not read fixture")?;
    let mut table = reader
        .load_table("users", opts)
        .context("could not load table")?;
    Ok(table
        .available_columns()
        .context("could not obtain available columns")?
        .into_iter()
        .map(|col| col.name)
        .collect())
}

#[rstest]
#[case::defaults(LoadSheetOrTableOptions::new_for_sheet(), &["col", "col_1", "col_2"])]
#[case::custom_separator(
    LoadSheetOrTableOptions::new_for_sheet().duplicate_column_separator("."),
    &["col", "col.1", "col.2"],
)]
#[case::empty_separator(
    LoadSheetOrTableOptions::new_for_sheet().duplicate_column_separator(""),
    &["col", "col1", "col2"],
)]
#[case::no_header_default_prefix(
    LoadSheetOrTableOptions::new_for_sheet().no_header_row(),
    &["__UNNAMED__0", "__UNNAMED__1", "__UNNAMED__2"],
)]
#[case::custom_prefix(
    LoadSheetOrTableOptions::new_for_sheet().no_header_row().unnamed_column_prefix("column_"),
    &["column_0", "column_1", "column_2"],
)]
#[case::empty_prefix(
    LoadSheetOrTableOptions::new_for_sheet().no_header_row().unnamed_column_prefix(""),
    &["0", "1", "2"],
)]
#[case::generated_names_are_deduplicated(
    LoadSheetOrTableOptions::new_for_sheet()
        .no_header_row()
        .column_names(["1"])
        .unnamed_column_prefix(""),
    &["1", "1_1", "2"],
)]
#[case::collision_chain(
    LoadSheetOrTableOptions::new_for_sheet().column_names(["a", "a", "a_1"]),
    &["a", "a_1", "a_1_1"],
)]
fn test_sheet_column_naming(
    #[case] opts: LoadSheetOrTableOptions,
    #[case] expected: &[&str],
) -> Result<()> {
    assert_eq!(sheet_column_names(opts)?, expected);
    Ok(())
}

#[rstest]
#[case::defaults(
    LoadSheetOrTableOptions::new_for_table().column_names(["a", "a"]),
    &["a", "a_1", "__UNNAMED__2", "__UNNAMED__3"],
)]
#[case::custom_options(
    LoadSheetOrTableOptions::new_for_table()
        .column_names(["a", "a"])
        .duplicate_column_separator("-")
        .unnamed_column_prefix("col"),
    &["a", "a-1", "col2", "col3"],
)]
fn test_table_column_naming(
    #[case] opts: LoadSheetOrTableOptions,
    #[case] expected: &[&str],
) -> Result<()> {
    assert_eq!(table_column_names(opts)?, expected);
    Ok(())
}

#[test]
fn test_table_selected_columns_refers_to_table_header_names() -> Result<()> {
    // The generated name of unselected column 0 ("Column0") clashes with the table header of
    // column 1, which gets renamed. Selection still uses the table header name.
    let mut reader = fastexcel::read_excel(path_for_fixture("table-with-numbered-headers.xlsx"))
        .context("could not read fixture")?;
    let opts = LoadSheetOrTableOptions::new_for_table()
        .selected_columns(SelectedColumns::Selection(vec!["Column0".into()]))
        .unnamed_column_prefix("Column");
    let mut table = reader
        .load_table("numbered", opts)
        .context("could not load table")?;

    let available: Vec<String> = table
        .available_columns()
        .context("could not obtain available columns")?
        .into_iter()
        .map(|col| col.name)
        .collect();
    assert_eq!(available, ["Column0", "Column0_1", "Column2"]);

    let selected: Vec<(String, usize)> = table
        .selected_columns()
        .into_iter()
        .map(|col| (col.name, col.index))
        .collect();
    assert_eq!(selected, [("Column0_1".to_owned(), 1)]);
    Ok(())
}
