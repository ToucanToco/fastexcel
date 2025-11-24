#[macro_use]
mod utils;

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use fastexcel::{ExcelReader, LoadSheetOrTableOptions};
use pretty_assertions::assert_eq;
use rstest::{fixture, rstest};

use crate::utils::path_for_fixture;

#[fixture]
fn reader() -> ExcelReader {
    fastexcel::read_excel(path_for_fixture("sheet-and-table-with-whitespace.xlsx"))
        .expect("could not read fixture")
}

const DATES: &[Option<NaiveDateTime>] = &[
    Some(
        NaiveDate::from_ymd_opt(2025, 11, 19)
            .unwrap()
            .and_hms_opt(14, 34, 2)
            .unwrap(),
    ),
    Some(
        NaiveDate::from_ymd_opt(2025, 11, 20)
            .unwrap()
            .and_hms_opt(14, 56, 34)
            .unwrap(),
    ),
    Some(
        NaiveDate::from_ymd_opt(2025, 11, 21)
            .unwrap()
            .and_hms_opt(15, 19, 6)
            .unwrap(),
    ),
    None,
    Some(
        NaiveDate::from_ymd_opt(2025, 11, 22)
            .unwrap()
            .and_hms_opt(15, 41, 38)
            .unwrap(),
    ),
    Some(
        NaiveDate::from_ymd_opt(2025, 11, 23)
            .unwrap()
            .and_hms_opt(16, 4, 10)
            .unwrap(),
    ),
    None,
    None,
    None,
    None,
];

#[rstest]
fn test_skip_tail_rows_behavior(mut reader: ExcelReader) -> Result<()> {
    let expected_columns_with_whitespace = fe_columns!(
        // String because the last row contains a space
        "Column One" => [Some("1"), Some("2"), Some("3"), None, Some("5"), None, None, None, None, Some(" ")],
        "Column Two" => [Some("one"), Some("two"), None, Some("four"), Some("five"), None, None, Some(""), None, None],
        "Column Three" => DATES,
    );
    let expected_columns_without_whitespace = fe_columns!(
        // Not string rows -> float
        "Column One" => [Some(1.0), Some(2.0), Some(3.0), None, Some(5.0), None],
        "Column Two" => [Some("one"), Some("two"), None, Some("four"), Some("five"), None],
        "Column Three" => &DATES[0..6],
    );

    let sheet = reader
        .load_sheet(
            "Without Table".into(),
            LoadSheetOrTableOptions::new_for_sheet(),
        )
        .context(r#"could not load sheet "Without Table""#)?;
    let sheet_columns = sheet
        .to_columns()
        .context("could not convert sheet to columns")?;
    assert_eq!(sheet_columns, expected_columns_with_whitespace);

    let table = reader
        .load_table(
            "Table_with_whitespace",
            LoadSheetOrTableOptions::new_for_table(),
        )
        .context(r#"could not load table "Table_with_whitespace""#)?;
    let table_columns = table
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(table_columns, expected_columns_with_whitespace);

    let sheet_without_tail_whitespace = reader
        .load_sheet(
            "Without Table".into(),
            LoadSheetOrTableOptions::new_for_sheet().skip_whitespace_tail_rows(true),
        )
        .context(r#"could not load sheet "Without Table""#)?;
    let sheet_without_tail_whitespace_columns = sheet_without_tail_whitespace
        .to_columns()
        .context("could not convert sheet to columns")?;
    assert_eq!(
        sheet_without_tail_whitespace_columns,
        expected_columns_without_whitespace
    );

    let table_without_tail_whitespace = reader
        .load_table(
            "Table_with_whitespace",
            LoadSheetOrTableOptions::new_for_table().skip_whitespace_tail_rows(true),
        )
        .context(r#"could not load table "Table_with_whitespace""#)?;
    let table_columns_without_tail_whitespace = table_without_tail_whitespace
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(
        table_columns_without_tail_whitespace,
        expected_columns_without_whitespace
    );

    Ok(())
}

#[rstest]
fn test_skip_tail_rows_and_whitespace_as_null_behavior(mut reader: ExcelReader) -> Result<()> {
    let expected_columns_with_whitespace_as_null = fe_columns!(
        // All rows should be taken into account but the space in the last row should be considered null
        "Column One" => [Some(1.0), Some(2.0), Some(3.0), None, Some(5.0), None, None, None, None, None],
        // All rows should be taken into account but the empty string in 8th row should be considered null
        "Column Two" => [Some("one"), Some("two"), None, Some("four"), Some("five"), None, None, None, None, None],
        "Column Three" => DATES,
    );
    let expected_columns_without_whitespace = fe_columns!(
        "Column One" => [Some(1.0), Some(2.0), Some(3.0), None, Some(5.0), None],
        "Column Two" => [Some("one"), Some("two"), None, Some("four"), Some("five"), None],
        "Column Three" => &DATES[0..6],
    );

    let sheet = reader
        .load_sheet(
            "Without Table".into(),
            LoadSheetOrTableOptions::new_for_sheet().whitespace_as_null(true),
        )
        .context(r#"could not load sheet "Without Table""#)?;
    let sheet_columns = sheet
        .to_columns()
        .context("could not convert sheet to columns")?;
    assert_eq!(sheet_columns, expected_columns_with_whitespace_as_null);

    let table = reader
        .load_table(
            "Table_with_whitespace",
            LoadSheetOrTableOptions::new_for_table().whitespace_as_null(true),
        )
        .context(r#"could not load table "Table_with_whitespace""#)?;
    let table_columns = table
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(table_columns, expected_columns_with_whitespace_as_null);

    let sheet_without_tail_whitespace = reader
        .load_sheet(
            "Without Table".into(),
            LoadSheetOrTableOptions::new_for_sheet()
                .whitespace_as_null(true)
                .skip_whitespace_tail_rows(true),
        )
        .context(r#"could not load sheet "Without Table""#)?;
    let sheet_without_tail_whitespace_columns = sheet_without_tail_whitespace
        .to_columns()
        .context("could not convert sheet to columns")?;
    assert_eq!(
        sheet_without_tail_whitespace_columns,
        expected_columns_without_whitespace
    );

    let table_without_tail_whitespace = reader
        .load_table(
            "Table_with_whitespace",
            LoadSheetOrTableOptions::new_for_table()
                .whitespace_as_null(true)
                .skip_whitespace_tail_rows(true),
        )
        .context(r#"could not load table "Table_with_whitespace""#)?;
    let table_columns_without_tail_whitespace = table_without_tail_whitespace
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(
        table_columns_without_tail_whitespace,
        expected_columns_without_whitespace
    );

    Ok(())
}
