#![cfg(not(feature = "__pyo3-tests"))]

#[macro_use]
mod utils;
use anyhow::{Context, Result};
use chrono::NaiveDate;
use pretty_assertions::assert_eq;

use utils::path_for_fixture;

#[test]
fn test_single_sheet() -> Result<()> {
    let mut reader = fastexcel::read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
        .context("could not read excel file")?;

    assert_eq!(reader.sheet_names(), vec!["January"]);
    let mut sheet_by_name = reader
        .load_sheet("January".into(), Default::default())
        .context("could not load sheet by name")?;
    let mut sheet_by_idx = reader
        .load_sheet(0.into(), Default::default())
        .context("could not load sheet by index")?;

    assert_eq!(sheet_by_name.name(), sheet_by_idx.name());
    assert_eq!(sheet_by_name.name(), "January");

    assert_eq!(sheet_by_name.height(), sheet_by_idx.height());
    assert_eq!(sheet_by_name.height(), 2);

    assert_eq!(sheet_by_name.width(), sheet_by_idx.width());
    assert_eq!(sheet_by_name.width(), 2);

    let columns_by_name = sheet_by_name
        .to_columns()
        .context("could not convert sheet by name to columns")?;
    let columns_by_idx = sheet_by_idx
        .to_columns()
        .context("could not convert sheet by index to columns")?;

    assert_eq!(&columns_by_name, &columns_by_idx);
    let expected_columns = vec![
        fe_column!("Month", vec![Some(1.0), Some(2.0)])?,
        fe_column!("Year", vec![Some(2019.0), Some(2020.0)])?,
    ];
    assert_eq!(&columns_by_name, &expected_columns);

    Ok(())
}

#[test]
fn test_single_sheet_bytes() -> Result<()> {
    let bytes = std::fs::read(path_for_fixture("fixture-single-sheet.xlsx"))?;

    let mut reader = fastexcel::ExcelReader::try_from(bytes.as_slice())
        .context("could not create reader from bytes")?;

    assert_eq!(reader.sheet_names(), vec!["January"]);
    let mut sheet_by_name = reader
        .load_sheet("January".into(), Default::default())
        .context("could not load sheet by name")?;
    let mut sheet_by_idx = reader
        .load_sheet(0.into(), Default::default())
        .context("could not load sheet by index")?;

    assert_eq!(sheet_by_name.name(), sheet_by_idx.name());
    assert_eq!(sheet_by_name.name(), "January");

    assert_eq!(sheet_by_name.height(), sheet_by_idx.height());
    assert_eq!(sheet_by_name.height(), 2);

    assert_eq!(sheet_by_name.width(), sheet_by_idx.width());
    assert_eq!(sheet_by_name.width(), 2);

    let columns_by_name = sheet_by_name
        .to_columns()
        .context("could not convert sheet by name to columns")?;
    let columns_by_idx = sheet_by_idx
        .to_columns()
        .context("could not convert sheet by index to columns")?;

    assert_eq!(&columns_by_name, &columns_by_idx);
    let expected_columns = vec![
        fe_column!("Month", vec![Some(1.0), Some(2.0)])?,
        fe_column!("Year", vec![Some(2019.0), Some(2020.0)])?,
    ];
    assert_eq!(&columns_by_name, &expected_columns);

    Ok(())
}

#[test]
fn test_single_sheet_with_types() -> Result<()> {
    let mut excel_reader =
        fastexcel::read_excel(path_for_fixture("fixture-single-sheet-with-types.xlsx"))
            .context("could not read excel file")?;

    let mut sheet = excel_reader
        .load_sheet(0.into(), Default::default())
        .context("could not load sheet")?;

    assert_eq!(sheet.name(), "Sheet1");
    assert_eq!(sheet.height(), sheet.total_height());
    assert_eq!(sheet.height(), 3);
    assert_eq!(sheet.width(), 4);

    let columns = sheet
        .to_columns()
        .context("could not convert sheet by name to columns")?;

    let naive_date = NaiveDate::from_ymd_opt(2022, 3, 2)
        .unwrap()
        .and_hms_opt(5, 43, 4)
        .unwrap();

    let expected_columns = vec![
        fe_column!("__UNNAMED__0", vec![Some(0.0), Some(1.0), Some(2.0)])?,
        fe_column!("bools", vec![Some(true), Some(false), Some(true)])?,
        fe_column!("dates", [Some(naive_date); 3].to_vec())?,
        fe_column!("floats", vec![Some(12.35), Some(42.69), Some(1234567.0)])?,
    ];
    assert_eq!(&columns, &expected_columns);

    Ok(())
}
