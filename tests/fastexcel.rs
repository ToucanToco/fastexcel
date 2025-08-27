mod utils;
use anyhow::{Context, Result};
use pretty_assertions::assert_eq;

use crate::utils::path_for_fixture;

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
    Ok(())
}
