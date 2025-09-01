#[allow(unused_macros)]
mod utils;

use anyhow::{Context, Result};
use fastexcel::{LoadSheetOrTableOptions, SheetVisible};
use pretty_assertions::assert_matches;

use crate::utils::path_for_fixture;

#[test]
fn sheet_visibility() -> Result<()> {
    let mut reader = fastexcel::read_excel(path_for_fixture(
        "fixture-sheets-different-visibilities.xlsx",
    ))
    .context("could not read excel file")?;

    let sheet_0 = reader.load_sheet(0.into(), LoadSheetOrTableOptions::new_for_sheet())?;
    let sheet_1 = reader.load_sheet(1.into(), LoadSheetOrTableOptions::new_for_sheet())?;
    let sheet_2 = reader.load_sheet(2.into(), LoadSheetOrTableOptions::new_for_sheet())?;

    assert_matches!(sheet_0.visible(), SheetVisible::Visible);
    assert_matches!(sheet_1.visible(), SheetVisible::Hidden);
    assert_matches!(sheet_2.visible(), SheetVisible::VeryHidden);

    Ok(())
}
