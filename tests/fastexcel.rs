#[macro_use]
mod utils;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use fastexcel::{FastExcelColumn, LoadSheetOrTableOptions, SkipRows};
#[cfg(feature = "polars")]
use polars_core::{df, frame::DataFrame};
use pretty_assertions::assert_eq;
use rstest::rstest;
use utils::path_for_fixture;

#[test]
fn test_single_sheet() -> Result<()> {
    let mut reader = fastexcel::read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
        .context("could not read excel file")?;

    assert_eq!(reader.sheet_names(), vec!["January"]);
    let mut sheet_by_name = reader
        .load_sheet("January".into(), LoadSheetOrTableOptions::new_for_sheet())
        .context("could not load sheet by name")?;
    let mut sheet_by_idx = reader
        .load_sheet(0.into(), LoadSheetOrTableOptions::new_for_sheet())
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
    let expected_columns = fe_columns!(
        "Month" => [1.0, 2.0],
        "Year" => [2019.0, 2020.0],
    );
    assert_eq!(&columns_by_name, &expected_columns);

    #[cfg(feature = "polars")]
    {
        let df_by_name = sheet_by_name
            .to_polars()
            .context("could not convert sheet by name to DataFrame")?;
        let df_by_idx = sheet_by_idx
            .to_polars()
            .context("could not convert sheet by index to DataFrame")?;
        let expected_df = df!(
            "Month" => [1.0, 2.0],
            "Year" => [2019.0, 2020.0]
        )
        .context("could not create expected DataFrame")?;
        assert_eq!(&df_by_name, &df_by_idx);
        assert!(df_by_name.equals_missing(&expected_df));
    }

    Ok(())
}

#[test]
fn test_single_sheet_bytes() -> Result<()> {
    let bytes = std::fs::read(path_for_fixture("fixture-single-sheet.xlsx"))?;

    let mut reader = fastexcel::ExcelReader::try_from(bytes.as_slice())
        .context("could not create reader from bytes")?;

    assert_eq!(reader.sheet_names(), vec!["January"]);
    let mut sheet_by_name = reader
        .load_sheet("January".into(), LoadSheetOrTableOptions::new_for_sheet())
        .context("could not load sheet by name")?;
    let mut sheet_by_idx = reader
        .load_sheet(0.into(), LoadSheetOrTableOptions::new_for_sheet())
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
    let expected_columns = fe_columns!(
        "Month" => [1.0, 2.0],
        "Year" => [2019.0, 2020.0]
    );
    assert_eq!(&columns_by_name, &expected_columns);

    #[cfg(feature = "polars")]
    {
        let df_by_name = sheet_by_name
            .to_polars()
            .context("could not convert sheet by name to DataFrame")?;
        let df_by_idx = sheet_by_idx
            .to_polars()
            .context("could not convert sheet by index to DataFrame")?;
        let expected_df = df!(
            "Month" => [1.0, 2.0],
            "Year" => [2019.0, 2020.0]
        )
        .context("could not create expected DataFrame")?;
        assert_eq!(&df_by_name, &df_by_idx);
        assert!(df_by_name.equals_missing(&expected_df));
    }

    Ok(())
}

#[test]
fn test_single_sheet_with_types() -> Result<()> {
    let mut excel_reader =
        fastexcel::read_excel(path_for_fixture("fixture-single-sheet-with-types.xlsx"))
            .context("could not read excel file")?;

    let mut sheet = excel_reader
        .load_sheet(0.into(), LoadSheetOrTableOptions::new_for_sheet())
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

    let expected_columns = fe_columns!(
        "__UNNAMED__0" => [0.0, 1.0, 2.0],
        "bools" => [true, false, true],
        "dates" => [naive_date; 3],
        "floats" => [12.35, 42.69, 1234567.0],
    );
    assert_eq!(&columns, &expected_columns);

    #[cfg(feature = "polars")]
    {
        let df = sheet
            .to_polars()
            .context("could not convert sheet to DataFrame")?;
        let expected_df = df!(
            "__UNNAMED__0" => [0.0, 1.0, 2.0],
            "bools" => [true, false, true],
            "dates" => [naive_date; 3],
            "floats" => [12.35, 42.69, 1234567.0],
        )
        .context("could not create expected DataFrame")?;

        assert!(df.equals_missing(&expected_df));
    }

    Ok(())
}

#[test]
fn test_multiple_sheets() -> Result<()> {
    let mut excel_reader = fastexcel::read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))
        .context("could not read excel file")?;

    let sheet_0 = excel_reader
        .load_sheet(0.into(), LoadSheetOrTableOptions::new_for_sheet())
        .context("could not load sheet 0 by idx")?;
    let expected_columns_sheet_0 = fe_columns!("Month" => [1.0], "Year" => [2019.0]);
    let sheet_0_columns = sheet_0
        .to_columns()
        .context("could not convert sheet 0 to columns")?;
    assert_eq!(sheet_0_columns, expected_columns_sheet_0);

    let sheet_1 = excel_reader
        .load_sheet(1.into(), LoadSheetOrTableOptions::new_for_sheet())
        .context("could not load sheet 1 by idx")?;
    let expected_columns_sheet_1 =
        fe_columns!("Month" => [2.0, 3.0, 4.0], "Year" => [2019.0, 2021.0, 2022.0]);
    let sheet_1_columns = sheet_1
        .to_columns()
        .context("could not convert sheet 1 to columns")?;
    assert_eq!(sheet_1_columns, expected_columns_sheet_1);

    let sheet_unnamed_columns = excel_reader
        .load_sheet(
            "With unnamed columns".into(),
            LoadSheetOrTableOptions::new_for_sheet(),
        )
        .context("could not load sheet \"With unnamed columns\" by idx")?;
    let expected_columns_sheet_unnamed_columns = fe_columns!(
        "col1" => [2.0, 3.0],
        "__UNNAMED__1" => [1.5, 2.5],
        "col3" => ["hello", "world"],
        "__UNNAMED__3" => [-5.0, -6.0],
        "col5" => ["a", "b"],
    );
    let sheet_unnamed_columns_columns = sheet_unnamed_columns
        .to_columns()
        .context("could not convert sheet \"With unnamed columns\" to columns")?;

    assert_eq!(
        sheet_unnamed_columns_columns,
        expected_columns_sheet_unnamed_columns
    );

    #[cfg(feature = "polars")]
    {
        let expected_df_sheet_0 = df!("Month" => [1.0], "Year" => [2019.0])?;
        let df_sheet_0 = sheet_0
            .to_polars()
            .context("could not convert sheet 0 to DataFrame")?;
        assert!(expected_df_sheet_0.equals_missing(&df_sheet_0));

        let expected_df_sheet_1 =
            df!("Month" => [2.0, 3.0, 4.0], "Year" => [2019.0, 2021.0, 2022.0])?;
        let df_sheet_1 = sheet_1
            .to_polars()
            .context("could not convert sheet 1 to DataFrame")?;
        assert!(expected_df_sheet_1.equals_missing(&df_sheet_1));

        let expected_df_sheet_unnamed_columns = df!(
            "col1" => [2.0, 3.0],
            "__UNNAMED__1" => [1.5, 2.5],
            "col3" => ["hello", "world"],
            "__UNNAMED__3" => [-5.0, -6.0],
            "col5" => ["a", "b"],
        )?;
        let df_sheet_unnamed_columns = sheet_unnamed_columns
            .to_polars()
            .context("could not convert sheet \"With unnamed columns\" to DataFrame")?;
        assert!(expected_df_sheet_unnamed_columns.equals_missing(&df_sheet_unnamed_columns));
    }

    Ok(())
}

#[test]
fn test_sheet_with_header_row_diff_from_zero() -> Result<()> {
    let mut excel_reader =
        fastexcel::read_excel(path_for_fixture("fixture-changing-header-location.xlsx"))
            .context("could not read excel file")?;

    assert_eq!(
        excel_reader.sheet_names(),
        vec!["Sheet1", "Sheet2", "Sheet3"]
    );

    let mut sheet_by_name = excel_reader
        .load_sheet(
            "Sheet1".into(),
            LoadSheetOrTableOptions::new_for_sheet().header_row(1),
        )
        .context("could not load sheet \"Sheet1\" by name")?;

    let mut sheet_by_idx = excel_reader
        .load_sheet(
            0.into(),
            LoadSheetOrTableOptions::new_for_sheet().header_row(1),
        )
        .context("could not load sheet 0 by index")?;

    assert_eq!(sheet_by_name.name(), sheet_by_idx.name());
    assert_eq!(sheet_by_name.name(), "Sheet1");

    assert_eq!(sheet_by_name.height(), sheet_by_idx.height());
    assert_eq!(sheet_by_name.height(), 2);

    assert_eq!(sheet_by_name.width(), sheet_by_idx.width());
    assert_eq!(sheet_by_name.width(), 2);

    let expected_columns = fe_columns!(
        "Month" => [1.0, 2.0],
        "Year" => [2019.0, 2020.0]
    );

    let columns_by_name = sheet_by_name
        .to_columns()
        .context("could not convert sheet \"Sheet1\" to columns")?;
    let columns_by_idx = sheet_by_idx
        .to_columns()
        .context("could not convert sheet 0 to columns")?;
    assert_eq!(&columns_by_name, &columns_by_idx);
    assert_eq!(&columns_by_name, &expected_columns);

    #[cfg(feature = "polars")]
    {
        let df_by_name = sheet_by_name
            .to_polars()
            .context("could not convert sheet \"Sheet1\" to DataFrame")?;
        let df_by_idx = sheet_by_idx
            .to_polars()
            .context("could not convert sheet 0 to DataFrame")?;
        let expected_df = df!(
            "Month" => [1.0, 2.0],
            "Year" => [2019.0, 2020.0]
        )?;

        assert!(df_by_name.equals_missing(&df_by_idx));
        assert!(expected_df.equals_missing(&df_by_name));
    }

    Ok(())
}

#[test]
fn test_sheet_with_pagination_and_without_headers() -> Result<()> {
    let mut excel_reader =
        fastexcel::read_excel(path_for_fixture("fixture-single-sheet-with-types.xlsx"))
            .context("could not read excel file")?;

    let opts = LoadSheetOrTableOptions::new_for_sheet()
        .n_rows(1)
        .skip_rows(SkipRows::Simple(1))
        .no_header_row()
        .column_names(["This", "Is", "Amazing", "Stuff"]);
    let mut sheet = excel_reader
        .load_sheet(0.into(), opts)
        .context("could not load sheet 0")?;

    assert_eq!(sheet.name(), "Sheet1");
    assert_eq!(sheet.height(), 1);
    assert_eq!(sheet.width(), 4);

    let naive_dt = NaiveDate::from_ymd_opt(2022, 3, 2)
        .unwrap()
        .and_hms_opt(5, 43, 4)
        .unwrap();

    let expected_columns = fe_columns!(
        "This" => [0.0],
        "Is" => [true],
        "Amazing" => [naive_dt],
        "Stuff" => [12.35],
    );

    let sheet_columns = sheet
        .to_columns()
        .context("could not convert sheet to columns")?;
    assert_eq!(&sheet_columns, &expected_columns);

    #[cfg(feature = "polars")]
    {
        let df = sheet
            .to_polars()
            .context("could not convert sheet to DataFrame")?;
        let expected_df = df!(
            "This" => [0.0],
            "Is" => [true],
            "Amazing" => [naive_dt],
            "Stuff" => [12.35],
        )?;

        assert!(df.equals_missing(&expected_df));
    }

    Ok(())
}

#[rstest]
#[case(Some(0), SkipRows::SkipEmptyRowsAtBeginning, fe_columns!("a" => ["b", "c", "d", "e", "f"], "0" => [1.0, 2.0, 3.0, 4.0, 5.0]))]
#[case(
    None,
    SkipRows::Simple(0),
    fe_columns!(
        "__UNNAMED__0" => [None, None, Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [None, None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )
)]
#[case(
    None,
    SkipRows::SkipEmptyRowsAtBeginning,
    fe_columns!(
        "__UNNAMED__0" => ["a", "b", "c", "d", "e", "f"],
        "__UNNAMED__1" => [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    )
)]
#[case(
    Some(0),
    SkipRows::Simple(0),
    fe_columns!(
        "__UNNAMED__0" => [None, Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )
)]
#[case(
    Some(0),
    SkipRows::Simple(1),
    fe_columns!(
        "__UNNAMED__0" => [Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )
)]
#[case(
    None,
    SkipRows::Simple(2),
    fe_columns!(
        "__UNNAMED__0" => [Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )
)]
#[case(
    None,
    SkipRows::Simple(3),
    fe_columns!(
        "__UNNAMED__0" => [Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )
)]
#[case(
    Some(1),
    SkipRows::Simple(0),
    fe_columns!("__UNNAMED__0" => ["a", "b", "c", "d", "e", "f"], "__UNNAMED__1" => [0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
)]
#[case(Some(2), SkipRows::Simple(0), fe_columns!("a" => ["b", "c", "d", "e", "f"], "0" => [1.0, 2.0, 3.0, 4.0, 5.0]))]
#[case(
    Some(2),
    SkipRows::SkipEmptyRowsAtBeginning,
    fe_columns!("a" => ["b", "c", "d", "e", "f"], "0" => [1.0, 2.0, 3.0, 4.0, 5.0])
)]
fn test_header_row_and_skip_rows(
    #[case] header_row: Option<usize>,
    #[case] skip_rows: SkipRows,
    #[case] expected: Vec<FastExcelColumn>,
) -> Result<()> {
    let mut excel_reader = fastexcel::read_excel(path_for_fixture("no-header.xlsx"))
        .context("could not read excel file")?;

    let opts = LoadSheetOrTableOptions {
        header_row,
        skip_rows,
        ..LoadSheetOrTableOptions::new_for_sheet()
    };
    let sheet = excel_reader
        .load_sheet(0.into(), opts)
        .context("could not load sheet 0")?;

    let sheet_columns = sheet
        .to_columns()
        .context("could not convert sheet to columns")?;
    assert_eq!(&sheet_columns, &expected);
    Ok(())
}

#[cfg(feature = "polars")]
#[rstest]
#[case(Some(0), SkipRows::SkipEmptyRowsAtBeginning, df!("a" => ["b", "c", "d", "e", "f"], "0" => [1.0, 2.0, 3.0, 4.0, 5.0])?)]
#[case(
    None,
    SkipRows::Simple(0),
    df!(
        "__UNNAMED__0" => [None, None, Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [None, None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )?
)]
#[case(
    None,
    SkipRows::SkipEmptyRowsAtBeginning,
    df!(
        "__UNNAMED__0" => ["a", "b", "c", "d", "e", "f"],
        "__UNNAMED__1" => [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    )?
)]
#[case(
    Some(0),
    SkipRows::Simple(0),
    df!(
        "__UNNAMED__0" => [None, Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )?
)]
#[case(
    Some(0),
    SkipRows::Simple(1),
    df!(
        "__UNNAMED__0" => [Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )?
)]
#[case(
    None,
    SkipRows::Simple(2),
    df!(
        "__UNNAMED__0" => [Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )?
)]
#[case(
    None,
    SkipRows::Simple(3),
    df!(
        "__UNNAMED__0" => [Some("b"), Some("c"), Some("d"), Some("e"), Some("f")],
        "__UNNAMED__1" => [Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
    )?
)]
#[case(
    Some(1),
    SkipRows::Simple(0),
    df!("__UNNAMED__0" => ["a", "b", "c", "d", "e", "f"], "__UNNAMED__1" => [0.0, 1.0, 2.0, 3.0, 4.0, 5.0])?
)]
#[case(Some(2), SkipRows::Simple(0), df!("a" => ["b", "c", "d", "e", "f"], "0" => [1.0, 2.0, 3.0, 4.0, 5.0])?)]
#[case(
    Some(2),
    SkipRows::SkipEmptyRowsAtBeginning,
    df!("a" => ["b", "c", "d", "e", "f"], "0" => [1.0, 2.0, 3.0, 4.0, 5.0])?
)]
fn test_header_row_and_skip_rows_polars(
    #[case] header_row: Option<usize>,
    #[case] skip_rows: SkipRows,
    #[case] expected: DataFrame,
) -> Result<()> {
    let mut excel_reader = fastexcel::read_excel(path_for_fixture("no-header.xlsx"))
        .context("could not read excel file")?;

    let opts = LoadSheetOrTableOptions {
        header_row,
        skip_rows,
        ..LoadSheetOrTableOptions::new_for_sheet()
    };
    let sheet = excel_reader
        .load_sheet(0.into(), opts)
        .context("could not load sheet 0")?;

    let df = sheet
        .to_polars()
        .context("could not convert sheet to DataFrame")?;

    assert!(df.equals_missing(&expected));

    Ok(())
}
