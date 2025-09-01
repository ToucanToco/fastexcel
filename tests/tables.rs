use anyhow::{Context, Result};
use chrono::NaiveDate;
use fastexcel::LoadSheetOrTableOptions;
use pretty_assertions::assert_eq;
use rstest::{fixture, rstest};

use crate::utils::path_for_fixture;

#[macro_use]
mod utils;

#[fixture]
fn reader() -> fastexcel::ExcelReader {
    fastexcel::read_excel(path_for_fixture("sheet-with-tables.xlsx"))
        .expect("could not read excel file")
}

#[rstest]
#[case::all_sheets(None, vec!["users"])]
#[case::sheet_with_tables(Some("sheet1"), vec!["users"])]
#[case::sheet_without_tables(Some("sheet2"), vec![])]
fn test_table_names(
    mut reader: fastexcel::ExcelReader,
    #[case] sheet_name: Option<&str>,
    #[case] expected: Vec<&str>,
) -> Result<()> {
    let table_names = reader
        .table_names(sheet_name)
        .context("Failed to get table names")?;
    assert_eq!(table_names, expected);
    Ok(())
}

#[rstest]
fn test_load_table(mut reader: fastexcel::ExcelReader) -> Result<()> {
    let mut table = reader
        .load_table("users", LoadSheetOrTableOptions::new_for_table())
        .context("Failed to load table")?;

    assert_eq!(table.name(), "users");
    assert_eq!(table.sheet_name(), "sheet1");
    assert!(table.specified_dtypes().is_none());
    assert_eq!(table.total_height(), 3);
    assert_eq!(table.offset(), 0);
    assert_eq!(table.height(), 3);
    assert_eq!(table.width(), 4);
    let available_columns = table
        .available_columns()
        .context("could not obtain available columns for table")?;
    let expected_column_info = vec![
        fastexcel::ColumnInfo {
            name: "User Id".into(),
            index: 0,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "FirstName".into(),
            index: 1,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "LastName".into(),
            index: 2,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "Date".into(),
            index: 3,
            dtype: fastexcel::DType::DateTime,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];
    assert_eq!(available_columns, expected_column_info);

    let dates = [
        NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2024, 5, 4)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        NaiveDate::from_ymd_opt(2025, 2, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    ];

    let expected_columns = fe_columns!(
        "User Id" => [1.0, 2.0, 5.0],
        "FirstName" => ["Peter", "John", "Hans"],
        "LastName" => ["Müller", "Meier", "Fricker"],
        "Date" => dates.as_slice(),
    );

    let table_columns = table
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(table_columns, expected_columns);

    #[cfg(feature = "polars")]
    {
        use polars_core::df;

        let expected_df = df!(
            "User Id" => [1.0, 2.0, 5.0],
            "FirstName" => ["Peter", "John", "Hans"],
            "LastName" => ["Müller", "Meier", "Fricker"],
            "Date" => dates.as_slice(),
        )?;

        let df = table
            .to_polars()
            .context("could not convert table to polars dataframe")?;
        assert!(df.equals_missing(&expected_df))
    }

    Ok(())
}
