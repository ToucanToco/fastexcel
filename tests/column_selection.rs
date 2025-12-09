use anyhow::{Context, Result};
use fastexcel::{IdxOrName, LoadSheetOrTableOptions, SelectedColumns};
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
fn test_use_columns_with_table(mut reader: fastexcel::ExcelReader) -> Result<()> {
    let selected_columns = SelectedColumns::Selection(vec![
        IdxOrName::Name("User Id".to_string()),
        IdxOrName::Name("FirstName".to_string()),
    ]);

    let opts = LoadSheetOrTableOptions::new_for_table().selected_columns(selected_columns);

    let mut table = reader
        .load_table("users", opts)
        .context("Failed to load table")?;

    assert_eq!(table.name(), "users");
    assert_eq!(table.width(), 4);
    assert_eq!(table.height(), 3);

    let available_columns = table
        .available_columns()
        .context("could not obtain available columns for table")?;
    let expected_available_columns = vec![
        fastexcel::ColumnInfo {
            name: "User Id".into(),
            index: 0,
            absolute_index: 0,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "FirstName".into(),
            index: 1,
            absolute_index: 1,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "__UNNAMED__2".into(),
            index: 2,
            absolute_index: 2,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Generated,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "__UNNAMED__3".into(),
            index: 3,
            absolute_index: 3,
            dtype: fastexcel::DType::DateTime,
            column_name_from: fastexcel::ColumnNameFrom::Generated,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];
    assert_eq!(available_columns, expected_available_columns);

    let selected_columns_info = table.selected_columns();
    let expected_selected_columns = vec![
        fastexcel::ColumnInfo {
            name: "User Id".into(),
            index: 0,
            absolute_index: 0,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "FirstName".into(),
            index: 1,
            absolute_index: 1,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];
    assert_eq!(selected_columns_info, expected_selected_columns);

    let expected_columns = fe_columns!(
        "User Id" => [1.0, 2.0, 5.0],
        "FirstName" => ["Peter", "John", "Hans"],
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
        )?;

        let df = table
            .to_polars()
            .context("could not convert table to polars dataframe")?;
        assert!(df.equals_missing(&expected_df))
    }

    Ok(())
}

#[rstest]
fn test_use_columns_with_table_and_provided_columns(
    mut reader: fastexcel::ExcelReader,
) -> Result<()> {
    let selected_columns = SelectedColumns::Selection(vec![0.into(), 2.into()]);

    let opts = LoadSheetOrTableOptions::new_for_table()
        .column_names(vec!["user_id", "last_name"])
        .selected_columns(selected_columns);

    let mut table = reader
        .load_table("users", opts)
        .context("Failed to load table")?;

    assert_eq!(table.name(), "users");
    assert_eq!(table.width(), 4);
    assert_eq!(table.height(), 3);

    let available_columns = table
        .available_columns()
        .context("could not obtain available columns for table")?;
    let expected_available_columns = vec![
        fastexcel::ColumnInfo {
            name: "user_id".into(),
            index: 0,
            absolute_index: 0,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "__UNNAMED__1".into(),
            index: 1,
            absolute_index: 1,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Generated,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "last_name".into(),
            index: 2,
            absolute_index: 2,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "__UNNAMED__3".into(),
            index: 3,
            absolute_index: 3,
            dtype: fastexcel::DType::DateTime,
            column_name_from: fastexcel::ColumnNameFrom::Generated,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];
    assert_eq!(available_columns, expected_available_columns);

    let selected_columns_info = table.selected_columns();
    let expected_selected_columns = vec![
        fastexcel::ColumnInfo {
            name: "user_id".into(),
            index: 0,
            absolute_index: 0,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "last_name".into(),
            index: 2,
            absolute_index: 2,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];
    assert_eq!(selected_columns_info, expected_selected_columns);

    let expected_columns = fe_columns!(
        "user_id" => [1.0, 2.0, 5.0],
        "last_name" => ["Müller", "Meier", "Fricker"],
    );

    let table_columns = table
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(table_columns, expected_columns);

    #[cfg(feature = "polars")]
    {
        use polars_core::df;

        let expected_df = df!(
            "user_id" => [1.0, 2.0, 5.0],
            "last_name" => ["Müller", "Meier", "Fricker"],
        )?;

        let df = table
            .to_polars()
            .context("could not convert table to polars dataframe")?;
        assert!(df.equals_missing(&expected_df))
    }

    Ok(())
}
