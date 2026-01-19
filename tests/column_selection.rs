use anyhow::{Context, Result};
use fastexcel::{DType, DTypes, IdxOrName, LoadSheetOrTableOptions, SelectedColumns};
use pretty_assertions::assert_eq;
use rstest::{fixture, rstest};
use std::collections::HashMap;

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

#[fixture]
fn reader_with_offset() -> fastexcel::ExcelReader {
    fastexcel::read_excel(path_for_fixture("sheet-and-table-with-offset.xlsx"))
        .expect("could not read excel file")
}

#[rstest]
fn test_use_column_range_with_offset_with_table_and_specified_dtypes(
    mut reader_with_offset: fastexcel::ExcelReader,
) -> Result<()> {
    let dtypes_map: HashMap<IdxOrName, DType> = [
        (IdxOrName::Idx(3), DType::Int),
        (IdxOrName::Name("Column at E5".to_owned()), DType::String),
    ]
    .into_iter()
    .collect();

    let selected_columns_closed = "D:E"
        .parse::<SelectedColumns>()
        .context("could not parse column selection")?;

    let opts_closed_range = LoadSheetOrTableOptions::new_for_table()
        .selected_columns(selected_columns_closed)
        .with_dtypes(DTypes::Map(dtypes_map.clone()));

    let table_closed = reader_with_offset
        .load_table("TableAtD5", opts_closed_range)
        .context("Failed to load table with closed range")?;

    let selected_columns_open_ended = "D:"
        .parse::<SelectedColumns>()
        .context("could not parse column selection")?;

    let opts_open_ended_range = LoadSheetOrTableOptions::new_for_table()
        .selected_columns(selected_columns_open_ended)
        .with_dtypes(DTypes::Map(dtypes_map.clone()));

    let table_open_ended = reader_with_offset
        .load_table("TableAtD5", opts_open_ended_range)
        .context("Failed to load table with open-ended range")?;

    assert_eq!(table_closed.name(), "TableAtD5");
    assert_eq!(table_open_ended.name(), "TableAtD5");

    let expected_selected_columns = vec![
        fastexcel::ColumnInfo {
            name: "Column at D5".to_owned(),
            index: 0,
            absolute_index: 3,
            dtype: fastexcel::DType::Int,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::ProvidedByIndex,
        },
        fastexcel::ColumnInfo {
            name: "Column at E5".to_owned(),
            index: 1,
            absolute_index: 4,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::ProvidedByName,
        },
    ];
    assert_eq!(table_closed.selected_columns(), expected_selected_columns);
    assert_eq!(
        table_open_ended.selected_columns(),
        expected_selected_columns
    );

    let expected_columns = fe_columns!(
        "Column at D5" => [1_i64, 2, 3, 4],
        "Column at E5" => ["4", "5", "6", "8"],
    );

    assert_eq!(
        table_closed
            .to_columns()
            .context("could not convert table to columns")?,
        expected_columns
    );

    assert_eq!(
        table_open_ended
            .to_columns()
            .context("could not convert table to columns")?,
        expected_columns
    );

    #[cfg(feature = "polars")]
    {
        use polars_core::df;

        let expected_df = df!(
            "Column at D5" => [1_i64, 2, 3, 4],
            "Column at E5" => ["4", "5", "6", "8"],
        )?;

        let df_closed = table_closed
            .to_polars()
            .context("could not convert table to polars dataframe")?;
        let df_open_ended = table_open_ended
            .to_polars()
            .context("could not convert table to polars dataframe")?;

        assert!(df_closed.equals_missing(&expected_df));
        assert!(df_open_ended.equals_missing(&expected_df));
    }

    Ok(())
}

/// This test ensures that index-based selection is correctly resolved when used with an offset
/// table: the selected indices should be absolute, and it should be able to handle both index-based
/// and name-based selection.
#[rstest]
fn test_use_column_names_with_offset_table_by_index_and_name(
    mut reader_with_offset: fastexcel::ExcelReader,
) -> Result<()> {
    let selected_columns = SelectedColumns::Selection(vec![
        IdxOrName::Name("Column at D5".to_string()),
        IdxOrName::Idx(4),
    ]);

    let opts = LoadSheetOrTableOptions::new_for_table().selected_columns(selected_columns);

    let table = reader_with_offset
        .load_table("TableAtD5", opts)
        .context("Failed to load table")?;

    assert_eq!(table.name(), "TableAtD5");

    let expected_selected_columns = vec![
        fastexcel::ColumnInfo {
            name: "Column at D5".to_owned(),
            index: 0,
            absolute_index: 3,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "Column at E5".to_owned(),
            index: 1,
            absolute_index: 4,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::Provided,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];

    let selected_columns_info = table.selected_columns();
    assert_eq!(selected_columns_info, expected_selected_columns);

    let expected_columns = fe_columns!(
        "Column at D5" => [1.0, 2.0, 3.0, 4.0],
        "Column at E5" => [4.0, 5.0, 6.0, 8.0],
    );

    let table_columns = table
        .to_columns()
        .context("could not convert table to columns")?;
    assert_eq!(table_columns, expected_columns);

    #[cfg(feature = "polars")]
    {
        use polars_core::df;

        let expected_df = df!(
            "Column at D5" => [1.0, 2.0, 3.0, 4.0],
            "Column at E5" => [4.0, 5.0, 6.0, 8.0],
        )?;

        let df = table
            .to_polars()
            .context("could not convert table to polars dataframe")?;
        assert!(df.equals_missing(&expected_df))
    }

    Ok(())
}

#[rstest]
fn test_use_column_range_with_offset_with_sheet_and_specified_dtypes(
    mut reader_with_offset: fastexcel::ExcelReader,
) -> Result<()> {
    // Create dtypes map: {7: "int", "Column at I10": "string"}
    // Note: Column H is at index 7, Column I is at index 8, Column K is at index 10
    let dtypes_map: HashMap<IdxOrName, DType> = [
        (IdxOrName::Idx(7), DType::Int),
        (IdxOrName::Name("Column at I10".to_owned()), DType::String),
    ]
    .into_iter()
    .collect();

    let selected_columns_closed = "H:K"
        .parse::<SelectedColumns>()
        .context("could not parse column selection")?;

    let opts_closed_range = LoadSheetOrTableOptions::new_for_sheet()
        .header_row(9)
        .selected_columns(selected_columns_closed)
        .with_dtypes(DTypes::Map(dtypes_map.clone()));

    let sheet_closed = reader_with_offset
        .load_sheet("without-table".into(), opts_closed_range)
        .context("Failed to load sheet with closed range")?;

    let selected_columns_open_ended = "H:"
        .parse::<SelectedColumns>()
        .context("could not parse column selection")?;

    let opts_open_ended_range = LoadSheetOrTableOptions::new_for_sheet()
        .header_row(9)
        .selected_columns(selected_columns_open_ended)
        .with_dtypes(DTypes::Map(dtypes_map.clone()));

    let sheet_open_ended = reader_with_offset
        .load_sheet("without-table".into(), opts_open_ended_range)
        .context("Failed to load sheet with open-ended range")?;

    assert_eq!(sheet_closed.name(), "without-table");
    assert_eq!(sheet_open_ended.name(), "without-table");

    let expected_selected_columns = vec![
        fastexcel::ColumnInfo {
            name: "Column at H10".to_owned(),
            index: 0,
            absolute_index: 7,
            dtype: fastexcel::DType::Int,
            column_name_from: fastexcel::ColumnNameFrom::LookedUp,
            dtype_from: fastexcel::DTypeFrom::ProvidedByIndex,
        },
        fastexcel::ColumnInfo {
            name: "Column at I10".to_owned(),
            index: 1,
            absolute_index: 8,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::LookedUp,
            dtype_from: fastexcel::DTypeFrom::ProvidedByName,
        },
        fastexcel::ColumnInfo {
            name: "__UNNAMED__2".to_owned(),
            index: 2,
            absolute_index: 9,
            dtype: fastexcel::DType::String,
            column_name_from: fastexcel::ColumnNameFrom::Generated,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
        fastexcel::ColumnInfo {
            name: "Column at K10".to_owned(),
            index: 3,
            absolute_index: 10,
            dtype: fastexcel::DType::Float,
            column_name_from: fastexcel::ColumnNameFrom::LookedUp,
            dtype_from: fastexcel::DTypeFrom::Guessed,
        },
    ];
    assert_eq!(sheet_closed.selected_columns(), &expected_selected_columns);
    assert_eq!(
        sheet_open_ended.selected_columns(),
        &expected_selected_columns
    );

    let expected_columns = fe_columns!(
        "Column at H10" => [1_i64, 2, 3],
        "Column at I10" => ["4", "5", "6"],
        "__UNNAMED__2" => [Option::<&str>::None, None, None],
        "Column at K10" => [7.0, 8.0, 9.0],
    );

    assert_eq!(
        sheet_closed
            .to_columns()
            .context("could not convert sheet to columns")?,
        expected_columns
    );

    assert_eq!(
        sheet_open_ended
            .to_columns()
            .context("could not convert sheet to columns")?,
        expected_columns
    );

    #[cfg(feature = "polars")]
    {
        use polars_core::df;

        let expected_df = df!(
            "Column at H10" => [1_i64, 2, 3],
            "Column at I10" => ["4", "5", "6"],
            "__UNNAMED__2" => [Option::<&str>::None, None, None],
            "Column at K10" => [7.0, 8.0, 9.0],
        )?;

        let df_closed = sheet_closed
            .to_polars()
            .context("could not convert sheet to polars dataframe")?;
        let df_open_ended = sheet_open_ended
            .to_polars()
            .context("could not convert sheet to polars dataframe")?;

        assert!(df_closed.equals_missing(&expected_df));
        assert!(df_open_ended.equals_missing(&expected_df));
    }

    Ok(())
}
