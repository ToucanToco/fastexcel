use std::fmt::Debug;

use std::{collections::HashSet, sync::OnceLock};

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use calamine::{CellErrorType, CellType, DataType, Range};

use crate::types::python::excelsheet::sheet_data::ExcelSheetData;
use crate::{
    error::{FastExcelErrorKind, FastExcelResult},
    types::{dtype::DTypeMap, python::excelsheet::SelectedColumns},
};

/// All the possible string values that should be considered as NULL
const NULL_STRING_VALUES: [&str; 19] = [
    "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN",
    "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null",
];

fn get_cell_type<DT: CellType + Debug + DataType>(
    data: &Range<DT>,
    row: usize,
    col: usize,
) -> FastExcelResult<ArrowDataType> {
    let cell = data
        .get((row, col))
        .ok_or_else(|| FastExcelErrorKind::CannotRetrieveCellData(row, col))?;

    if cell.is_int() {
        Ok(ArrowDataType::Int64)
    } else if cell.is_float() {
        Ok(ArrowDataType::Float64)
    } else if cell.is_string() {
        if NULL_STRING_VALUES.contains(&cell.get_string().unwrap()) {
            Ok(ArrowDataType::Null)
        } else {
            Ok(ArrowDataType::Utf8)
        }
    } else if cell.is_bool() {
        Ok(ArrowDataType::Boolean)
    } else if cell.is_datetime() {
        // Since calamine 0.24.0, a new ExcelDateTime exists for the Datetime type. It can either be
        // a duration or a datatime
        let excel_datetime = cell
            .get_datetime()
            .expect("calamine indicated that cell is a datetime but get_datetime returned None");
        Ok(if excel_datetime.is_datetime() {
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        } else {
            ArrowDataType::Duration(TimeUnit::Millisecond)
        })
    }
    // These types contain an ISO8601 representation of a date/datetime or a durat
    else if cell.is_datetime_iso() {
        match cell.as_datetime() {
            // If we cannot convert the cell to a datetime, we're working on a date
            Some(_) => Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
            // NOTE: not using the Date64 type on purpose, as pyarrow converts it to a datetime
            // rather than a date
            None => Ok(ArrowDataType::Date32),
        }
    }
    // Simple durations
    else if cell.is_duration_iso() {
        Ok(ArrowDataType::Duration(TimeUnit::Millisecond))
    }
    // Empty cell
    else if cell.is_empty() {
        Ok(ArrowDataType::Null)
    } else if cell.is_error() {
        match cell.get_error() {
            // considering cells with #N/A! as null
            Some(CellErrorType::NA) => Ok(ArrowDataType::Null),
            Some(err) => Err(FastExcelErrorKind::CalamineCellError(err.to_owned()).into()),
            None => Err(FastExcelErrorKind::Internal(format!(
                "cell is an error but get_error returned None: {cell:?}"
            ))
            .into()),
        }
    } else {
        Err(FastExcelErrorKind::Internal(format!("unsupported cell type: {cell:?}")).into())
    }
}

static FLOAT_TYPES_CELL: OnceLock<HashSet<ArrowDataType>> = OnceLock::new();
static INT_TYPES_CELL: OnceLock<HashSet<ArrowDataType>> = OnceLock::new();
static STRING_TYPES_CELL: OnceLock<HashSet<ArrowDataType>> = OnceLock::new();

fn float_types() -> &'static HashSet<ArrowDataType> {
    FLOAT_TYPES_CELL.get_or_init(|| {
        HashSet::from([
            ArrowDataType::Int64,
            ArrowDataType::Float64,
            ArrowDataType::Boolean,
        ])
    })
}

fn int_types() -> &'static HashSet<ArrowDataType> {
    INT_TYPES_CELL.get_or_init(|| HashSet::from([ArrowDataType::Int64, ArrowDataType::Boolean]))
}

fn string_types() -> &'static HashSet<ArrowDataType> {
    STRING_TYPES_CELL.get_or_init(|| {
        HashSet::from([
            ArrowDataType::Int64,
            ArrowDataType::Float64,
            ArrowDataType::Utf8,
        ])
    })
}

fn get_arrow_column_type(
    sheet_data: &ExcelSheetData,
    start_row: usize,
    end_row: usize,
    col: usize,
) -> FastExcelResult<ArrowDataType> {
    let mut column_types = match sheet_data {
        ExcelSheetData::Owned(data) => (start_row..end_row)
            .map(|row| get_cell_type(data, row, col))
            .collect::<FastExcelResult<HashSet<_>>>()?,
        ExcelSheetData::Ref(data) => (start_row..end_row)
            .map(|row| get_cell_type(data, row, col))
            .collect::<FastExcelResult<HashSet<_>>>()?,
    };

    // All columns are nullable anyway so we're not taking Null into account here
    column_types.remove(&ArrowDataType::Null);

    if column_types.is_empty() {
        // If no type apart from NULL was found, it's a NULL column
        Ok(ArrowDataType::Null)
    } else if column_types.len() == 1 {
        // If a single non-null type was found, return it
        Ok(column_types.into_iter().next().unwrap())
    } else if column_types.is_subset(int_types()) {
        // If every cell in the column can be converted to an int, return int64
        Ok(ArrowDataType::Int64)
    } else if column_types.is_subset(float_types()) {
        // If every cell in the column can be converted to a float, return Float64
        Ok(ArrowDataType::Float64)
    } else if column_types.is_subset(string_types()) {
        // If every cell in the column can be converted to a string, return Utf8
        Ok(ArrowDataType::Utf8)
    } else {
        // NOTE: Not being too smart about multi-types columns for now
        Err(
            FastExcelErrorKind::UnsupportedColumnTypeCombination(format!("{column_types:?}"))
                .into(),
        )
    }
}

pub(crate) fn alias_for_name(name: &str, existing_names: &[String]) -> String {
    fn rec(name: &str, existing_names: &[String], depth: usize) -> String {
        let alias = if depth == 0 {
            name.to_owned()
        } else {
            format!("{name}_{depth}")
        };
        match existing_names
            .iter()
            .any(|existing_name| existing_name == &alias)
        {
            true => rec(name, existing_names, depth + 1),
            false => alias,
        }
    }

    rec(name, existing_names, 0)
}

pub(crate) fn arrow_schema_from_column_names_and_range(
    range: &ExcelSheetData,
    column_names: &[String],
    row_idx: usize,
    row_limit: usize,
    selected_columns: &SelectedColumns,
    dtypes: Option<&DTypeMap>,
) -> FastExcelResult<Schema> {
    // clippy suggests to split this type annotation into type declaration, but that would make it
    // less clear IMO
    #[allow(clippy::type_complexity)]
    let arrow_type_for_column: Box<dyn Fn(usize, &String) -> FastExcelResult<ArrowDataType>> =
        match selected_columns {
            // In case all columns are selected, we look up the dtype for the column by name,
            // fallback on a lookup by index, and finally on get_arrow_column_type
            SelectedColumns::All => Box::new(|col_idx, col_name| match dtypes {
                None => get_arrow_column_type(range, row_idx, row_limit, col_idx),
                Some(dts) => {
                    if let Some(dtype_by_name) = dts.dtype_for_col_name(col_name) {
                        Ok(dtype_by_name.into())
                    } else if let Some(dtype_by_idx) = dts.dtype_for_col_idx(col_idx) {
                        Ok(dtype_by_idx.into())
                    } else {
                        get_arrow_column_type(range, row_idx, row_limit, col_idx)
                    }
                }
            }),
            // If columns are selected by name, look up the dtype by name and fallback on
            // get_arrow_column_type
            SelectedColumns::ByName(_) => Box::new(|col_idx, col_name| {
                dtypes
                    .and_then(|dtypes| dtypes.dtype_for_col_name(col_name))
                    .map(|dtype| Ok(dtype.into()))
                    .unwrap_or_else(|| get_arrow_column_type(range, row_idx, row_limit, col_idx))
            }),

            // If columns are selected by index, look up the dtype by name and fallback on
            // get_arrow_column_type
            SelectedColumns::ByIndex(_) => Box::new(|col_idx, _col_name| {
                dtypes
                    .and_then(|dtypes| dtypes.dtype_for_col_idx(col_idx))
                    .map(|dtype| Ok(dtype.into()))
                    .unwrap_or_else(|| get_arrow_column_type(range, row_idx, row_limit, col_idx))
            }),
        };

    let mut fields = Vec::with_capacity(column_names.len());
    let mut existing_names = Vec::with_capacity(column_names.len());

    for (idx, name) in column_names.iter().enumerate() {
        // If we have an index for the given column, extract it and add it to the schema. Otherwise,
        // just ignore it
        if let Some(col_idx) = match selected_columns {
            SelectedColumns::All => Some(idx),
            _ => selected_columns.idx_for_column(column_names, name, idx),
        } {
            let col_type = arrow_type_for_column(col_idx, name)?;
            let aliased_name = alias_for_name(name, &existing_names);
            fields.push(Field::new(&aliased_name, col_type, true));
            existing_names.push(aliased_name);
        }
    }

    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use calamine::{Cell, Data, DataRef};
    use rstest::{fixture, rstest};

    use super::*;

    #[fixture]
    fn range_data() -> ExcelSheetData<'static> {
        Range::from_sparse(vec![
            // First column
            Cell::new((0, 0), Data::Bool(true)),
            Cell::new((1, 0), Data::Bool(false)),
            Cell::new((2, 0), Data::String("NULL".to_string())),
            Cell::new((3, 0), Data::Int(42)),
            Cell::new((4, 0), Data::Float(13.37)),
            Cell::new((5, 0), Data::String("hello".to_string())),
            Cell::new((6, 0), Data::Empty),
            Cell::new((7, 0), Data::String("#N/A".to_string())),
            Cell::new((8, 0), Data::Int(12)),
            Cell::new((9, 0), Data::Float(12.21)),
            Cell::new((10, 0), Data::Bool(true)),
            Cell::new((11, 0), Data::Int(1337)),
        ])
        .into()
    }

    #[fixture]
    fn range_data_ref() -> ExcelSheetData<'static> {
        Range::from_sparse(vec![
            // First column
            Cell::new((0, 0), DataRef::Bool(true)),
            Cell::new((1, 0), DataRef::Bool(false)),
            Cell::new((2, 0), DataRef::SharedString("NULL")),
            Cell::new((3, 0), DataRef::Int(42)),
            Cell::new((4, 0), DataRef::Float(13.37)),
            Cell::new((5, 0), DataRef::SharedString("hello")),
            Cell::new((6, 0), DataRef::Empty),
            Cell::new((7, 0), DataRef::SharedString("#N/A")),
            Cell::new((8, 0), DataRef::Int(12)),
            Cell::new((9, 0), DataRef::Float(12.21)),
            Cell::new((10, 0), DataRef::Bool(true)),
            Cell::new((11, 0), DataRef::Int(1337)),
        ])
        .into()
    }

    #[rstest]
    // pure bool
    #[case(0, 2, ArrowDataType::Boolean)]
    // pure int
    #[case(3, 4, ArrowDataType::Int64)]
    // pure float
    #[case(4, 5, ArrowDataType::Float64)]
    // pure string
    #[case(5, 6, ArrowDataType::Utf8)]
    // pure int + float
    #[case(3, 5, ArrowDataType::Float64)]
    // null + int + float
    #[case(2, 5, ArrowDataType::Float64)]
    // float + string
    #[case(4, 6, ArrowDataType::Utf8)]
    // int + float + string
    #[case(3, 6, ArrowDataType::Utf8)]
    // null + int + float + string + empty + null
    #[case(2, 8, ArrowDataType::Utf8)]
    // empty + null + int
    #[case(6, 9, ArrowDataType::Int64)]
    // int + float + null
    #[case(7, 10, ArrowDataType::Float64)]
    // int + float + bool + null
    #[case(7, 11, ArrowDataType::Float64)]
    // int + bool
    #[case(10, 12, ArrowDataType::Int64)]
    fn get_arrow_column_type_multi_dtype_ok(
        range_data: ExcelSheetData<'_>,
        range_data_ref: ExcelSheetData<'_>,
        #[case] start_row: usize,
        #[case] end_row: usize,
        #[case] expected: ArrowDataType,
    ) {
        assert_eq!(
            get_arrow_column_type(&range_data, start_row, end_row, 0).unwrap(),
            expected
        );
        assert_eq!(
            get_arrow_column_type(&range_data_ref, start_row, end_row, 0).unwrap(),
            expected
        );
    }
}
