use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, DurationMillisecondArray, Float64Array, Int64Array,
    NullArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{Field, Schema};
use calamine::{CellType, DataType, Range};

use super::cell_extractors;
use crate::{
    data::{ExcelSheetData, RowSelector, generate_row_selector},
    error::{ErrorContext, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::DType,
        excelsheet::{CellError, CellErrors, SkipRows, column_info::ColumnInfo},
    },
};

mod with_error_impls {
    use super::*;

    pub(crate) fn create_boolean_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(BooleanArray::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else if let Some(b) = cell_extractors::extract_boolean(cell) {
                    Some(b)
                } else {
                    cell_errors.push(CellError {
                        position: (row, col),
                        row_offset: offset,
                        detail: format!("Expected boolean but got '{cell:?}"),
                    });
                    None
                }
            })
        })));

        (arr, cell_errors)
    }

    pub(crate) fn create_int_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(Int64Array::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else {
                    match cell_extractors::extract_int(cell) {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected int but got '{cell:?}'"),
                            });
                            None
                        }
                    }
                }
            })
        })));
        (arr, cell_errors)
    }

    pub(crate) fn create_float_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(Float64Array::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else {
                    match cell_extractors::extract_float(cell) {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected float but got '{cell:?}'"),
                            });
                            None
                        }
                    }
                }
            })
        })));
        (arr, cell_errors)
    }

    pub(crate) fn create_string_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(StringArray::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else {
                    match cell_extractors::extract_string(cell) {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected string but got '{cell:?}'"),
                            });
                            None
                        }
                    }
                }
            })
        })));

        (arr, cell_errors)
    }

    pub(crate) fn create_date_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(Date32Array::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else {
                    match cell_extractors::extract_date_as_num_days(cell) {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected date but got '{:?}'", cell),
                            });
                            None
                        }
                    }
                }
            })
        })));

        (arr, cell_errors)
    }

    pub(crate) fn create_datetime_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];
        let arr = Arc::new(TimestampMillisecondArray::from_iter((offset..limit).map(
            |row| {
                data.get((row, col)).and_then(|cell| {
                    if cell.is_empty() {
                        None
                    } else {
                        match cell_extractors::extract_datetime_as_timestamp_ms(cell) {
                            Some(value) => Some(value),
                            None => {
                                cell_errors.push(CellError {
                                    position: (row, col),
                                    row_offset: offset,
                                    detail: format!("Expected datetime but got '{:?}'", cell),
                                });
                                None
                            }
                        }
                    }
                })
            },
        )));
        (arr, cell_errors)
    }

    pub(crate) fn create_duration_array_with_errors<CT: CellType + DataType + Debug>(
        data: &Range<CT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];
        let arr = Arc::new(DurationMillisecondArray::from_iter((offset..limit).map(
            |row| {
                data.get((row, col)).and_then(|cell| {
                    if cell.is_empty() {
                        None
                    } else {
                        match cell_extractors::extract_duration_as_ms(cell) {
                            Some(value) => Some(value),
                            None => {
                                cell_errors.push(CellError {
                                    position: (row, col),
                                    row_offset: offset,
                                    detail: format!("Expected duration but got '{cell:?}'"),
                                });
                                None
                            }
                        }
                    }
                })
            },
        )));
        (arr, cell_errors)
    }
}

pub(crate) fn create_boolean_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(BooleanArray::from_iter(row_iter.map(|row| {
        data.get((row, col))
            .and_then(cell_extractors::extract_boolean)
    })))
}

pub(crate) fn create_int_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(Int64Array::from_iter(row_iter.map(|row| {
        data.get((row, col)).and_then(cell_extractors::extract_int)
    })))
}

pub(crate) fn create_float_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(Float64Array::from_iter(row_iter.map(|row| {
        data.get((row, col))
            .and_then(cell_extractors::extract_float)
    })))
}

pub(crate) fn create_string_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(StringArray::from_iter(row_iter.map(|row| {
        data.get((row, col))
            .and_then(cell_extractors::extract_string)
    })))
}

pub(crate) fn create_date_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(Date32Array::from_iter(row_iter.map(|row| {
        data.get((row, col))
            .and_then(cell_extractors::extract_date_as_num_days)
    })))
}

pub(crate) fn create_datetime_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(TimestampMillisecondArray::from_iter(row_iter.map(|row| {
        data.get((row, col))
            .and_then(cell_extractors::extract_datetime_as_timestamp_ms)
    })))
}

pub(crate) fn create_duration_array<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    row_iter: impl Iterator<Item = usize>,
) -> Arc<dyn Array> {
    Arc::new(DurationMillisecondArray::from_iter(row_iter.map(|row| {
        data.get((row, col))
            .and_then(cell_extractors::extract_duration_as_ms)
    })))
}

macro_rules! create_array_function_with_errors {
    ($func_name:ident) => {
        pub(crate) fn $func_name(
            data: &ExcelSheetData,
            col: usize,
            offset: usize,
            limit: usize,
        ) -> (Arc<dyn Array>, Vec<CellError>) {
            match data {
                ExcelSheetData::Owned(range) => {
                    with_error_impls::$func_name(range, col, offset, limit)
                }
                ExcelSheetData::Ref(range) => {
                    with_error_impls::$func_name(range, col, offset, limit)
                }
            }
        }
    };
}

create_array_function_with_errors!(create_boolean_array_with_errors);
create_array_function_with_errors!(create_int_array_with_errors);
create_array_function_with_errors!(create_float_array_with_errors);
create_array_function_with_errors!(create_string_array_with_errors);
create_array_function_with_errors!(create_date_array_with_errors);
create_array_function_with_errors!(create_datetime_array_with_errors);
create_array_function_with_errors!(create_duration_array_with_errors);

/// Converts a list of ColumnInfo to an arrow Schema
pub(crate) fn selected_columns_to_schema(columns: &[ColumnInfo]) -> Schema {
    let fields: Vec<_> = columns.iter().map(Into::<Field>::into).collect();
    Schema::new(fields)
}

/// Creates an arrow RecordBatch from an Iterator over (column_name, column data tuples) and an arrow schema
pub(crate) fn record_batch_from_name_array_iterator<
    'a,
    I: Iterator<Item = (&'a str, Arc<dyn Array>)>,
>(
    iter: I,
    schema: Schema,
) -> FastExcelResult<RecordBatch> {
    let mut iter = iter.peekable();
    // If the iterable is empty, try_from_iter returns an Err
    if iter.peek().is_none() {
        Ok(RecordBatch::new_empty(Arc::new(schema)))
    } else {
        // We use `try_from_iter_with_nullable` because `try_from_iter` relies on `array.null_count() > 0;`
        // to determine if the array is nullable. This is not the case for `NullArray` which has no nulls.
        RecordBatch::try_from_iter_with_nullable(iter.map(|(field_name, array)| {
            let nullable = array.is_nullable();
            (field_name, array, nullable)
        }))
        .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
        .with_context(|| "could not create RecordBatch from iterable")
    }
}

/// Creates an arrow `RecordBatch` from `ExcelSheetData`. Expects the following parameters:
/// * `columns`: a slice of `ColumnInfo`, representing the columns that should be extracted from the range
/// * `data`: the sheets data, as an `ExcelSheetData`
/// * `offset`: the row index at which to start
/// * `limit`: the row index at which to stop (excluded)
pub(crate) fn record_batch_from_data_and_columns<CT: CellType + DataType>(
    columns: &[ColumnInfo],
    data: &Range<CT>,
    offset: usize,
    limit: usize,
) -> FastExcelResult<RecordBatch> {
    // Use RowSelector::Range for simple offset..limit case - no Vec allocation!
    let row_selector = RowSelector::Range(offset..limit);
    record_batch_from_data_and_columns_with_row_selector(columns, data, &row_selector)
}

pub(crate) fn record_batch_from_data_and_columns_with_skip_rows<CT: CellType + DataType>(
    columns: &[ColumnInfo],
    data: &Range<CT>,
    skip_rows: &SkipRows,
    offset: usize,
    limit: usize,
) -> FastExcelResult<RecordBatch> {
    // Generate row selector - ranges for simple cases, filtered Vec only when needed
    let row_selector = generate_row_selector(skip_rows, offset, limit)?;
    record_batch_from_data_and_columns_with_row_selector(columns, data, &row_selector)
}

fn record_batch_from_data_and_columns_with_row_selector<CT: CellType + DataType>(
    columns: &[ColumnInfo],
    data: &Range<CT>,
    row_selector: &RowSelector,
) -> FastExcelResult<RecordBatch> {
    let schema = selected_columns_to_schema(columns);
    let row_count = row_selector.len();
    let iter = columns.iter().map(|column_info| {
        let col_idx = column_info.index;
        let dtype = column_info.dtype;
        (
            column_info.name.as_str(),
            match dtype {
                DType::Null => Arc::new(NullArray::new(row_count)),
                DType::Int => create_int_array(data, col_idx, row_selector.iter()),
                DType::Float => create_float_array(data, col_idx, row_selector.iter()),
                DType::String => create_string_array(data, col_idx, row_selector.iter()),
                DType::Bool => create_boolean_array(data, col_idx, row_selector.iter()),
                DType::DateTime => create_datetime_array(data, col_idx, row_selector.iter()),
                DType::Date => create_date_array(data, col_idx, row_selector.iter()),
                DType::Duration => create_duration_array(data, col_idx, row_selector.iter()),
            },
        )
    });

    record_batch_from_name_array_iterator(iter, schema)
}

pub(crate) fn record_batch_from_data_and_columns_with_errors(
    columns: &[ColumnInfo],
    data: &ExcelSheetData,
    offset: usize,
    limit: usize,
) -> FastExcelResult<(RecordBatch, CellErrors)> {
    let schema = selected_columns_to_schema(columns);

    let mut cell_errors = vec![];

    let iter = columns.iter().map(|column_info| {
        let col_idx = column_info.index;
        let dtype = column_info.dtype;

        let (array, new_cell_errors) = match dtype {
            DType::Null => (Arc::new(NullArray::new(limit - offset)) as ArrayRef, vec![]),
            DType::Int => create_int_array_with_errors(data, col_idx, offset, limit),
            DType::Float => create_float_array_with_errors(data, col_idx, offset, limit),
            DType::String => create_string_array_with_errors(data, col_idx, offset, limit),
            DType::Bool => create_boolean_array_with_errors(data, col_idx, offset, limit),
            DType::DateTime => create_datetime_array_with_errors(data, col_idx, offset, limit),
            DType::Date => create_date_array_with_errors(data, col_idx, offset, limit),
            DType::Duration => create_duration_array_with_errors(data, col_idx, offset, limit),
        };

        cell_errors.extend(new_cell_errors);

        (column_info.name.as_str(), array)
    });

    let record_batch = record_batch_from_name_array_iterator(iter, schema)?;

    Ok((
        record_batch,
        CellErrors {
            errors: cell_errors,
        },
    ))
}

impl RowSelector {
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            RowSelector::Range(range) => Box::new(range.clone()),
            RowSelector::Filtered(vec) => Box::new(vec.iter().copied()),
        }
    }
}
