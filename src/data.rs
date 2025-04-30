use std::sync::Arc;

use arrow::{
    array::{Array, NullArray, RecordBatch},
    datatypes::{Field, Schema},
};
use calamine::{Data as CalData, DataRef as CalDataRef, DataType, Range};

use crate::{
    error::{ErrorContext, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DType, DTypeCoercion, get_dtype_for_column},
        python::excelsheet::column_info::ColumnInfo,
        python::excelsheet::{CellError, CellErrors},
    },
};

#[derive(Debug)]
pub(crate) enum ExcelSheetData<'r> {
    Owned(Range<CalData>),
    Ref(Range<CalDataRef<'r>>),
}

impl ExcelSheetData<'_> {
    pub(crate) fn width(&self) -> usize {
        match self {
            ExcelSheetData::Owned(range) => range.width(),
            ExcelSheetData::Ref(range) => range.width(),
        }
    }

    pub(crate) fn height(&self) -> usize {
        match self {
            ExcelSheetData::Owned(range) => range.height(),
            ExcelSheetData::Ref(range) => range.height(),
        }
    }

    pub(super) fn get_as_string(&self, pos: (usize, usize)) -> Option<String> {
        match self {
            ExcelSheetData::Owned(range) => range.get(pos).and_then(|data| data.as_string()),
            ExcelSheetData::Ref(range) => range.get(pos).and_then(|data| data.as_string()),
        }
    }

    pub(crate) fn dtype_for_column(
        &self,
        start_row: usize,
        end_row: usize,
        col: usize,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<DType> {
        match self {
            ExcelSheetData::Owned(data) => {
                get_dtype_for_column(data, start_row, end_row, col, dtype_coercion)
            }
            ExcelSheetData::Ref(data) => {
                get_dtype_for_column(data, start_row, end_row, col, dtype_coercion)
            }
        }
    }
}

impl From<Range<CalData>> for ExcelSheetData<'_> {
    fn from(range: Range<CalData>) -> Self {
        Self::Owned(range)
    }
}

impl<'a> From<Range<CalDataRef<'a>>> for ExcelSheetData<'a> {
    fn from(range: Range<CalDataRef<'a>>) -> Self {
        Self::Ref(range)
    }
}

mod array_impls {
    use std::fmt::Debug;
    use std::sync::Arc;

    use arrow::array::{
        Array, BooleanArray, Date32Array, DurationMillisecondArray, Float64Array, Int64Array,
        StringArray, TimestampMillisecondArray,
    };
    use calamine::{CellType, DataType, Range};
    use chrono::NaiveDate;

    use crate::types::{dtype::excel_float_to_string, python::excelsheet::CellError};

    // TODO: DRY duplicated code between create and create...with_errors functions
    pub(crate) fn create_boolean_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        Arc::new(BooleanArray::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if let Some(b) = cell.get_bool() {
                    Some(b)
                } else if let Some(i) = cell.get_int() {
                    Some(i != 0)
                }
                // clippy formats else if let Some(blah) = ... { Some(x) } else { None } to the .map form
                else {
                    cell.get_float().map(|f| f != 0.0)
                }
            })
        })))
    }

    pub(crate) fn create_boolean_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(BooleanArray::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else if let Some(b) = cell.get_bool() {
                    Some(b)
                } else if let Some(i) = cell.get_int() {
                    Some(i != 0)
                } else if let Some(f) = cell.get_float() {
                    Some(f != 0.0)
                } else {
                    cell_errors.push(CellError {
                        position: (row, col),
                        row_offset: offset,
                        detail: format!("Expected boolean but got '{:?}", cell),
                    });
                    None
                }
            })
        })));

        (arr, cell_errors)
    }

    pub(crate) fn create_int_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        Arc::new(Int64Array::from_iter(
            (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.as_i64())),
        ))
    }

    pub(crate) fn create_int_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
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
                    match cell.as_i64() {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected int but got '{:?}'", cell),
                            });
                            None
                        }
                    }
                }
            })
        })));
        (arr, cell_errors)
    }

    pub(crate) fn create_float_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        Arc::new(Float64Array::from_iter(
            (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.as_f64())),
        ))
    }

    pub(crate) fn create_float_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
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
                    match cell.as_f64() {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected float but got '{:?}'", cell),
                            });
                            None
                        }
                    }
                }
            })
        })));
        (arr, cell_errors)
    }

    pub(crate) fn create_string_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        Arc::new(StringArray::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_string() {
                    cell.get_string().map(str::to_string)
                } else if cell.is_datetime() {
                    cell.get_datetime()
                        .and_then(|dt| dt.as_datetime())
                        .map(|dt| dt.to_string())
                } else if cell.is_datetime_iso() {
                    cell.get_datetime_iso().map(str::to_string)
                } else if cell.is_bool() {
                    cell.get_bool().map(|v| v.to_string())
                } else if cell.is_float() {
                    cell.get_float().map(excel_float_to_string)
                } else {
                    cell.as_string()
                }
            })
        })))
    }

    pub(crate) fn create_string_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let arr = Arc::new(StringArray::from_iter((offset..limit).map(|row| {
            data.get((row, col)).and_then(|cell| {
                if cell.is_empty() {
                    None
                } else if cell.is_string() {
                    cell.get_string().map(str::to_string)
                } else if cell.is_datetime() {
                    cell.get_datetime()
                        .and_then(|dt| dt.as_datetime())
                        .map(|dt| dt.to_string())
                } else if cell.is_datetime_iso() {
                    cell.get_datetime_iso().map(str::to_string)
                } else if cell.is_bool() {
                    cell.get_bool().map(|v| v.to_string())
                } else if cell.is_float() {
                    cell.get_float().map(excel_float_to_string)
                } else {
                    match cell.as_string() {
                        Some(value) => Some(value),
                        None => {
                            cell_errors.push(CellError {
                                position: (row, col),
                                row_offset: offset,
                                detail: format!("Expected string but got '{:?}'", cell),
                            });
                            None
                        }
                    }
                }
            })
        })));

        (arr, cell_errors)
    }

    fn duration_type_to_i64<DT: CellType + DataType>(caldt: &DT) -> Option<i64> {
        caldt.as_duration().map(|d| d.num_milliseconds())
    }

    pub(crate) fn create_date_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Arc::new(Date32Array::from_iter((offset..limit).map(|row| {
            data.get((row, col))
                .and_then(|caldate| caldate.as_date())
                .and_then(|date| i32::try_from(date.signed_duration_since(epoch).num_days()).ok())
        })))
    }

    pub(crate) fn create_date_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let arr = Arc::new(Date32Array::from_iter((offset..limit).map(|row| {
            data.get((row, col))
                .and_then(|cell| {
                    if cell.is_empty() {
                        None
                    } else {
                        match cell.as_date() {
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
                .and_then(|date| i32::try_from(date.signed_duration_since(epoch).num_days()).ok())
        })));

        (arr, cell_errors)
    }

    pub(crate) fn create_datetime_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        Arc::new(TimestampMillisecondArray::from_iter((offset..limit).map(
            |row| {
                data.get((row, col))
                    .and_then(|caldt| caldt.as_datetime())
                    .map(|dt| dt.and_utc().timestamp_millis())
            },
        )))
    }

    pub(crate) fn create_datetime_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> (Arc<dyn Array>, Vec<CellError>) {
        let mut cell_errors = vec![];
        let arr = Arc::new(TimestampMillisecondArray::from_iter((offset..limit).map(
            |row| {
                data.get((row, col))
                    .and_then(|cell| {
                        if cell.is_empty() {
                            None
                        } else {
                            match cell.as_datetime() {
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
                    .map(|dt| dt.and_utc().timestamp_millis())
            },
        )));
        (arr, cell_errors)
    }

    pub(crate) fn create_duration_array<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Arc<dyn Array> {
        Arc::new(DurationMillisecondArray::from_iter(
            (offset..limit).map(|row| data.get((row, col)).and_then(duration_type_to_i64)),
        ))
    }

    pub(crate) fn create_duration_array_with_errors<DT: CellType + DataType + Debug>(
        data: &Range<DT>,
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
                        match duration_type_to_i64(cell) {
                            Some(value) => Some(value),
                            None => {
                                cell_errors.push(CellError {
                                    position: (row, col),
                                    row_offset: offset,
                                    detail: format!("Expected duration but got '{:?}'", cell),
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

/// Creates a function that will dispatch ExcelData to the generic create_x_array implementation
macro_rules! create_array_function {
    ($func_name:ident) => {
        pub(crate) fn $func_name(
            data: &ExcelSheetData,
            col: usize,
            offset: usize,
            limit: usize,
        ) -> Arc<dyn Array> {
            match data {
                ExcelSheetData::Owned(range) => array_impls::$func_name(range, col, offset, limit),
                ExcelSheetData::Ref(range) => array_impls::$func_name(range, col, offset, limit),
            }
        }
    };
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
                ExcelSheetData::Owned(range) => array_impls::$func_name(range, col, offset, limit),
                ExcelSheetData::Ref(range) => array_impls::$func_name(range, col, offset, limit),
            }
        }
    };
}

create_array_function!(create_boolean_array);
create_array_function!(create_string_array);
create_array_function!(create_int_array);
create_array_function!(create_float_array);
create_array_function!(create_datetime_array);
create_array_function!(create_date_array);
create_array_function!(create_duration_array);

create_array_function_with_errors!(create_boolean_array_with_errors);
create_array_function_with_errors!(create_int_array_with_errors);
create_array_function_with_errors!(create_float_array_with_errors);
create_array_function_with_errors!(create_string_array_with_errors);
create_array_function_with_errors!(create_date_array_with_errors);
create_array_function_with_errors!(create_datetime_array_with_errors);
create_array_function_with_errors!(create_duration_array_with_errors);

pub(crate) use array_impls::create_boolean_array as create_boolean_array_from_range;
pub(crate) use array_impls::create_date_array as create_date_array_from_range;
pub(crate) use array_impls::create_datetime_array as create_datetime_array_from_range;
pub(crate) use array_impls::create_duration_array as create_duration_array_from_range;
pub(crate) use array_impls::create_float_array as create_float_array_from_range;
pub(crate) use array_impls::create_int_array as create_int_array_from_range;
pub(crate) use array_impls::create_string_array as create_string_array_from_range;

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
pub(crate) fn record_batch_from_data_and_columns(
    columns: &[ColumnInfo],
    data: &ExcelSheetData,
    offset: usize,
    limit: usize,
) -> FastExcelResult<RecordBatch> {
    let schema = selected_columns_to_schema(columns);
    let iter = columns.iter().map(|column_info| {
        let col_idx = column_info.index();
        let dtype = *column_info.dtype();
        (
            column_info.name.as_str(),
            match dtype {
                DType::Null => Arc::new(NullArray::new(limit - offset)),
                DType::Int => create_int_array(data, col_idx, offset, limit),
                DType::Float => create_float_array(data, col_idx, offset, limit),
                DType::String => create_string_array(data, col_idx, offset, limit),
                DType::Bool => create_boolean_array(data, col_idx, offset, limit),
                DType::DateTime => create_datetime_array(data, col_idx, offset, limit),
                DType::Date => create_date_array(data, col_idx, offset, limit),
                DType::Duration => create_duration_array(data, col_idx, offset, limit),
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
        let col_idx = column_info.index();
        let dtype = *column_info.dtype();

        let (array, new_cell_errors) = match dtype {
            DType::Null => (
                Arc::new(NullArray::new(limit - offset)) as Arc<dyn arrow::array::Array>,
                vec![],
            ),
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
