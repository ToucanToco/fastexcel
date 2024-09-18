use std::sync::Arc;

use arrow::array::Array;
use calamine::{Data as CalData, DataRef as CalDataRef, DataType, Range};

use crate::{
    error::FastExcelResult,
    types::dtype::{get_dtype_for_column, DType, DTypeCoercion},
};

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
    use std::sync::Arc;

    use arrow::array::{
        Array, BooleanArray, Date32Array, DurationMillisecondArray, Float64Array, Int64Array,
        StringArray, TimestampMillisecondArray,
    };
    use calamine::{CellType, DataType, Range};
    use chrono::NaiveDate;

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
                } else {
                    cell.as_string()
                }
            })
        })))
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

create_array_function!(create_boolean_array);
create_array_function!(create_string_array);
create_array_function!(create_int_array);
create_array_function!(create_float_array);
create_array_function!(create_datetime_array);
create_array_function!(create_date_array);
create_array_function!(create_duration_array);

pub(crate) use array_impls::create_boolean_array as create_boolean_array_from_range;
pub(crate) use array_impls::create_date_array as create_date_array_from_range;
pub(crate) use array_impls::create_datetime_array as create_datetime_array_from_range;
pub(crate) use array_impls::create_duration_array as create_duration_array_from_range;
pub(crate) use array_impls::create_float_array as create_float_array_from_range;
pub(crate) use array_impls::create_int_array as create_int_array_from_range;
pub(crate) use array_impls::create_string_array as create_string_array_from_range;
