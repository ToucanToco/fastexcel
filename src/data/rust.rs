use calamine::{CellType, DataType, Range};
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};

use super::cell_extractors;
use crate::data::ExcelSheetData;

mod vec_impls {
    use super::*;

    pub(crate) fn create_boolean_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<bool>> {
        (offset..limit)
            .map(|row| {
                data.get((row, col))
                    .and_then(cell_extractors::extract_boolean)
            })
            .collect()
    }

    pub(crate) fn create_int_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<i64>> {
        (offset..limit)
            .map(|row| data.get((row, col)).and_then(cell_extractors::extract_int))
            .collect()
    }

    pub(crate) fn create_float_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<f64>> {
        (offset..limit)
            .map(|row| {
                data.get((row, col))
                    .and_then(cell_extractors::extract_float)
            })
            .collect()
    }

    pub(crate) fn create_string_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<String>> {
        (offset..limit)
            .map(|row| {
                data.get((row, col))
                    .and_then(cell_extractors::extract_string)
            })
            .collect()
    }

    pub(crate) fn create_date_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<NaiveDate>> {
        (offset..limit)
            .map(|row| data.get((row, col)).and_then(cell_extractors::extract_date))
            .collect()
    }

    pub(crate) fn create_datetime_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<NaiveDateTime>> {
        (offset..limit)
            .map(|row| {
                data.get((row, col))
                    .and_then(cell_extractors::extract_datetime)
            })
            .collect()
    }

    pub(crate) fn create_duration_vec<DT: CellType + DataType>(
        data: &Range<DT>,
        col: usize,
        offset: usize,
        limit: usize,
    ) -> Vec<Option<TimeDelta>> {
        (offset..limit)
            .map(|row| {
                data.get((row, col))
                    .and_then(cell_extractors::extract_duration)
            })
            .collect()
    }
}

/// Creates a function that will dispatch ExcelData to the generic create_x_array implementation
macro_rules! create_vec_function {
    ($func_name:ident,$type:ty) => {
        pub(crate) fn $func_name(
            data: &ExcelSheetData,
            col: usize,
            offset: usize,
            limit: usize,
        ) -> Vec<Option<$type>> {
            match data {
                ExcelSheetData::Owned(range) => vec_impls::$func_name(range, col, offset, limit),
                ExcelSheetData::Ref(range) => vec_impls::$func_name(range, col, offset, limit),
            }
        }
    };
}

create_vec_function!(create_boolean_vec, bool);
create_vec_function!(create_string_vec, String);
create_vec_function!(create_int_vec, i64);
create_vec_function!(create_float_vec, f64);
create_vec_function!(create_datetime_vec, NaiveDateTime);
create_vec_function!(create_date_vec, NaiveDate);
create_vec_function!(create_duration_vec, TimeDelta);
