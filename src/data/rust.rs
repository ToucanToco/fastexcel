use calamine::{CellType, DataType, Range};
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};

use super::cell_extractors;

pub(crate) fn create_boolean_vec<CT: CellType + DataType>(
    data: &Range<CT>,
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

pub(crate) fn create_int_vec<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Vec<Option<i64>> {
    (offset..limit)
        .map(|row| data.get((row, col)).and_then(cell_extractors::extract_int))
        .collect()
}

pub(crate) fn create_float_vec<CT: CellType + DataType>(
    data: &Range<CT>,
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

pub(crate) fn create_string_vec<CT: CellType + DataType>(
    data: &Range<CT>,
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

pub(crate) fn create_date_vec<CT: CellType + DataType>(
    data: &Range<CT>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Vec<Option<NaiveDate>> {
    (offset..limit)
        .map(|row| data.get((row, col)).and_then(cell_extractors::extract_date))
        .collect()
}

pub(crate) fn create_datetime_vec<CT: CellType + DataType>(
    data: &Range<CT>,
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

pub(crate) fn create_duration_vec<CT: CellType + DataType>(
    data: &Range<CT>,
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
