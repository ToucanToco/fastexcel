use calamine::{CellType, DataType};
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};

use crate::types::dtype::excel_float_to_string;

pub(super) fn extract_boolean<DT: CellType + DataType>(cell: &DT) -> Option<bool> {
    if let Some(b) = cell.get_bool() {
        Some(b)
    } else if let Some(i) = cell.get_int() {
        Some(i != 0)
    }
    // clippy formats else if let Some(blah) = ... { Some(x) } else { None } to the .map form
    else {
        cell.get_float().map(|f| f != 0.0)
    }
}

pub(super) fn extract_int<DT: CellType + DataType>(cell: &DT) -> Option<i64> {
    cell.as_i64()
}

pub(super) fn extract_float<DT: CellType + DataType>(cell: &DT) -> Option<f64> {
    cell.as_f64()
}

pub(super) fn extract_string<DT: CellType + DataType>(cell: &DT) -> Option<String> {
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
}

pub(super) fn extract_date<DT: CellType + DataType>(cell: &DT) -> Option<NaiveDate> {
    cell.as_date()
}

#[cfg(feature = "python")]
const EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Failed to create EPOCH");

#[cfg(feature = "python")]
pub(super) fn extract_date_as_num_days<DT: CellType + DataType>(cell: &DT) -> Option<i32> {
    extract_date(cell)
        .and_then(|date| i32::try_from(date.signed_duration_since(EPOCH).num_days()).ok())
}

pub(super) fn extract_datetime<DT: CellType + DataType>(cell: &DT) -> Option<NaiveDateTime> {
    cell.as_datetime()
}

#[cfg(feature = "python")]
pub(super) fn extract_datetime_as_timestamp_ms<DT: CellType + DataType>(cell: &DT) -> Option<i64> {
    extract_datetime(cell).map(|dt| dt.and_utc().timestamp_millis())
}

pub(super) fn extract_duration<DT: CellType + DataType>(cell: &DT) -> Option<TimeDelta> {
    cell.as_duration()
}

#[cfg(feature = "python")]
pub(super) fn extract_duration_as_ms<DT: CellType + DataType>(cell: &DT) -> Option<i64> {
    extract_duration(cell).map(|d| d.num_milliseconds())
}
