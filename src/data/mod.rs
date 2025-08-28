mod cell_extractors;
#[cfg(feature = "python")]
mod python;
mod rust;
use chrono::{Duration, NaiveDate, NaiveDateTime};
#[cfg(feature = "python")]
pub(crate) use python::*;

use calamine::{Data as CalData, DataRef as CalDataRef, DataType, Range};

use crate::{
    data::rust::{
        create_boolean_vec, create_date_vec, create_datetime_vec, create_duration_vec,
        create_float_vec, create_int_vec, create_string_vec,
    },
    error::{FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DType, DTypeCoercion, get_dtype_for_column},
        excelsheet::column_info::ColumnInfo,
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

#[derive(Debug, Clone, PartialEq)]
pub enum FastExcelSeries {
    Null,
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Datetime(Vec<Option<NaiveDateTime>>),
    Date(Vec<Option<NaiveDate>>),
    Duration(Vec<Option<Duration>>),
}

macro_rules! from_vec_or_array {
    ($type:ty, $variant:ident) => {
        impl From<Vec<Option<$type>>> for FastExcelSeries {
            fn from(vec: Vec<Option<$type>>) -> Self {
                Self::$variant(vec)
            }
        }

        impl<const N: usize> From<[Option<$type>; N]> for FastExcelSeries {
            fn from(arr: [Option<$type>; N]) -> Self {
                Self::$variant(arr.to_vec())
            }
        }

        impl<const N: usize> From<[$type; N]> for FastExcelSeries {
            fn from(arr: [$type; N]) -> Self {
                Self::$variant(arr.into_iter().map(Some).collect())
            }
        }
    };
}

from_vec_or_array!(bool, Bool);
from_vec_or_array!(String, String);
from_vec_or_array!(i64, Int);
from_vec_or_array!(f64, Float);
from_vec_or_array!(NaiveDateTime, Datetime);
from_vec_or_array!(NaiveDate, Date);
from_vec_or_array!(Duration, Duration);

// Conflicting impls when using `From<AsRef<[&str]>>`
impl<const N: usize> From<[Option<&str>; N]> for FastExcelSeries {
    fn from(arr: [Option<&str>; N]) -> Self {
        Self::String(arr.into_iter().map(|s| s.map(|s| s.to_string())).collect())
    }
}

impl<const N: usize> From<[&str; N]> for FastExcelSeries {
    fn from(arr: [&str; N]) -> Self {
        Self::String(arr.into_iter().map(|s| Some(s.to_string())).collect())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FastExcelColumn {
    pub name: String,
    pub(crate) data: FastExcelSeries,
    len: usize,
}

impl FastExcelColumn {
    pub fn try_new(
        name: String,
        data: FastExcelSeries,
        len: Option<usize>,
    ) -> FastExcelResult<Self> {
        let data_len = match &data {
            FastExcelSeries::Null => None,
            FastExcelSeries::Bool(v) => Some(v.len()),
            FastExcelSeries::String(v) => Some(v.len()),
            FastExcelSeries::Int(v) => Some(v.len()),
            FastExcelSeries::Float(v) => Some(v.len()),
            FastExcelSeries::Datetime(v) => Some(v.len()),
            FastExcelSeries::Date(v) => Some(v.len()),
            FastExcelSeries::Duration(v) => Some(v.len()),
        };
        if let Some(len) = len
            && let Some(data_len) = data_len
            && data_len != len
        {
            return Err(FastExcelErrorKind::InvalidColumn(format!(
                "Column '{name}' has length {data_len} but expected {len}"
            ))
            .into());
        }
        let len = len.or(data_len).ok_or_else(|| {
            FastExcelErrorKind::InvalidColumn(
                "`len` is mandatory for `FastExcelSeries::Null`".to_string(),
            )
        })?;
        Ok(Self { name, data, len })
    }

    pub fn null<S: Into<String>>(name: S, len: usize) -> Self {
        Self {
            name: name.into(),
            data: FastExcelSeries::Null,
            len,
        }
    }

    pub(crate) fn try_from_column_info(
        column_info: &ColumnInfo,
        data: &ExcelSheetData,
        offset: usize,
        limit: usize,
    ) -> FastExcelResult<Self> {
        let len = limit.checked_sub(offset).ok_or_else(|| {
            FastExcelErrorKind::InvalidParameters(format!(
                "limit is smaller than offset: {limit} is smaller than {offset}"
            ))
        })?;
        let data = match column_info.dtype() {
            DType::Null => FastExcelSeries::Null,
            DType::Int => {
                FastExcelSeries::Int(create_int_vec(data, column_info.index(), offset, limit))
            }
            DType::Float => {
                FastExcelSeries::Float(create_float_vec(data, column_info.index(), offset, limit))
            }
            DType::String => {
                FastExcelSeries::String(create_string_vec(data, column_info.index(), offset, limit))
            }
            DType::Bool => {
                FastExcelSeries::Bool(create_boolean_vec(data, column_info.index(), offset, limit))
            }
            DType::DateTime => FastExcelSeries::Datetime(create_datetime_vec(
                data,
                column_info.index(),
                offset,
                limit,
            )),
            DType::Date => {
                FastExcelSeries::Date(create_date_vec(data, column_info.index(), offset, limit))
            }
            DType::Duration => FastExcelSeries::Duration(create_duration_vec(
                data,
                column_info.index(),
                offset,
                limit,
            )),
        };
        Ok(Self {
            name: column_info.name.clone(),
            data,
            len,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
