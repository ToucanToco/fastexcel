use std::{collections::HashMap, str::FromStr};

use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use pyo3::{
    types::{IntoPyDict, PyDict},
    PyObject, Python, ToPyObject,
};

use crate::error::{FastExcelError, FastExcelErrorKind, FastExcelResult};

#[derive(Debug)]
pub(crate) enum DType {
    Null,
    Int,
    Float,
    String,
    Bool,
    DateTime,
    Date,
    Duration,
}

impl FromStr for DType {
    type Err = FastExcelError;

    fn from_str(raw_dtype: &str) -> FastExcelResult<Self> {
        match raw_dtype {
            "null" => Ok(Self::Null),
            "int" => Ok(Self::Int),
            "float" => Ok(Self::Float),
            "string" => Ok(Self::String),
            "boolean" => Ok(Self::Bool),
            "datetime" => Ok(Self::DateTime),
            "date" => Ok(Self::Date),
            "duration" => Ok(Self::Duration),
            _ => Err(FastExcelErrorKind::InvalidParameters(format!(
                "unsupported dtype: \"{raw_dtype}\""
            ))
            .into()),
        }
    }
}

impl ToPyObject for DType {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            DType::Null => "null",
            DType::Int => "int",
            DType::Float => "float",
            DType::String => "string",
            DType::Bool => "boolean",
            DType::DateTime => "datetime",
            DType::Date => "date",
            DType::Duration => "duration",
        }
        .to_object(py)
    }
}

#[derive(Debug)]
pub(crate) enum DTypeMap {
    ByIndex(HashMap<usize, DType>),
    ByName(HashMap<String, DType>),
}

impl DTypeMap {
    pub(crate) fn dtype_for_col_name(&self, col_name: &String) -> Option<&DType> {
        match self {
            DTypeMap::ByName(name_map) => name_map.get(col_name),
            _ => None,
        }
    }

    pub(crate) fn dtype_for_col_idx(&self, col_idx: usize) -> Option<&DType> {
        match self {
            DTypeMap::ByIndex(idx_map) => idx_map.get(&col_idx),
            _ => None,
        }
    }
}

impl<S: AsRef<str>> TryFrom<HashMap<usize, S>> for DTypeMap {
    type Error = FastExcelError;

    fn try_from(value: HashMap<usize, S>) -> FastExcelResult<Self> {
        value
            .into_iter()
            .map(|(column, raw_dtype)| {
                raw_dtype
                    .as_ref()
                    .parse()
                    .map(|raw_dtype| (column, raw_dtype))
            })
            .collect::<FastExcelResult<HashMap<_, _>>>()
            .map(Self::ByIndex)
    }
}

impl<S: AsRef<str>> TryFrom<HashMap<String, S>> for DTypeMap {
    type Error = FastExcelError;

    fn try_from(value: HashMap<String, S>) -> FastExcelResult<Self> {
        value
            .into_iter()
            .map(|(column, raw_dtype)| {
                raw_dtype
                    .as_ref()
                    .parse()
                    .map(|raw_dtype| (column, raw_dtype))
            })
            .collect::<FastExcelResult<HashMap<_, _>>>()
            .map(Self::ByName)
    }
}

impl TryFrom<&PyDict> for DTypeMap {
    type Error = FastExcelError;

    fn try_from(py_dict: &PyDict) -> FastExcelResult<Self> {
        if let Ok(string_map) = py_dict.extract::<HashMap<String, &str>>() {
            string_map.try_into()
        } else if let Ok(string_map) = py_dict.extract::<HashMap<usize, &str>>() {
            string_map.try_into()
        } else {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "unsupported dtype map: {py_dict:?}"
            ))
            .into())
        }
    }
}

impl From<&DType> for ArrowDataType {
    fn from(dtype: &DType) -> Self {
        match dtype {
            DType::Null => ArrowDataType::Null,
            DType::Int => ArrowDataType::Int64,
            DType::Float => ArrowDataType::Float64,
            DType::String => ArrowDataType::Utf8,
            DType::Bool => ArrowDataType::Boolean,
            DType::DateTime => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            DType::Date => ArrowDataType::Date32,
            DType::Duration => ArrowDataType::Duration(TimeUnit::Millisecond),
        }
    }
}

impl ToPyObject for DTypeMap {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            DTypeMap::ByIndex(idx_map) => idx_map
                .iter()
                .map(|(k, v)| (k, v.to_object(py)))
                .into_py_dict(py)
                .into(),

            DTypeMap::ByName(name_map) => name_map
                .iter()
                .map(|(k, v)| (k, v.to_object(py)))
                .into_py_dict(py)
                .into(),
        }
    }
}
