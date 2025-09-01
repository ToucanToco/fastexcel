use arrow_schema::{DataType as ArrowDataType, TimeUnit};
use pyo3::{
    Bound, FromPyObject, IntoPyObject, PyAny, PyResult, Python,
    types::{PyAnyMethods, PyString},
};

use crate::{
    error::{FastExcelErrorKind, py_errors::IntoPyResult},
    types::dtype::{DType, DTypeCoercion, DTypeMap, DTypes},
};

impl<'py> IntoPyObject<'py> for DType {
    type Target = PyString;

    type Output = Bound<'py, Self::Target>;

    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_string().into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for &DType {
    type Target = PyString;

    type Output = Bound<'py, Self::Target>;

    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.to_string().into_pyobject(py)
    }
}

impl FromPyObject<'_> for DType {
    fn extract_bound(py_dtype: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(dtype_pystr) = py_dtype.extract::<String>() {
            dtype_pystr.parse()
        } else {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "{py_dtype:?} cannot be converted to str"
            ))
            .into())
        }
        .into_pyresult()
    }
}

impl FromPyObject<'_> for DTypes {
    fn extract_bound(py_dtypes: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(py_dtypes_str) = py_dtypes.extract::<String>() {
            py_dtypes_str.parse()
        } else {
            Ok(DTypes::Map(py_dtypes.extract::<DTypeMap>()?))
        }
        .into_pyresult()
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

impl FromPyObject<'_> for DTypeCoercion {
    fn extract_bound(py_dtype_coercion: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(dtype_coercion_pystr) = py_dtype_coercion.extract::<String>() {
            dtype_coercion_pystr.parse()
        } else {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "{py_dtype_coercion:?} cannot be converted to str"
            ))
            .into())
        }
        .into_pyresult()
    }
}
