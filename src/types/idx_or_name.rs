use pyo3::{
    prelude::PyAnyMethods, Bound, FromPyObject, PyAny, PyObject, PyResult, Python, ToPyObject,
};

use crate::error::{py_errors::IntoPyResult, FastExcelError, FastExcelErrorKind, FastExcelResult};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) enum IdxOrName {
    Idx(usize),
    Name(String),
}

impl IdxOrName {
    pub(crate) fn format_message(&self) -> String {
        match self {
            Self::Idx(idx) => format!("at index {idx}"),
            Self::Name(name) => format!("with name \"{name}\""),
        }
    }
}

impl TryFrom<&Bound<'_, PyAny>> for IdxOrName {
    type Error = FastExcelError;

    fn try_from(value: &Bound<'_, PyAny>) -> FastExcelResult<Self> {
        if let Ok(index) = value.extract() {
            Ok(Self::Idx(index))
        } else if let Ok(name) = value.extract() {
            Ok(Self::Name(name))
        } else {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "cannot create IdxOrName from {value:?}"
            ))
            .into())
        }
    }
}

impl FromPyObject<'_> for IdxOrName {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        ob.try_into().into_pyresult()
    }
}

impl ToPyObject for IdxOrName {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            IdxOrName::Idx(idx) => idx.to_object(py),
            IdxOrName::Name(name) => name.to_object(py),
        }
    }
}

impl From<usize> for IdxOrName {
    fn from(index: usize) -> Self {
        Self::Idx(index)
    }
}

impl From<String> for IdxOrName {
    fn from(name: String) -> Self {
        Self::Name(name)
    }
}
