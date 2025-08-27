use pyo3::{
    Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyResult, Python,
    types::PyAnyMethods,
};

use crate::{
    error::{FastExcelError, FastExcelErrorKind, FastExcelResult, py_errors::IntoPyResult},
    types::idx_or_name::IdxOrName,
};

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

impl<'py> IntoPyObject<'py> for IdxOrName {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = pyo3::PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            IdxOrName::Idx(idx) => idx.into_bound_py_any(py),
            IdxOrName::Name(name) => name.into_bound_py_any(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for &IdxOrName {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = pyo3::PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            IdxOrName::Idx(idx) => idx.into_bound_py_any(py),
            IdxOrName::Name(name) => name.into_bound_py_any(py),
        }
    }
}
