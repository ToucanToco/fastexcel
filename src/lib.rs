mod data;
mod error;
mod types;
mod utils;

use std::fmt::Display;

#[cfg(feature = "python")]
use error::py_errors;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use types::excelsheet::{CellError, CellErrors};

pub use data::{FastExcelColumn, FastExcelSeries};
use error::ErrorContext;
pub use error::{FastExcelError, FastExcelErrorKind, FastExcelResult};
pub use types::{
    ColumnInfo, ColumnNameFrom, DType, DTypeCoercion, DTypeFrom, DTypes, DefinedName, ExcelReader,
    ExcelSheet, ExcelTable, IdxOrName, LoadSheetOrTableOptions, SelectedColumns, SheetVisible,
    SkipRows,
};

/// Reads an excel file and returns an object allowing to access its sheets, tables, and a bit of metadata.
/// This is a wrapper around `ExcelReader::try_from_path`.
pub fn read_excel<S: AsRef<str> + Display>(path: S) -> FastExcelResult<ExcelReader> {
    ExcelReader::try_from_path(path.as_ref())
        .with_context(|| format!("could not load excel file at {path}"))
}

#[cfg(feature = "python")]
/// Reads an excel file and returns an object allowing to access its sheets, tables, and a bit of metadata
#[pyfunction(name = "read_excel")]
fn py_read_excel<'py>(source: &Bound<'_, PyAny>, py: Python<'py>) -> PyResult<ExcelReader> {
    use py_errors::IntoPyResult;

    if let Ok(path) = source.extract::<String>() {
        py.detach(|| ExcelReader::try_from_path(&path))
            .with_context(|| format!("could not load excel file at {path}"))
            .into_pyresult()
    } else if let Ok(bytes) = source.extract::<&[u8]>() {
        py.detach(|| ExcelReader::try_from(bytes))
            .with_context(|| "could not load excel file for those bytes")
            .into_pyresult()
    } else {
        Err(py_errors::InvalidParametersError::new_err(
            "source must be a string or bytes",
        ))
    }
}

// Taken from pydantic-core:
// https://github.com/pydantic/pydantic-core/blob/main/src/lib.rs#L24
#[cfg(feature = "python")]
fn get_python_version() -> String {
    let version = env!("CARGO_PKG_VERSION").to_string();
    // cargo uses "1.0-alpha1" etc. while python uses "1.0.0a1", this is not full compatibility,
    // but it's good enough for now
    // see https://docs.rs/semver/1.0.9/semver/struct.Version.html#method.parse for rust spec
    // see https://peps.python.org/pep-0440/ for python spec
    // it seems the dot after "alpha/beta" e.g. "-alpha.1" is not necessary, hence why this works
    version.replace("-alpha", "a").replace("-beta", "b")
}

#[cfg(feature = "python")]
#[pymodule(gil_used = false)]
fn _fastexcel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    use crate::types::excelsheet::column_info::{ColumnInfo, ColumnInfoNoDtype};

    pyo3_log::init();

    let py = m.py();
    m.add_function(wrap_pyfunction!(py_read_excel, m)?)?;
    m.add_class::<ColumnInfo>()?;
    m.add_class::<ColumnInfoNoDtype>()?;
    m.add_class::<DefinedName>()?;
    m.add_class::<CellError>()?;
    m.add_class::<CellErrors>()?;
    m.add_class::<ExcelSheet>()?;
    m.add_class::<ExcelReader>()?;
    m.add_class::<ExcelTable>()?;
    m.add("__version__", get_python_version())?;

    // errors
    [
        ("FastExcelError", py.get_type::<py_errors::FastExcelError>()),
        (
            "UnsupportedColumnTypeCombinationError",
            py.get_type::<py_errors::UnsupportedColumnTypeCombinationError>(),
        ),
        (
            "CannotRetrieveCellDataError",
            py.get_type::<py_errors::CannotRetrieveCellDataError>(),
        ),
        (
            "CalamineCellError",
            py.get_type::<py_errors::CalamineCellError>(),
        ),
        ("CalamineError", py.get_type::<py_errors::CalamineError>()),
        (
            "SheetNotFoundError",
            py.get_type::<py_errors::SheetNotFoundError>(),
        ),
        (
            "ColumnNotFoundError",
            py.get_type::<py_errors::ColumnNotFoundError>(),
        ),
        ("ArrowError", py.get_type::<py_errors::ArrowError>()),
        (
            "InvalidParametersError",
            py.get_type::<py_errors::InvalidParametersError>(),
        ),
    ]
    .into_iter()
    .try_for_each(|(exc_name, exc_type)| m.add(exc_name, exc_type))
}
