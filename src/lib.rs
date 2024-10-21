mod data;
mod error;
mod types;
mod utils;

use error::{py_errors, ErrorContext};
use pyo3::prelude::*;
use types::python::{
    excelsheet::column_info::ColumnInfo, table::ExcelTable, ExcelReader, ExcelSheet,
};

/// Reads an excel file and returns an object allowing to access its sheets and a bit of metadata
#[pyfunction]
fn read_excel(source: &Bound<'_, PyAny>) -> PyResult<ExcelReader> {
    use py_errors::IntoPyResult;

    if let Ok(path) = source.extract::<String>() {
        ExcelReader::try_from_path(&path)
            .with_context(|| format!("could not load excel file at {path}"))
            .into_pyresult()
    } else if let Ok(bytes) = source.extract::<&[u8]>() {
        ExcelReader::try_from(bytes)
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
fn get_version() -> String {
    let version = env!("CARGO_PKG_VERSION").to_string();
    // cargo uses "1.0-alpha1" etc. while python uses "1.0.0a1", this is not full compatibility,
    // but it's good enough for now
    // see https://docs.rs/semver/1.0.9/semver/struct.Version.html#method.parse for rust spec
    // see https://peps.python.org/pep-0440/ for python spec
    // it seems the dot after "alpha/beta" e.g. "-alpha.1" is not necessary, hence why this works
    version.replace("-alpha", "a").replace("-beta", "b")
}

#[pymodule]
fn _fastexcel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    let py = m.py();
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    m.add_class::<ColumnInfo>()?;
    m.add_class::<ExcelSheet>()?;
    m.add_class::<ExcelReader>()?;
    m.add_class::<ExcelTable>()?;
    m.add("__version__", get_version())?;

    // errors
    [
        (
            "FastExcelError",
            py.get_type_bound::<py_errors::FastExcelError>(),
        ),
        (
            "UnsupportedColumnTypeCombinationError",
            py.get_type_bound::<py_errors::UnsupportedColumnTypeCombinationError>(),
        ),
        (
            "CannotRetrieveCellDataError",
            py.get_type_bound::<py_errors::CannotRetrieveCellDataError>(),
        ),
        (
            "CalamineCellError",
            py.get_type_bound::<py_errors::CalamineCellError>(),
        ),
        (
            "CalamineError",
            py.get_type_bound::<py_errors::CalamineError>(),
        ),
        (
            "SheetNotFoundError",
            py.get_type_bound::<py_errors::SheetNotFoundError>(),
        ),
        (
            "ColumnNotFoundError",
            py.get_type_bound::<py_errors::ColumnNotFoundError>(),
        ),
        ("ArrowError", py.get_type_bound::<py_errors::ArrowError>()),
        (
            "InvalidParametersError",
            py.get_type_bound::<py_errors::InvalidParametersError>(),
        ),
    ]
    .into_iter()
    .try_for_each(|(exc_name, exc_type)| m.add(exc_name, exc_type))
}
