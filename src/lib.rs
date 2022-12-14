mod types;
mod utils;

use anyhow::Result;
use pyo3::prelude::*;
use types::{ExcelReader, ExcelSheet};

/// Reads an excel file and returns an object allowing to access its sheets and a bit of metadata
#[pyfunction]
fn read_excel(path: &str) -> Result<ExcelReader> {
    Ok(ExcelReader::try_from_path(path).unwrap())
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
fn _fastexcel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    m.add_class::<ExcelSheet>()?;
    m.add_class::<ExcelReader>()?;
    m.add("__version__", get_version())?;
    Ok(())
}
