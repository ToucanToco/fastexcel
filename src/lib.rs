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

#[pymodule]
fn _fastexcel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    m.add_class::<ExcelSheet>()?;
    m.add_class::<ExcelReader>()?;
    Ok(())
}
