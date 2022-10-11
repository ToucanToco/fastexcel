mod core;
mod types;
mod utils;

use anyhow::Result;
use pyo3::prelude::*;
use types::ExcelSheetIterator;

use crate::core::load_excel_file;

/// Reads an excel file and returns the list of sheet names along with an iterator over its
/// sheets. Sheets are represented by ExcelSheet objects
#[pyfunction]
fn read_excel(path: &str) -> Result<(Vec<String>, ExcelSheetIterator)> {
    load_excel_file(path)
}

#[pymodule]
fn fastexcel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    Ok(())
}
