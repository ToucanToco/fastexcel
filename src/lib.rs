mod core;
mod types;
mod utils;

use anyhow::{Context, Result};
use pyo3::prelude::*;

use crate::core::extract_sheets_iter;
use crate::types::ExcelSheetIterator;

/// Reads an excel file and returns aan iterator of bytes. Each bytes objects represents a sheet of
/// the file as an Arrow RecordBatch, serialized in the IPC format
#[pyfunction]
fn read_excel(path: &str) -> Result<ExcelSheetIterator> {
    extract_sheets_iter(path).with_context(|| format!("could not load file at {path}"))
}

#[pymodule]
fn fastexcel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    Ok(())
}
