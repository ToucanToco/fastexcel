mod core;
mod types;
mod utils;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use pyo3::{prelude::*, types::PyBytes};

use crate::core::{extract_sheets_iter, ExcelSheetIterator};
use crate::utils::arrow::record_batch_to_bytes;

#[pyclass]
struct PyExcelSheetIterator {
    it: ExcelSheetIterator,
}

fn record_batch_to_pybytes<'p>(py: Python<'p>, rb: &RecordBatch) -> Result<&'p PyBytes> {
    record_batch_to_bytes(rb).map(|bytes| PyBytes::new(py, bytes.as_slice()))
}

#[pymethods]
impl PyExcelSheetIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> Result<Option<PyObject>> {
        match slf.it.next() {
            None => Ok(None),
            Some(sheet) => record_batch_to_pybytes(py, &RecordBatch::try_from(&sheet?)?)
                .map(|b| Some(b.into())),
        }
    }
}

/// Reads an excel file and returns aan iterator of bytes. Each bytes objects represents a sheet of
/// the file as an Arrow RecordBatch, serialized in the IPC format
#[pyfunction]
fn read_excel(path: &str) -> Result<PyExcelSheetIterator> {
    let sheets =
        extract_sheets_iter(path).with_context(|| format!("could not load file at {path}"))?;
    Ok(PyExcelSheetIterator { it: sheets })
}

#[pymodule]
fn fastexcel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    Ok(())
}
