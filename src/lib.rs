mod core;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use pyo3::{prelude::*, types::PyBytes};

use crate::core::{extract_sheets, extract_sheets_iter, record_batch_to_bytes, ExcelSheetIterator};

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

#[pyfunction]
fn read_excel_lazy(path: &str) -> Result<PyExcelSheetIterator> {
    let sheets =
        extract_sheets_iter(path).with_context(|| format!("could not load file at {path}"))?;
    Ok(PyExcelSheetIterator { it: sheets })
}

/// Reads an excel file and returns a list of bytes representing. Each bytes objects
/// represents a sheet of the file as an Arrow RecordBatch, serialized in the IPC format
#[pyfunction]
fn read_excel<'p>(py: Python<'p>, path: &str) -> Result<Vec<&'p PyBytes>> {
    // FIXME: Allocating two vecs here, extract_sheets should return an Iterator
    let sheets = extract_sheets(path).with_context(|| format!("could not load file at {path}"))?;
    sheets
        .iter()
        // FIXME: Allocating AGAIN here (PyBytes::new clones, we'll probably have to use some unsafe
        // shit here, such as from_raw_ptr)
        .map(|rb| record_batch_to_bytes(rb).map(|v| PyBytes::new(py, v.as_slice())))
        .collect()
}

/// A Python module implemented in Rust.
#[pymodule]
fn fastexcel(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_excel, m)?)?;
    m.add_function(wrap_pyfunction!(read_excel_lazy, m)?)?;
    Ok(())
}
