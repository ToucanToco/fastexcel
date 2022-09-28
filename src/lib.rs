mod core;

use anyhow::{Context, Result};
use pyo3::{prelude::*, types::PyBytes};

use crate::core::{extract_sheets, record_batch_to_bytes};

/// Formats the sum of two numbers as string.
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
    Ok(())
}
