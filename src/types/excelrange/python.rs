use pyo3::{PyResult, pymethods};

use super::ExcelRange;

#[pymethods]
impl ExcelRange {
    pub fn __repr__(&self) -> String {
        format!(
            "ExcelRange<start=({}, {}), end=({}, {}), width={}, height={}>",
            self.start.0,
            self.start.1,
            self.end.0,
            self.end.1,
            self.width(),
            self.height()
        )
    }

    #[getter("width")]
    pub fn py_width(&self) -> usize {
        self.width()
    }

    #[getter("height")]
    pub fn py_height(&self) -> usize {
        self.height()
    }

    #[getter("start")]
    pub fn py_start(&self) -> (usize, usize) {
        self.start()
    }

    #[getter("end")]
    pub fn py_end(&self) -> (usize, usize) {
        self.end()
    }

    /// Get cell value at position relative to range start
    pub fn get_cell(&self, row: usize, col: usize) -> PyResult<Option<String>> {
        Ok(self.get(row, col).map(|data| format!("{}", data)))
    }

    /// Convert the range to a list of lists (rows of values)
    pub fn to_list(&self) -> Vec<Vec<Option<String>>> {
        let mut rows = Vec::new();
        for row_idx in 0..self.height() {
            let mut row = Vec::new();
            for col_idx in 0..self.width() {
                row.push(self.get(row_idx, col_idx).map(|data| format!("{}", data)));
            }
            rows.push(row);
        }
        rows
    }
}
