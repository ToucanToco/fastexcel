#[cfg(feature = "python")]
mod python;

use calamine::{Data, Range};
#[cfg(feature = "python")]
use pyo3::pyclass;

use crate::error::FastExcelResult;

/// Represents a range of cells in an Excel sheet
#[derive(Debug)]
#[cfg_attr(feature = "python", pyclass(name = "_ExcelRange"))]
pub struct ExcelRange {
    range: Range<Data>,
    start: (usize, usize),
    end: (usize, usize),
}

impl ExcelRange {
    pub fn new(range: Range<Data>, start: (usize, usize), end: (usize, usize)) -> Self {
        Self { range, start, end }
    }

    /// Get the width of the range (number of columns)
    pub fn width(&self) -> usize {
        self.end.1 - self.start.1 + 1
    }

    /// Get the height of the range (number of rows)
    pub fn height(&self) -> usize {
        self.end.0 - self.start.0 + 1
    }

    /// Get the starting position (row, column) as 0-based indices
    pub fn start(&self) -> (usize, usize) {
        self.start
    }

    /// Get the ending position (row, column) as 0-based indices
    pub fn end(&self) -> (usize, usize) {
        self.end
    }

    /// Get a cell value at the given position relative to the range start
    pub fn get(&self, row: usize, col: usize) -> Option<&Data> {
        let abs_row = self.start.0 + row;
        let abs_col = self.start.1 + col;

        if abs_row <= self.end.0 && abs_col <= self.end.1 {
            self.range.get_value((abs_row as u32, abs_col as u32))
        } else {
            None
        }
    }

    /// Get the underlying Range for direct access
    pub fn inner(&self) -> &Range<Data> {
        &self.range
    }

    /// Extract a subrange from the current range
    pub fn subrange(
        &self,
        start: (usize, usize),
        end: (usize, usize),
    ) -> FastExcelResult<Range<Data>> {
        let abs_start = (
            (self.start.0 + start.0) as u32,
            (self.start.1 + start.1) as u32,
        );
        let abs_end = ((self.start.0 + end.0) as u32, (self.start.1 + end.1) as u32);

        Ok(self.range.range(abs_start, abs_end))
    }
}
