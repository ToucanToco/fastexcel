use std::fmt::Debug;
use std::{fs::File, io::BufReader};

use anyhow::{Context, Result};
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatch;
use calamine::{open_workbook_auto, CellType, DataTypeTrait, Range, Reader, Sheets};
use pyo3::prelude::PyObject;
use pyo3::{pyclass, pymethods, PyResult, Python};

use crate::types::excelsheet::sheet_column_names_from_header_and_range;
use crate::utils::arrow::arrow_schema_from_column_names_and_range;

use super::excelsheet::record_batch_from_data_and_schema;
use super::{
    excelsheet::{Header, Pagination},
    ExcelSheet,
};

#[pyclass(name = "_ExcelReader")]
pub(crate) struct ExcelReader {
    sheets: Sheets<BufReader<File>>,
    #[pyo3(get)]
    sheet_names: Vec<String>,
    path: String,
}

impl ExcelReader {
    // NOTE: Not implementing TryFrom here, because we're aren't building the file from the passed
    // string, but rather from the file pointed by it. Semantically, try_from_path is clearer
    pub(crate) fn try_from_path(path: &str) -> Result<Self> {
        let sheets = open_workbook_auto(path)
            .with_context(|| format!("Could not open workbook at {path}"))?;
        let sheet_names = sheets.sheet_names().to_owned();
        Ok(Self {
            sheets,
            sheet_names,
            path: path.to_owned(),
        })
    }

    fn load_sheet_eager<DT: CellType + DataTypeTrait + Debug>(
        data: Range<DT>,
        pagination: Pagination,
        header: Header,
    ) -> Result<RecordBatch> {
        let column_names = sheet_column_names_from_header_and_range(&header, &data);

        let offset = header.offset() + pagination.offset();
        let limit = {
            let upper_bound = data.height();
            if let Some(n_rows) = pagination.n_rows() {
                let limit = offset + n_rows;
                if limit < upper_bound {
                    limit
                } else {
                    upper_bound
                }
            } else {
                upper_bound
            }
        };
        let schema = arrow_schema_from_column_names_and_range(&data, &column_names, offset)
            .with_context(|| "could not build arrow schema")?;

        record_batch_from_data_and_schema(schema, &data, offset, limit)
    }
}

#[pymethods]
impl ExcelReader {
    pub fn __repr__(&self) -> String {
        format!("ExcelReader<{}>", &self.path)
    }

    #[pyo3(signature = (
        name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None
    ))]
    pub fn load_sheet_by_name(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
    ) -> Result<ExcelSheet> {
        let range = self
            .sheets
            .worksheet_range(&name)
            .with_context(|| format!("Error while loading sheet {name}"))?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range)?;

        Ok(ExcelSheet::new(name, range, header, pagination))
    }

    #[pyo3(signature = (
        name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None
    ))]
    pub fn load_sheet_by_name_eager(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let range = self
            .sheets
            .worksheet_range(&name)
            .with_context(|| format!("Error while loading sheet {name}"))?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range)?;
        let rb = ExcelReader::load_sheet_eager(range, pagination, header)
            .with_context(|| "could not load sheet eagerly")?;
        rb.to_pyarrow(py)
    }

    #[pyo3(signature = (
        idx,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None)
    )]
    pub fn load_sheet_by_idx(
        &mut self,
        idx: usize,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
    ) -> Result<ExcelSheet> {
        let name = self
            .sheet_names
            .get(idx)
            .with_context(|| {
                format!(
                    "Sheet index {idx} is out of range. File has {} sheets",
                    self.sheet_names.len()
                )
            })?
            .to_owned();
        let range = self
            .sheets
            .worksheet_range_at(idx)
            .with_context(|| format!("Sheet at idx {idx} not found"))?
            .with_context(|| format!("Error while loading sheet at idx {idx}"))?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range)?;
        Ok(ExcelSheet::new(name, range, header, pagination))
    }

    #[pyo3(signature = (
        idx,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None)
    )]
    pub fn load_sheet_by_idx_eager(
        &mut self,
        idx: usize,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let range = self
            .sheets
            .worksheet_range_at(idx)
            .with_context(|| format!("Sheet at idx {idx} not found"))?
            .with_context(|| format!("Error while loading sheet at idx {idx}"))?;
        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range)?;
        let rb = ExcelReader::load_sheet_eager(range, pagination, header)
            .with_context(|| "could not load sheet eagerly")?;
        rb.to_pyarrow(py)
    }
}
