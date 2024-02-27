use std::fmt::Debug;
use std::{fs::File, io::BufReader};

use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};
use calamine::{open_workbook_auto, CellType, DataType, Range, Reader, Sheets};
use pyo3::{prelude::PyObject, pyclass, pymethods, types::PyList, PyResult, Python};

use crate::error::{
    py_errors::IntoPyResult, ErrorContext, FastExcelErrorKind, FastExcelResult, IdxOrName,
};

use crate::types::excelsheet::sheet_column_names_from_header_and_range;
use crate::utils::arrow::arrow_schema_from_column_names_and_range;
use crate::utils::schema::get_schema_sample_rows;

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
    pub(crate) fn try_from_path(path: &str) -> FastExcelResult<Self> {
        let sheets = open_workbook_auto(path)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| format!("Could not open workbook at {path}"))?;
        let sheet_names = sheets.sheet_names().to_owned();
        Ok(Self {
            sheets,
            sheet_names,
            path: path.to_owned(),
        })
    }

    fn load_sheet_eager<DT: CellType + DataType + Debug>(
        data: Range<DT>,
        pagination: Pagination,
        header: Header,
        sample_rows: Option<usize>,
    ) -> FastExcelResult<RecordBatch> {
        let column_names = sheet_column_names_from_header_and_range(&header, &data);

        let offset = header.offset() + pagination.offset();
        let limit = {
            let upper_bound = data.height();
            if let Some(n_rows) = pagination.n_rows() {
                // minimum value between (offset+n_rows) and the data's height
                std::cmp::min(offset + n_rows, upper_bound)
            } else {
                upper_bound
            }
        };

        let schema_sample_rows = get_schema_sample_rows(sample_rows, offset, limit);

        let schema = arrow_schema_from_column_names_and_range(
            &data,
            &column_names,
            offset,
            schema_sample_rows,
        )
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
        n_rows = None,
        schema_sample_rows = 1_000,
        use_columns = None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_sheet_by_name(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        use_columns: Option<&PyList>,
    ) -> PyResult<ExcelSheet> {
        let range = self
            .sheets
            .worksheet_range(&name)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| format!("Error while loading sheet {name}"))
            .into_pyresult()?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let selected_columns = use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | None, got {use_columns:?}")).into_pyresult()?;
        ExcelSheet::try_new(
            name,
            range,
            header,
            pagination,
            schema_sample_rows,
            selected_columns,
        )
        .into_pyresult()
    }

    #[pyo3(signature = (
        name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None,
        schema_sample_rows = 1_000,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_sheet_by_name_eager(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let range = self
            .sheets
            .worksheet_range(&name)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| format!("Error while loading sheet {name}"))
            .into_pyresult()?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let rb = ExcelReader::load_sheet_eager(range, pagination, header, schema_sample_rows)
            .with_context(|| "could not load sheet eagerly")
            .into_pyresult()?;
        rb.to_pyarrow(py)
    }

    #[pyo3(signature = (
        idx,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None,
        schema_sample_rows = 1_000,
        use_columns = None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_sheet_by_idx(
        &mut self,
        idx: usize,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        use_columns: Option<&PyList>,
    ) -> PyResult<ExcelSheet> {
        let name = self
            .sheet_names
            .get(idx)
            .ok_or_else(|| FastExcelErrorKind::SheetNotFound(IdxOrName::Idx(idx)).into())
            .with_context(|| {
                format!(
                    "Sheet index {idx} is out of range. File has {} sheets",
                    self.sheet_names.len()
                )
            })
            .into_pyresult()?
            .to_owned();

        let range = self
            .sheets
            .worksheet_range_at(idx)
            // Returns Option<Result<Range<Data>, Self::Error>>, so we convert the Option into a
            // SheetNotFoundError and unwrap it
            .ok_or_else(|| FastExcelErrorKind::SheetNotFound(IdxOrName::Idx(idx)).into())
            .into_pyresult()?
            // And here, we convert the calamine error in an owned error and unwrap it
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .into_pyresult()?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let selected_columns = use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | None, got {use_columns:?}")).into_pyresult()?;
        ExcelSheet::try_new(
            name,
            range,
            header,
            pagination,
            schema_sample_rows,
            selected_columns,
        )
        .into_pyresult()
    }

    #[pyo3(signature = (
        idx,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None,
        schema_sample_rows = 1_000,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_sheet_by_idx_eager(
        &mut self,
        idx: usize,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let range = self
            .sheets
            .worksheet_range_at(idx)
            // Returns Option<Result<Range<Data>, Self::Error>>, so we convert the Option into a
            // SheetNotFoundError and unwrap it
            .ok_or_else(|| FastExcelErrorKind::SheetNotFound(SheetIdxOrName::Idx(idx)).into())
            .into_pyresult()?
            // And here, we convert the calamine error in an owned error and unwrap it
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .into_pyresult()?;
        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let rb = ExcelReader::load_sheet_eager(range, pagination, header, schema_sample_rows)
            .with_context(|| "could not load sheet eagerly")
            .into_pyresult()?;
        rb.to_pyarrow(py)
    }
}
