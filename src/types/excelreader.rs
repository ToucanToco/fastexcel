use std::fmt::Debug;
use std::{
    fs::File,
    io::{BufReader, Cursor},
};

use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};
use calamine::{
    open_workbook_auto, open_workbook_auto_from_rs, CellType, Data, DataRef, DataType, Range,
    Reader, Sheets,
};
use pyo3::{prelude::PyObject, pyclass, pymethods, types::PyDict, PyAny, PyResult, Python};

use crate::error::{
    py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    IdxOrName,
};

use crate::types::excelsheet::sheet_column_names_from_header_and_range;
use crate::utils::arrow::arrow_schema_from_column_names_and_range;
use crate::utils::schema::get_schema_sample_rows;

use super::excelsheet::{record_batch_from_data_and_schema, SelectedColumns};
use super::{
    dtype::DTypeMap,
    excelsheet::{Header, Pagination},
    ExcelSheet,
};

enum ExcelSheets {
    File(Sheets<BufReader<File>>),
    Bytes(Sheets<Cursor<Vec<u8>>>),
}

impl ExcelSheets {
    fn worksheet_range(&mut self, name: &str) -> FastExcelResult<Range<Data>> {
        match self {
            Self::File(sheets) => sheets.worksheet_range(name),
            Self::Bytes(sheets) => sheets.worksheet_range(name),
        }
        .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
        .with_context(|| format!("Error while loading sheet {name}"))
    }

    fn worksheet_range_at(&mut self, idx: usize) -> FastExcelResult<Range<Data>> {
        match self {
            Self::File(sheets) => sheets.worksheet_range_at(idx),
            Self::Bytes(sheets) => sheets.worksheet_range_at(idx),
        }
        // Returns Option<Result<Range<Data>, Self::Error>>, so we convert the Option into a
        // SheetNotFoundError
        .ok_or_else(|| {
            FastExcelError::from(FastExcelErrorKind::SheetNotFound(IdxOrName::Idx(idx)))
        })?
        // And here, we convert the calamine error in an owned error and unwrap it
        .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
    }

    #[allow(dead_code)]
    fn sheet_names(&self) -> Vec<String> {
        match self {
            Self::File(sheets) => sheets.sheet_names(),
            Self::Bytes(sheets) => sheets.sheet_names(),
        }
    }

    fn supports_by_ref(&self) -> bool {
        matches!(
            self,
            Self::File(Sheets::Xlsx(_)) | Self::Bytes(Sheets::Xlsx(_))
        )
    }

    fn worksheet_range_ref<'a>(&'a mut self, name: &str) -> FastExcelResult<Range<DataRef<'a>>> {
        match self {
            ExcelSheets::File(Sheets::Xlsx(sheets)) => Ok(sheets.worksheet_range_ref(name)?),
            ExcelSheets::Bytes(Sheets::Xlsx(sheets)) => Ok(sheets.worksheet_range_ref(name)?),
            _ => Err(FastExcelErrorKind::Internal(
                "sheets do not support worksheet_range_ref".to_string(),
            )
            .into()),
        }
        .with_context(|| format!("Error while loading sheet {name}"))
    }
}

#[pyclass(name = "_ExcelReader")]
pub(crate) struct ExcelReader {
    sheets: ExcelSheets,
    #[pyo3(get)]
    sheet_names: Vec<String>,
    source: String,
}

impl ExcelReader {
    fn build_selected_columns(use_columns: Option<&PyAny>) -> PyResult<SelectedColumns> {
        use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | None, got {use_columns:?}")).into_pyresult()
    }

    // NOTE: Not implementing TryFrom here, because we're aren't building the file from the passed
    // string, but rather from the file pointed by it. Semantically, try_from_path is clearer
    pub(crate) fn try_from_path(path: &str) -> FastExcelResult<Self> {
        let sheets = open_workbook_auto(path)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| format!("Could not open workbook at {path}"))?;
        let sheet_names = sheets.sheet_names().to_owned();
        Ok(Self {
            sheets: ExcelSheets::File(sheets),
            sheet_names,
            source: path.to_owned(),
        })
    }

    fn build_dtypes(raw_dtypes: Option<&PyDict>) -> FastExcelResult<Option<DTypeMap>> {
        match raw_dtypes {
            None => Ok(None),
            Some(py_dict) => py_dict.try_into().map(Some),
        }
        .with_context(|| "could not parse provided dtypes")
    }

    fn load_sheet_eager<DT: CellType + DataType + Debug>(
        data: Range<DT>,
        pagination: Pagination,
        header: Header,
        sample_rows: Option<usize>,
        selected_columns: &SelectedColumns,
        dtypes: Option<&DTypeMap>,
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
            selected_columns,
            dtypes,
        )
        .with_context(|| "could not build arrow schema")?;

        record_batch_from_data_and_schema(schema, &data, offset, limit)
    }
}

impl TryFrom<&[u8]> for ExcelReader {
    type Error = FastExcelError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let cursor = Cursor::new(bytes.to_vec());
        let sheets = open_workbook_auto_from_rs(cursor)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| "Could not open workbook from bytes")?;
        let sheet_names = sheets.sheet_names().to_owned();
        Ok(Self {
            sheets: ExcelSheets::Bytes(sheets),
            sheet_names,
            source: "bytes".to_owned(),
        })
    }
}

#[pymethods]
impl ExcelReader {
    pub fn __repr__(&self) -> String {
        format!("ExcelReader<{}>", &self.source)
    }

    #[pyo3(signature = (
        name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None,
        schema_sample_rows = 1_000,
        use_columns = None,
        dtypes = None,
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
        // pyo3 forces us to take an Option in case the default value is None
        use_columns: Option<&PyAny>,
        dtypes: Option<&PyDict>,
    ) -> PyResult<ExcelSheet> {
        let range = self.sheets.worksheet_range(&name).into_pyresult()?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let selected_columns = Self::build_selected_columns(use_columns)?;
        let dtypes = Self::build_dtypes(dtypes).into_pyresult()?;
        ExcelSheet::try_new(
            name,
            range,
            header,
            pagination,
            schema_sample_rows,
            selected_columns,
            dtypes,
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
        use_columns = None,
        dtypes = None,
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
        use_columns: Option<&PyAny>,
        dtypes: Option<&PyDict>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let header = Header::new(header_row, column_names);
        let dtypes = Self::build_dtypes(dtypes).into_pyresult()?;

        let rb = if self.sheets.supports_by_ref() {
            let range = self.sheets.worksheet_range_ref(&name).into_pyresult()?;

            let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
            let selected_columns = Self::build_selected_columns(use_columns)?;
            ExcelReader::load_sheet_eager(
                range,
                pagination,
                header,
                schema_sample_rows,
                &selected_columns,
                dtypes.as_ref(),
            )
            .with_context(|| "could not load sheet eagerly")
        } else {
            let range = self.sheets.worksheet_range(&name).into_pyresult()?;

            let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
            let selected_columns = Self::build_selected_columns(use_columns)?;
            ExcelReader::load_sheet_eager(
                range,
                pagination,
                header,
                schema_sample_rows,
                &selected_columns,
                dtypes.as_ref(),
            )
            .with_context(|| "could not load sheet eagerly")
        }
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
        use_columns = None,
        dtypes = None,
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
        use_columns: Option<&PyAny>,
        dtypes: Option<&PyDict>,
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

        let range = self.sheets.worksheet_range_at(idx).into_pyresult()?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let selected_columns = Self::build_selected_columns(use_columns)?;

        let dtypes = Self::build_dtypes(dtypes).into_pyresult()?;
        ExcelSheet::try_new(
            name,
            range,
            header,
            pagination,
            schema_sample_rows,
            selected_columns,
            dtypes,
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
        use_columns = None,
        dtypes = None,
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
        use_columns: Option<&PyAny>,
        dtypes: Option<&PyDict>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let range = self.sheets.worksheet_range_at(idx).into_pyresult()?;
        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let selected_columns = Self::build_selected_columns(use_columns)?;
        let dtypes = Self::build_dtypes(dtypes).into_pyresult()?;
        let rb = ExcelReader::load_sheet_eager(
            range,
            pagination,
            header,
            schema_sample_rows,
            &selected_columns,
            dtypes.as_ref(),
        )
        .with_context(|| "could not load sheet eagerly")
        .into_pyresult()?;
        rb.to_pyarrow(py)
    }
}
