use std::{
    fs::File,
    io::{BufReader, Cursor},
};

use calamine::{open_workbook_auto, open_workbook_auto_from_rs, Data, Range, Reader, Sheets};
use pyo3::{pyclass, pymethods, types::PyDict, PyAny, PyResult};

use crate::error::{
    py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    IdxOrName,
};

use super::{
    dtype::DTypeMap,
    excelsheet::{Header, Pagination, SelectedColumns},
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

    #[allow(dead_code)]
    fn sheet_names(&self) -> Vec<String> {
        match self {
            Self::File(sheets) => sheets.sheet_names(),
            Self::Bytes(sheets) => sheets.sheet_names(),
        }
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

    fn build_selected_columns(use_columns: Option<&PyAny>) -> FastExcelResult<SelectedColumns> {
        use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | None, got {use_columns:?}"))
    }

    fn load_sheet(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        use_columns: Option<&PyAny>,
        dtypes: Option<&PyDict>,
    ) -> FastExcelResult<ExcelSheet> {
        let range = self.sheets.worksheet_range(&name)?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range)?;
        let selected_columns = Self::build_selected_columns(use_columns)?;
        let dtypes = Self::build_dtypes(dtypes)?;
        ExcelSheet::try_new(
            name,
            range,
            header,
            pagination,
            schema_sample_rows,
            selected_columns,
            dtypes,
        )
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
        self.load_sheet(
            name,
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            use_columns,
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

        self.load_sheet(
            name,
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            use_columns,
            dtypes,
        )
        .into_pyresult()
    }
}
