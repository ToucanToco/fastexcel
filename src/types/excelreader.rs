use std::{
    fs::File,
    io::{BufReader, Cursor},
};

use calamine::{
    open_workbook_auto, open_workbook_auto_from_rs, Data, Error, Range, Reader, Sheets,
};
use pyo3::{pyclass, pymethods, PyAny, PyResult};

use crate::error::{
    py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    IdxOrName,
};

use super::{
    excelsheet::{Header, Pagination},
    ExcelSheet,
};

enum ExcelSheets {
    File(Sheets<BufReader<File>>),
    Bytes(Sheets<Cursor<Vec<u8>>>),
}

impl ExcelSheets {
    fn worksheet_range(&mut self, name: &str) -> Result<Range<Data>, Error> {
        match self {
            Self::File(sheets) => sheets.worksheet_range(name),
            Self::Bytes(sheets) => sheets.worksheet_range(name),
        }
    }

    fn worksheet_range_at(&mut self, idx: usize) -> Option<Result<Range<Data>, Error>> {
        match self {
            Self::File(sheets) => sheets.worksheet_range_at(idx),
            Self::Bytes(sheets) => sheets.worksheet_range_at(idx),
        }
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
        // pyo3 forces us to take an Option in case the default value is None
        use_columns: Option<&PyAny>,
    ) -> PyResult<ExcelSheet> {
        let range = self
            .sheets
            .worksheet_range(&name)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| format!("Error while loading sheet {name}"))
            .into_pyresult()?;

        let header = Header::new(header_row, column_names);
        let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
        let selected_columns = use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | None, got {use_columns:?}")).into_pyresult()?;
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
        use_columns: Option<&PyAny>,
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
        let selected_columns = use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | None, got {use_columns:?}")).into_pyresult()?;
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
}
