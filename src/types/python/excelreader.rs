use std::{
    fs::File,
    io::{BufReader, Cursor},
};

use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};
use pyo3::{prelude::PyObject, pyclass, pymethods, Bound, IntoPy, PyAny, PyResult, Python};

use calamine::{
    open_workbook_auto, open_workbook_auto_from_rs, Data, DataRef, HeaderRow, Range, Reader,
    ReaderRef, Sheet as CalamineSheet, Sheets, Table,
};

use crate::{
    data::{record_batch_from_data_and_columns, ExcelSheetData},
    error::{
        py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    },
    types::{
        dtype::{DTypeCoercion, DTypes},
        idx_or_name::IdxOrName,
        python::excelsheet::table::{extract_table_names, extract_table_range},
    },
    utils::schema::get_schema_sample_rows,
};

use pyo3::types::PyString;

use super::excelsheet::{
    column_info::{build_available_columns, build_available_columns_info},
    ExcelSheet, Header, Pagination, SelectedColumns,
};
use super::table::ExcelTable;

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
    fn sheet_metadata(&self) -> &[CalamineSheet] {
        match self {
            ExcelSheets::File(sheets) => sheets.sheets_metadata(),
            ExcelSheets::Bytes(sheets) => sheets.sheets_metadata(),
        }
    }

    fn table_names(&mut self, sheet_name: Option<&str>) -> FastExcelResult<Vec<String>> {
        match self {
            Self::File(sheets) => {
                extract_table_names(sheets, sheet_name)?.map(|v| v.into_iter().cloned().collect())
            }
            Self::Bytes(sheets) => {
                extract_table_names(sheets, sheet_name)?.map(|v| v.into_iter().cloned().collect())
            }
        }
    }

    fn supports_by_ref(&self) -> bool {
        matches!(
            self,
            Self::File(Sheets::Xlsx(_)) | Self::Bytes(Sheets::Xlsx(_))
        )
    }

    fn with_header_row(&mut self, header_row: HeaderRow) -> &mut Self {
        match self {
            Self::File(sheets) => {
                sheets.with_header_row(header_row);
                self
            }
            Self::Bytes(ref mut sheets) => {
                sheets.with_header_row(header_row);
                self
            }
        }
    }

    fn worksheet_range_ref(&mut self, name: &str) -> FastExcelResult<Range<DataRef>> {
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

    fn get_table(&mut self, name: &str) -> FastExcelResult<Table<Data>> {
        match self {
            Self::File(sheets) => extract_table_range(name, sheets)?,
            Self::Bytes(sheets) => extract_table_range(name, sheets)?,
        }
    }
}

#[pyclass(name = "_ExcelReader")]
pub(crate) struct ExcelReader {
    sheets: ExcelSheets,
    sheet_metadata: Vec<CalamineSheet>,
    source: String,
}

impl ExcelReader {
    fn build_selected_columns(
        use_columns: Option<&Bound<'_, PyAny>>,
    ) -> FastExcelResult<SelectedColumns> {
        use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None, got {use_columns:?}"))
    }

    // NOTE: Not implementing TryFrom here, because we're aren't building the file from the passed
    // string, but rather from the file pointed by it. Semantically, try_from_path is clearer
    pub(crate) fn try_from_path(path: &str) -> FastExcelResult<Self> {
        let sheets = open_workbook_auto(path)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| format!("Could not open workbook at {path}"))?;
        let sheet_metadata = sheets.sheets_metadata().to_owned();
        Ok(Self {
            sheets: ExcelSheets::File(sheets),
            sheet_metadata,
            source: path.to_owned(),
        })
    }

    fn load_sheet_eager(
        data: &ExcelSheetData,
        pagination: Pagination,
        header: Header,
        sample_rows: Option<usize>,
        selected_columns: &SelectedColumns,
        dtypes: Option<&DTypes>,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<RecordBatch> {
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

        let sample_rows_limit = get_schema_sample_rows(sample_rows, offset, limit);
        let available_columns_info = build_available_columns_info(data, selected_columns, &header)?;

        let available_columns = build_available_columns(
            available_columns_info,
            data,
            offset,
            sample_rows_limit,
            dtypes,
            dtype_coercion,
        )?;

        let final_columns = selected_columns.select_columns(&available_columns)?;

        record_batch_from_data_and_columns(&final_columns, data, offset, limit)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_sheet(
        &mut self,
        sheet_meta: CalamineSheet,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: Option<usize>,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        use_columns: Option<&Bound<'_, PyAny>>,
        dtypes: Option<DTypes>,
        eager: bool,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        // calamine `header_row` is the first row of the range to be read.
        // For us `header_row` can be `None` (meaning there is no header and we should start reading
        // the data at the beginning)
        let calamine_header_row = match (header_row, skip_rows) {
            (None, None) | (Some(0), None) => HeaderRow::FirstNonEmptyRow,
            (None, Some(_)) => HeaderRow::Row(0),
            (Some(row), _) => HeaderRow::Row(row as u32),
        };
        // And our header row is simply the first row of the data if defined.
        let data_header_row = header_row.and(Some(0));

        let header = Header::new(data_header_row, column_names);
        let selected_columns = Self::build_selected_columns(use_columns).into_pyresult()?;

        if eager && self.sheets.supports_by_ref() {
            let range = self
                .sheets
                .with_header_row(calamine_header_row)
                .worksheet_range_ref(&sheet_meta.name)
                .into_pyresult()?;
            let pagination =
                Pagination::new(skip_rows.unwrap_or(0), n_rows, &range).into_pyresult()?;
            Self::load_sheet_eager(
                &range.into(),
                pagination,
                header,
                schema_sample_rows,
                &selected_columns,
                dtypes.as_ref(),
                &dtype_coercion,
            )
            .into_pyresult()
            .and_then(|rb| rb.to_pyarrow(py))
        } else {
            let range = self
                .sheets
                .with_header_row(calamine_header_row)
                .worksheet_range(&sheet_meta.name)
                .into_pyresult()?;
            let pagination =
                Pagination::new(skip_rows.unwrap_or(0), n_rows, &range).into_pyresult()?;
            let sheet = ExcelSheet::try_new(
                sheet_meta,
                range.into(),
                header,
                pagination,
                schema_sample_rows,
                dtype_coercion,
                selected_columns,
                dtypes,
            )
            .into_pyresult()?;

            if eager {
                sheet.to_arrow(py)
            } else {
                Ok(sheet.into_py(py))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_table(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        use_columns: Option<&Bound<'_, PyAny>>,
        dtypes: Option<DTypes>,
        eager: bool,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let selected_columns = Self::build_selected_columns(use_columns).into_pyresult()?;

        let table = self.sheets.get_table(&name).into_pyresult()?;
        let header = {
            match (column_names, header_row) {
                (None, None) => Header::With(table.columns().into()),
                (None, Some(row)) => Header::At(row),
                (Some(column_names), _) => Header::With(column_names),
            }
        };

        let pagination = Pagination::new(skip_rows, n_rows, table.data()).into_pyresult()?;

        let excel_table = ExcelTable::try_new(
            table,
            header,
            pagination,
            schema_sample_rows,
            dtype_coercion,
            selected_columns,
            dtypes,
        )
        .into_pyresult()?;

        if eager {
            excel_table.to_arrow(py)
        } else {
            Ok(excel_table.into_py(py))
        }
    }
}

impl TryFrom<&[u8]> for ExcelReader {
    type Error = FastExcelError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let cursor = Cursor::new(bytes.to_vec());
        let sheets = open_workbook_auto_from_rs(cursor)
            .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
            .with_context(|| "Could not open workbook from bytes")?;
        let sheet_metadata = sheets.sheets_metadata().to_owned();
        Ok(Self {
            sheets: ExcelSheets::Bytes(sheets),
            sheet_metadata,
            source: "bytes".to_owned(),
        })
    }
}

#[pymethods]
impl ExcelReader {
    pub fn __repr__(&self) -> String {
        format!("ExcelReader<{}>", &self.source)
    }

    #[pyo3(signature = (sheet_name = None))]
    pub fn table_names(&mut self, sheet_name: Option<&str>) -> PyResult<Vec<String>> {
        self.sheets.table_names(sheet_name).into_pyresult()
    }

    #[pyo3(signature = (
        idx_or_name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = None,
        n_rows = None,
        schema_sample_rows = 1_000,
        dtype_coercion = DTypeCoercion::Coerce,
        use_columns = None,
        dtypes = None,
        eager = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_sheet(
        &mut self,
        idx_or_name: &Bound<'_, PyAny>,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: Option<usize>,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        use_columns: Option<&Bound<'_, PyAny>>,
        dtypes: Option<DTypes>,
        eager: bool,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        // Cannot use NonZeroUsize in the parameters, as it is not supported by pyo3
        if let Some(0) = schema_sample_rows {
            return Err(FastExcelErrorKind::InvalidParameters(
                "schema_sample_rows cannot be 0, as it would prevent dtype inferring".to_string(),
            )
            .into())
            .into_pyresult();
        }
        let sheet = idx_or_name
            .try_into()
            .and_then(|idx_or_name| match idx_or_name {
                IdxOrName::Name(name) => {
                    if let Some(sheet) = self.sheet_metadata.iter().find(|s| s.name == name) {
                        Ok(sheet)
                    } else {
                        Err(FastExcelErrorKind::SheetNotFound(IdxOrName::Name(name.clone())).into()).with_context(||  {
                            let available_sheets = self.sheet_metadata.iter().map(|s| format!("\"{}\"", s.name)).collect::<Vec<_>>().join(", ");
                            format!(
                                "Sheet \"{name}\" not found in file. Available sheets: {available_sheets}."
                            )
                        })
                    }
                }
                IdxOrName::Idx(idx) => self
                    .sheet_metadata
                    .get(idx)
                    .ok_or_else(|| FastExcelErrorKind::SheetNotFound(IdxOrName::Idx(idx)).into())
                    .with_context(|| format!(
                        "Sheet index {idx} is out of range. File has {} sheets.",
                        self.sheet_metadata.len()
                    )
                ),
            })
            .into_pyresult()?.to_owned();

        self.build_sheet(
            sheet,
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            dtype_coercion,
            use_columns,
            dtypes,
            eager,
            py,
        )
    }

    #[pyo3(signature = (
        name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None,
        schema_sample_rows = 1_000,
        dtype_coercion = DTypeCoercion::Coerce,
        use_columns = None,
        dtypes = None,
        eager = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_table(
        &mut self,
        name: &Bound<'_, PyString>,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        use_columns: Option<&Bound<'_, PyAny>>,
        dtypes: Option<DTypes>,
        eager: bool,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        // Cannot use NonZeroUsize in the parameters, as it is not supported by pyo3
        if let Some(0) = schema_sample_rows {
            return Err(FastExcelErrorKind::InvalidParameters(
                "schema_sample_rows cannot be 0, as it would prevent dtype inferring".to_string(),
            )
            .into())
            .into_pyresult();
        }
        self.build_table(
            name.to_string(),
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            dtype_coercion,
            use_columns,
            dtypes,
            eager,
            py,
        )
    }

    #[getter]
    pub fn sheet_names(&self) -> Vec<&str> {
        self.sheet_metadata
            .iter()
            .map(|s| s.name.as_str())
            .collect()
    }
}
