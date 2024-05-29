use std::{
    fs::File,
    io::{BufReader, Cursor},
};

use arrow::{
    datatypes::{Field, Schema},
    pyarrow::ToPyArrow,
    record_batch::RecordBatch,
};
use calamine::{
    open_workbook_auto, open_workbook_auto_from_rs, Data, DataRef, Range, Reader, Sheets,
};
use pyo3::{prelude::PyObject, pyclass, pymethods, IntoPy, PyAny, PyResult, Python};

use crate::{
    error::{
        py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    },
    types::{dtype::DTypeMap, idx_or_name::IdxOrName},
};

use crate::utils::schema::get_schema_sample_rows;

use super::excelsheet::record_batch_from_data_and_schema;
use super::excelsheet::{
    column_info::{build_available_columns, build_available_columns_info},
    sheet_data::ExcelSheetData,
};
use super::excelsheet::{ExcelSheet, Header, Pagination, SelectedColumns};

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
    fn build_selected_columns(use_columns: Option<&PyAny>) -> FastExcelResult<SelectedColumns> {
        use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | None, got {use_columns:?}"))
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

    fn load_sheet_eager(
        data: &ExcelSheetData,
        pagination: Pagination,
        header: Header,
        sample_rows: Option<usize>,
        selected_columns: &SelectedColumns,
        dtypes: Option<&DTypeMap>,
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
        )?;

        let fields = available_columns
            .iter()
            .map(Into::<Field>::into)
            .collect::<Vec<_>>();

        let schema = Schema::new(fields);

        record_batch_from_data_and_schema(schema, data, offset, limit)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_sheet(
        &mut self,
        name: String,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        use_columns: Option<&PyAny>,
        dtypes: Option<DTypeMap>,
        eager: bool,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let header = Header::new(header_row, column_names);
        let selected_columns = Self::build_selected_columns(use_columns).into_pyresult()?;
        if eager && self.sheets.supports_by_ref() {
            let range = self.sheets.worksheet_range_ref(&name).into_pyresult()?;
            let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
            Self::load_sheet_eager(
                &range.into(),
                pagination,
                header,
                schema_sample_rows,
                &selected_columns,
                dtypes.as_ref(),
            )
            .into_pyresult()
            .and_then(|rb| rb.to_pyarrow(py))
        } else {
            let range = self.sheets.worksheet_range(&name).into_pyresult()?;
            let pagination = Pagination::new(skip_rows, n_rows, &range).into_pyresult()?;
            let sheet = ExcelSheet::try_new(
                name,
                range.into(),
                header,
                pagination,
                schema_sample_rows,
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
        idx_or_name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = 0,
        n_rows = None,
        schema_sample_rows = 1_000,
        use_columns = None,
        dtypes = None,
        eager = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_sheet(
        &mut self,
        idx_or_name: &PyAny,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: usize,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        use_columns: Option<&PyAny>,
        dtypes: Option<DTypeMap>,
        eager: bool,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let name = idx_or_name
            .try_into()
            .and_then(|idx_or_name| match idx_or_name {
                IdxOrName::Name(name) => {
                    if self.sheet_names.contains(&name) {
                        Ok(name)
                    } else {
                        Err(FastExcelErrorKind::SheetNotFound(IdxOrName::Name(name.clone())).into()).with_context(||  {
                            let available_sheets = self.sheet_names.iter().map(|s| format!("\"{s}\"")).collect::<Vec<_>>().join(", ");
                            format!(
                                "Sheet \"{name}\" not found in file. Available sheets: {available_sheets}."
                            )
                        })
                    }
                }
                IdxOrName::Idx(idx) => self
                    .sheet_names
                    .get(idx)
                    .ok_or_else(|| FastExcelErrorKind::SheetNotFound(IdxOrName::Idx(idx)).into())
                    .with_context(|| {
                        format!(
                            "Sheet index {idx} is out of range. File has {} sheets.",
                            self.sheet_names.len()
                        )
                    })
                    .map(ToOwned::to_owned),
            })
            .into_pyresult()?;

        self.build_sheet(
            name,
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            use_columns,
            dtypes,
            eager,
            py,
        )
    }
}
