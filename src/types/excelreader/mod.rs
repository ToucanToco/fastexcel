#[cfg(feature = "python")]
mod python;

use std::{
    fs::File,
    io::{BufReader, Cursor},
};

#[cfg(feature = "python")]
use pyo3::pyclass;

use calamine::{
    CellType, Data, DataRef, HeaderRow, Range, Reader, ReaderRef, Sheet as CalamineSheet, Sheets,
    Table, open_workbook_auto, open_workbook_auto_from_rs,
};

use crate::{
    ExcelSheet,
    error::{ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DTypeCoercion, DTypes},
        excelsheet::{Header, Pagination, SelectedColumns},
        idx_or_name::IdxOrName,
    },
};

use super::excelsheet::table::{extract_table_names, extract_table_range};

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
            Self::Bytes(sheets) => {
                sheets.with_header_row(header_row);
                self
            }
        }
    }

    fn worksheet_range_ref(&mut self, name: &str) -> FastExcelResult<Range<DataRef<'_>>> {
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

#[derive(Debug)]
pub struct LoadSheetOptions {
    pub header_row: Option<usize>,
    pub column_names: Option<Vec<String>>,
    pub skip_rows: Option<usize>,
    pub n_rows: Option<usize>,
    pub schema_sample_rows: Option<usize>,
    pub dtype_coercion: DTypeCoercion,
    pub selected_columns: SelectedColumns,
    pub dtypes: Option<DTypes>,
}

impl Default for LoadSheetOptions {
    fn default() -> Self {
        Self {
            header_row: Some(0),
            column_names: Default::default(),
            skip_rows: Default::default(),
            n_rows: Default::default(),
            schema_sample_rows: Default::default(),
            dtype_coercion: Default::default(),
            selected_columns: Default::default(),
            dtypes: Default::default(),
        }
    }
}

impl LoadSheetOptions {
    /// Returns a `calamine::HeaderRow`, indicating the first row of the range to be read. For us,
    /// `header_row` can be `None` (meaning there is no header and we should start reading the data
    /// at the beginning of the sheet)
    fn calamine_header_row(&self) -> HeaderRow {
        match (self.header_row, self.skip_rows) {
            (None, None) | (Some(0), None) => HeaderRow::FirstNonEmptyRow,
            (None, Some(_)) => HeaderRow::Row(0),
            (Some(row), _) => HeaderRow::Row(row as u32),
        }
    }

    /// Returns the row number of the first data row to read, if defined
    fn data_header_row(&self) -> Option<usize> {
        self.header_row.and(Some(0))
    }

    fn pagination<CT: CellType>(&self, range: &Range<CT>) -> FastExcelResult<Pagination> {
        Pagination::try_new(self.skip_rows.unwrap_or(0), self.n_rows, range)
    }
}

#[cfg_attr(feature = "python", pyclass(name = "_ExcelReader"))]
pub struct ExcelReader {
    sheets: ExcelSheets,
    sheet_metadata: Vec<CalamineSheet>,
    source: String,
}

impl ExcelReader {
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

    fn find_sheet_meta(&self, idx_or_name: IdxOrName) -> FastExcelResult<&CalamineSheet> {
        match idx_or_name {
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
                .with_context(|| {
                    format!(
                        "Sheet index {idx} is out of range. File has {} sheets.",
                        self.sheet_metadata.len()
                    )
                }),
        }
    }

    /// Load a sheet from the Excel file.
    pub fn load_sheet(
        &mut self,
        idx_or_name: IdxOrName,
        opts: LoadSheetOptions,
    ) -> FastExcelResult<ExcelSheet> {
        let calamine_header_row = opts.calamine_header_row();
        let data_header_row = opts.data_header_row();

        let sheet_meta = self.find_sheet_meta(idx_or_name)?.to_owned();

        let range = self
            .sheets
            .with_header_row(calamine_header_row)
            .worksheet_range(&sheet_meta.name)?;

        let pagination = opts.pagination(&range)?;

        let header = Header::new(data_header_row, opts.column_names);

        ExcelSheet::try_new(
            sheet_meta,
            range.into(),
            header,
            pagination,
            opts.schema_sample_rows,
            opts.dtype_coercion,
            opts.selected_columns,
            opts.dtypes,
        )
    }

    pub fn sheet_names(&self) -> Vec<&str> {
        self.sheet_metadata
            .iter()
            .map(|s| s.name.as_str())
            .collect()
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
