#[cfg(feature = "python")]
mod python;

use std::{
    fs::File,
    io::{BufReader, Cursor},
};

use calamine::{
    Data, HeaderRow, Range, Reader, Sheet as CalamineSheet, Sheets, Table, open_workbook_auto,
    open_workbook_auto_from_rs,
};
#[cfg(feature = "python")]
use calamine::{DataRef, ReaderRef};
#[cfg(feature = "python")]
use pyo3::pyclass;

use crate::{
    ExcelSheet, ExcelTable,
    error::{ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DTypeCoercion, DTypes},
        excelsheet::{Header, Pagination, SelectedColumns, SkipRows},
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

    fn table_names(&mut self, sheet_name: Option<&str>) -> FastExcelResult<Vec<&str>> {
        let names = match self {
            Self::File(sheets) => extract_table_names(sheets, sheet_name),
            Self::Bytes(sheets) => extract_table_names(sheets, sheet_name),
        }?;
        Ok(names.into_iter().map(String::as_str).collect())
    }

    fn defined_names(&mut self) -> FastExcelResult<Vec<DefinedName>> {
        let defined_names = match self {
            Self::File(sheets) => sheets.defined_names(),
            Self::Bytes(sheets) => sheets.defined_names(),
        }
        .to_vec()
        .into_iter()
        .map(|(name, formula)| DefinedName { name, formula })
        .collect();
        Ok(defined_names)
    }

    #[cfg(feature = "python")]
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

    #[cfg(feature = "python")]
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
            Self::File(sheets) => extract_table_range(name, sheets),
            Self::Bytes(sheets) => extract_table_range(name, sheets),
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "python", pyclass(name = "DefinedName"))]
pub struct DefinedName {
    pub name: String,
    pub formula: String,
}

/// Options for loading a sheet or table.
#[derive(Debug)]
pub struct LoadSheetOrTableOptions {
    /// The index of the row containing the column labels. If `None`, the provided headers are used.
    /// Any row before the header row is skipped.
    pub header_row: Option<usize>,
    /// The column names to use. If `None`, the column names are inferred from the header row.
    pub column_names: Option<Vec<String>>,
    /// How rows should be skipped.
    pub skip_rows: SkipRows,
    /// The number of rows to read. If `None`, all rows are read.
    pub n_rows: Option<usize>,
    /// The number of rows to sample for schema inference. If `None`, all rows are sampled.
    pub schema_sample_rows: Option<usize>,
    /// How data types should be coerced.
    pub dtype_coercion: DTypeCoercion,
    /// The columns to select.
    pub selected_columns: SelectedColumns,
    /// Override the inferred data types.
    pub dtypes: Option<DTypes>,
}

impl LoadSheetOrTableOptions {
    /// Returns a `calamine::HeaderRow`, indicating the first row of the range to be read. For us,
    /// `header_row` can be `None` (meaning there is no header and we should start reading the data
    /// at the beginning of the sheet)
    fn calamine_header_row(&self) -> HeaderRow {
        match (self.header_row, &self.skip_rows) {
            (None | Some(0), SkipRows::SkipEmptyRowsAtBeginning) => HeaderRow::FirstNonEmptyRow,
            (None, _) => HeaderRow::Row(0),
            (Some(row), _) => HeaderRow::Row(row as u32),
        }
    }

    /// Returns the row number of the first data row to read, if defined
    fn data_header_row(&self) -> Option<usize> {
        self.header_row.and(Some(0))
    }

    /// Returns a new `LoadSheetOrTableOptions` instance for loading a sheet. `header_row` is set to
    /// `Some(0)`
    pub fn new_for_sheet() -> Self {
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

    /// Returns a new `LoadSheetOrTableOptions` instance for loading a sheet. `header_row` is set to
    /// `None`
    pub fn new_for_table() -> Self {
        Self {
            header_row: None,
            column_names: Default::default(),
            skip_rows: Default::default(),
            n_rows: Default::default(),
            schema_sample_rows: Default::default(),
            dtype_coercion: Default::default(),
            selected_columns: Default::default(),
            dtypes: Default::default(),
        }
    }

    pub fn header_row(mut self, header_row: usize) -> Self {
        self.header_row = Some(header_row);
        self
    }

    pub fn no_header_row(mut self) -> Self {
        self.header_row = None;
        self
    }

    pub fn column_names<I: IntoIterator<Item = impl Into<String>>>(
        mut self,
        column_names: I,
    ) -> Self {
        self.column_names = Some(column_names.into_iter().map(Into::into).collect());
        self
    }

    pub fn skip_rows(mut self, skip_rows: SkipRows) -> Self {
        self.skip_rows = skip_rows;
        self
    }

    pub fn n_rows(mut self, n_rows: usize) -> Self {
        self.n_rows = Some(n_rows);
        self
    }

    pub fn schema_sample_rows(mut self, schema_sample_rows: usize) -> Self {
        self.schema_sample_rows = Some(schema_sample_rows);
        self
    }

    pub fn dtype_coercion(mut self, dtype_coercion: DTypeCoercion) -> Self {
        self.dtype_coercion = dtype_coercion;
        self
    }

    pub fn selected_columns(mut self, selected_columns: SelectedColumns) -> Self {
        self.selected_columns = selected_columns;
        self
    }

    pub fn with_dtypes(mut self, dtypes: DTypes) -> Self {
        self.dtypes = Some(dtypes);
        self
    }
}

/// Represents an open Excel file and allows to access its sheets and tables.
#[cfg_attr(feature = "python", pyclass(name = "_ExcelReader"))]
pub struct ExcelReader {
    sheets: ExcelSheets,
    sheet_metadata: Vec<CalamineSheet>,
    #[cfg(feature = "python")]
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
            #[cfg(feature = "python")]
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
        opts: LoadSheetOrTableOptions,
    ) -> FastExcelResult<ExcelSheet> {
        let calamine_header_row = opts.calamine_header_row();
        let data_header_row = opts.data_header_row();

        let sheet_meta = self.find_sheet_meta(idx_or_name)?.to_owned();

        let range = self
            .sheets
            .with_header_row(calamine_header_row)
            .worksheet_range(&sheet_meta.name)?;

        let pagination = Pagination::try_new(opts.skip_rows, opts.n_rows, &range)?;

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

    /// Load a table from the Excel file.
    pub fn load_table(
        &mut self,
        name: &str,
        opts: LoadSheetOrTableOptions,
    ) -> FastExcelResult<ExcelTable> {
        let table = self.sheets.get_table(name)?;
        let pagination = Pagination::try_new(opts.skip_rows, opts.n_rows, table.data())?;

        let header = match (opts.column_names, opts.header_row) {
            (None, None) => Header::With(table.columns().into()),
            (None, Some(row)) => Header::At(row),
            (Some(column_names), _) => Header::With(column_names),
        };

        ExcelTable::try_new(
            table,
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

    pub fn table_names(&mut self, sheet_name: Option<&str>) -> FastExcelResult<Vec<&str>> {
        self.sheets.table_names(sheet_name)
    }

    pub fn defined_names(&mut self) -> FastExcelResult<Vec<DefinedName>> {
        self.sheets.defined_names()
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
            #[cfg(feature = "python")]
            source: "bytes".to_owned(),
        })
    }
}
