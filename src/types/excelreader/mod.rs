#[cfg(feature = "python")]
mod python;

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Cursor},
    str::FromStr,
};

use calamine::{
    Data, HeaderRow, Range, Reader, Sheet as CalamineSheet, Sheets, Table, open_workbook_auto,
    open_workbook_auto_from_rs,
};
#[cfg(feature = "python")]
use calamine::{DataRef, ReaderRef};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
#[cfg(feature = "python")]
use pyo3::pyclass;

use crate::{
    ExcelSheet, ExcelTable,
    error::{ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DTypeCoercion, DTypes},
        excelsheet::{SelectedColumns, SkipRows},
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

    fn workbook_properties(&mut self) -> FastExcelResult<WorkbookProperties> {
        match self {
            Self::File(sheets) => sheets.workbook_properties(),
            Self::Bytes(sheets) => sheets.workbook_properties(),
        }
        .map(WorkbookProperties::from)
        .map_err(|err| FastExcelErrorKind::CalamineError(err).into())
        .with_context(|| "Error while reading workbook properties")
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

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "python", pyclass(name = "DefinedName", skip_from_py_object))]
pub struct DefinedName {
    pub name: String,
    pub formula: String,
}

/// A date of a workbook document property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyDate {
    /// A date with a UTC offset, as stored by xlsx, xlsb and xls files.
    Aware(DateTime<FixedOffset>),
    /// A date without a UTC offset, as stored by ods files.
    Naive(NaiveDateTime),
}

impl FromStr for PropertyDate {
    type Err = chrono::ParseError;

    /// Parses a date in one of the W3C-DTF forms that document properties use, such as
    /// "2024-01-15T08:30:00Z", "2024-01-15T08:30+01:00", "2024-01-15" or "2024", or a date
    /// without a UTC offset such as "2024-06-20T14:22:00.123456789". Dates without a time are
    /// read as midnight on their first day.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        let error = match DateTime::parse_from_rfc3339(value) {
            Ok(date) => return Ok(Self::Aware(date)),
            Err(error) => error,
        };
        if let Ok(date) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M%#z") {
            return Ok(Self::Aware(date));
        }
        if let Ok(date) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
            return Ok(Self::Naive(date));
        }
        // "YYYY-MM-DD", "YYYY-MM" and "YYYY".
        [
            value.to_owned(),
            format!("{value}-01"),
            format!("{value}-01-01"),
        ]
        .iter()
        .find_map(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
        .map(|date| Self::Naive(date.and_time(NaiveTime::MIN)))
        .ok_or(error)
    }
}

/// The document properties of a workbook, such as its creator, title and dates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(
    feature = "python",
    pyclass(name = "WorkbookProperties", get_all, eq, skip_from_py_object)
)]
pub struct WorkbookProperties {
    pub creator: Option<String>,
    pub last_modified_by: Option<String>,
    pub created: Option<PropertyDate>,
    pub modified: Option<PropertyDate>,
    pub title: Option<String>,
    pub subject: Option<String>,
    pub description: Option<String>,
    pub keywords: Option<String>,
    pub category: Option<String>,
    pub content_status: Option<String>,
    pub revision: Option<String>,
    pub version: Option<String>,
    pub application: Option<String>,
    pub app_version: Option<String>,
    pub company: Option<String>,
    pub template: Option<String>,
    pub manager: Option<String>,
    /// Custom properties, with their values as the text stored in the file.
    pub custom_properties: BTreeMap<String, String>,
}

impl From<calamine::WorkbookProperties> for WorkbookProperties {
    fn from(props: calamine::WorkbookProperties) -> Self {
        Self {
            creator: props.creator,
            last_modified_by: props.last_modified_by,
            created: props.created.and_then(|created| created.parse().ok()),
            modified: props.modified.and_then(|modified| modified.parse().ok()),
            title: props.title,
            subject: props.subject,
            description: props.description,
            keywords: props.keywords,
            category: props.category,
            content_status: props.content_status,
            revision: props.revision,
            version: props.version,
            application: props.application,
            app_version: props.app_version,
            company: props.company,
            template: props.template,
            manager: props.manager,
            custom_properties: props.custom_properties.into_iter().collect(),
        }
    }
}

/// Options for loading a sheet or table.
#[non_exhaustive]
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
    /// Skip rows at the end of the sheet/table containing only whitespace and null values.
    pub skip_whitespace_tail_rows: bool,
    /// Consider cells containing only whitespace as null values.
    pub whitespace_as_null: bool,
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
    pub(crate) fn data_header_row(&self) -> Option<usize> {
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
            skip_whitespace_tail_rows: Default::default(),
            whitespace_as_null: Default::default(),
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
            skip_whitespace_tail_rows: Default::default(),
            whitespace_as_null: Default::default(),
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

    pub fn skip_whitespace_tail_rows(mut self, skip_whitespace_tail_rows: bool) -> Self {
        self.skip_whitespace_tail_rows = skip_whitespace_tail_rows;
        self
    }

    pub fn whitespace_as_null(mut self, whitespace_as_null: bool) -> Self {
        self.whitespace_as_null = whitespace_as_null;
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

        let sheet_meta = self.find_sheet_meta(idx_or_name)?.to_owned();

        let range = self
            .sheets
            .with_header_row(calamine_header_row)
            .worksheet_range(&sheet_meta.name)?;

        ExcelSheet::try_new(sheet_meta, range.into(), opts)
    }

    /// Load a table from the Excel file.
    pub fn load_table(
        &mut self,
        name: &str,
        opts: LoadSheetOrTableOptions,
    ) -> FastExcelResult<ExcelTable> {
        let table = self.sheets.get_table(name)?;
        ExcelTable::try_new(table, opts)
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

    pub fn workbook_properties(&mut self) -> FastExcelResult<WorkbookProperties> {
        self.sheets.workbook_properties()
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

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate};
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    use super::PropertyDate;

    fn aware(value: &str) -> PropertyDate {
        PropertyDate::Aware(DateTime::parse_from_rfc3339(value).unwrap())
    }

    fn naive(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> PropertyDate {
        let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
        PropertyDate::Naive(date.and_hms_opt(hour, min, sec).unwrap())
    }

    #[rstest]
    #[case("2024-01-15T08:30:00Z", aware("2024-01-15T08:30:00Z"))]
    #[case("2024-01-15T08:30:00.25+01:00", aware("2024-01-15T08:30:00.25+01:00"))]
    #[case(" 2024-01-15T08:30:00Z\n", aware("2024-01-15T08:30:00Z"))]
    #[case("2024-01-15T08:30Z", aware("2024-01-15T08:30:00Z"))]
    #[case("2024-01-15T08:30-05:00", aware("2024-01-15T08:30:00-05:00"))]
    #[case("0000-01-01T00:00:00Z", aware("0000-01-01T00:00:00Z"))]
    #[case("2024-06-20T14:22:00", naive(2024, 6, 20, 14, 22, 0))]
    #[case("2024-01-15", naive(2024, 1, 15, 0, 0, 0))]
    #[case("2024-01", naive(2024, 1, 1, 0, 0, 0))]
    #[case("2024", naive(2024, 1, 1, 0, 0, 0))]
    fn test_parse_property_date(#[case] value: &str, #[case] expected: PropertyDate) {
        assert_eq!(value.parse::<PropertyDate>(), Ok(expected));
    }

    #[test]
    fn test_parse_property_date_with_nanoseconds() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 20)
            .unwrap()
            .and_hms_nano_opt(14, 22, 0, 123_456_789)
            .unwrap();
        assert_eq!(
            "2024-06-20T14:22:00.123456789".parse::<PropertyDate>(),
            Ok(PropertyDate::Naive(date))
        );
    }

    #[rstest]
    #[case("")]
    #[case("yesterday")]
    #[case("2024-13")]
    #[case("2024-01-15T08")]
    #[case("15/01/2024")]
    fn test_parse_property_date_invalid(#[case] value: &str) {
        assert!(value.parse::<PropertyDate>().is_err());
    }
}
