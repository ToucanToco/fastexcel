use std::{cmp, collections::HashSet, str::FromStr, sync::Arc};

use crate::error::{
    py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    IdxOrName,
};

use arrow::{
    array::{
        Array, BooleanArray, Date32Array, DurationMillisecondArray, Float64Array, Int64Array,
        NullArray, StringArray, TimestampMillisecondArray,
    },
    datatypes::{DataType as ArrowDataType, Schema, TimeUnit},
    pyarrow::ToPyArrow,
    record_batch::RecordBatch,
};
use calamine::{Data as CalData, DataType, Range};
use chrono::NaiveDate;

use pyo3::{
    prelude::{pyclass, pymethods, PyObject, Python},
    types::{PyList, PyString},
    PyAny, PyResult,
};

use crate::utils::arrow::arrow_schema_from_column_names_and_range;

#[derive(Debug)]
pub(crate) enum Header {
    None,
    At(usize),
    With(Vec<String>),
}

impl Header {
    pub(crate) fn new(header_row: Option<usize>, column_names: Option<Vec<String>>) -> Self {
        match column_names {
            Some(headers) => Header::With(headers),
            None => match header_row {
                Some(row) => Header::At(row),
                None => Header::None,
            },
        }
    }

    pub(crate) fn offset(&self) -> usize {
        match self {
            Header::At(index) => index + 1,
            Header::None => 0,
            Header::With(_) => 0,
        }
    }
}

pub(crate) struct Pagination {
    skip_rows: usize,
    n_rows: Option<usize>,
}

impl Pagination {
    pub(crate) fn new(
        skip_rows: usize,
        n_rows: Option<usize>,
        range: &Range<CalData>,
    ) -> FastExcelResult<Self> {
        let max_height = range.height();
        if max_height < skip_rows {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "Too many rows skipped. Max height is {max_height}"
            ))
            .into())
        } else {
            Ok(Self { skip_rows, n_rows })
        }
    }

    pub(crate) fn offset(&self) -> usize {
        self.skip_rows
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum SelectedColumns {
    All,
    ByIndex(Vec<usize>),
    ByName(Vec<String>),
}

impl SelectedColumns {
    pub(crate) fn validate_columns(&self, column_names: &[String]) -> FastExcelResult<()> {
        match self {
            SelectedColumns::All => Ok(()),
            // If no selected indice is >= to the len of column_names, we're good
            SelectedColumns::ByIndex(indices) => indices.iter().try_for_each(|idx| {
                if idx >= &column_names.len() {
                    Err(FastExcelErrorKind::ColumnNotFound(IdxOrName::Idx(*idx)).into())
                } else {
                    Ok(())
                }
            }),
            // Every selected column must be in the provided column_names
            SelectedColumns::ByName(selected_names) => {
                selected_names.iter().try_for_each(|selected_name| {
                    if column_names.contains(selected_name) {
                        Ok(())
                    } else {
                        Err(FastExcelErrorKind::ColumnNotFound(IdxOrName::Name(
                            selected_name.to_string(),
                        ))
                        .into())
                    }
                })
            }
        }
    }

    pub(crate) fn idx_for_column(
        &self,
        col_names: &[String],
        col_name: &str,
        col_idx: usize,
    ) -> Option<usize> {
        match self {
            SelectedColumns::All => None,
            SelectedColumns::ByIndex(indices) => {
                if indices.contains(&col_idx) {
                    Some(col_idx)
                } else {
                    None
                }
            }
            SelectedColumns::ByName(names) => {
                // cannot use .contains() because we have &String and &str
                if names.iter().any(|name| name == col_name) {
                    col_names.iter().position(|name| name == col_name)
                } else {
                    None
                }
            }
        }
    }

    pub(crate) fn to_python<'p>(&self, py: Python<'p>) -> Option<&'p PyList> {
        match self {
            SelectedColumns::All => None,
            SelectedColumns::ByIndex(idx_vec) => Some(PyList::new(py, idx_vec)),
            SelectedColumns::ByName(name_vec) => Some(PyList::new(py, name_vec)),
        }
    }
}

impl TryFrom<&PyList> for SelectedColumns {
    type Error = FastExcelError;

    fn try_from(py_list: &PyList) -> FastExcelResult<Self> {
        use FastExcelErrorKind::InvalidParameters;

        if py_list.is_empty() {
            Err(InvalidParameters("list of selected columns is empty".to_string()).into())
        } else if let Ok(name_vec) = py_list.extract::<Vec<String>>() {
            Ok(Self::ByName(name_vec))
        } else if let Ok(index_vec) = py_list.extract::<Vec<usize>>() {
            Ok(Self::ByIndex(index_vec))
        } else {
            Err(
                InvalidParameters(format!("expected list[int] | list[str], got {py_list:?}"))
                    .into(),
            )
        }
    }
}

impl SelectedColumns {
    const ALPHABET: [char; 26] = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    fn col_idx_for_col_as_letter(col: &str) -> FastExcelResult<usize> {
        use FastExcelErrorKind::InvalidParameters;

        if col.len() != 1 {
            return Err(InvalidParameters(format!(
                "a column should be exactly one character, got {n_chars}",
                n_chars = col.len()
            ))
            .into());
        }

        col.chars()
            .next()
            .ok_or_else(|| {
                InvalidParameters(format!("could not get first char of column \"{col}\"")).into()
            })
            .and_then(|col_name| {
                Self::ALPHABET
                    .iter()
                    .position(|chr| chr == &col_name)
                    .ok_or_else(|| {
                        InvalidParameters(format!("Char is not a valid column name: {col_name}"))
                            .into()
                    })
            })
    }

    fn col_indices_for_letter_range(col_range: &str) -> FastExcelResult<Vec<usize>> {
        use FastExcelErrorKind::InvalidParameters;

        let col_elements = col_range.split(':').collect::<Vec<_>>();
        if col_elements.len() == 2 {
            let start = Self::col_idx_for_col_as_letter(col_elements[0])
                .with_context(|| format!("invalid start element for range \"{col_range}\""))?;
            let end = Self::col_idx_for_col_as_letter(col_elements[1])
                .with_context(|| format!("invalid end element for range \"{col_range}\""))?;

            match start.cmp(&end) {
                cmp::Ordering::Less => Ok((start..end).collect()),
                cmp::Ordering::Greater => Err(InvalidParameters(format!(
                    "end of range is before start: \"{col_range}\""
                ))
                .into()),
                cmp::Ordering::Equal => {
                    Err(InvalidParameters(format!("empty range: \"{col_range}\"")).into())
                }
            }
        } else {
            Err(InvalidParameters(format!(
                "expected range to contain exactly 2 elements, got {n_elements}: \"{col_range}\"",
                n_elements = col_elements.len()
            ))
            .into())
        }
    }
}

// Does not support columns beyond 26 for now
impl FromStr for SelectedColumns {
    type Err = FastExcelError;

    fn from_str(s: &str) -> FastExcelResult<Self> {
        let unique_col_indices: HashSet<usize> = s
            .to_uppercase()
            .split(',')
            .map(|col_or_range| {
                if col_or_range.contains(':') {
                    Self::col_indices_for_letter_range(col_or_range)
                } else {
                    Self::col_idx_for_col_as_letter(col_or_range).map(|idx| vec![idx])
                }
            })
            .collect::<FastExcelResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        let mut sorted_col_indices: Vec<usize> = unique_col_indices.into_iter().collect();
        sorted_col_indices.sort();
        Ok(Self::ByIndex(sorted_col_indices))
    }
}

impl TryFrom<Option<&PyAny>> for SelectedColumns {
    type Error = FastExcelError;

    fn try_from(py_any_opt: Option<&PyAny>) -> FastExcelResult<Self> {
        match py_any_opt {
            None => Ok(Self::All),
            Some(py_any) => {
                // Not trying to downcast to PyNone here as we assume that this would result in
                // py_any_opt being None
                if let Ok(py_str) = py_any.downcast::<PyString>() {
                    py_str
                        .to_str()
                        .map_err(|err| {
                            FastExcelErrorKind::InvalidParameters(format!(
                                "provided string is not valid unicode: {err}"
                            ))
                        })?
                        .parse()
                } else if let Ok(py_list) = py_any.downcast::<PyList>() {
                    py_list.try_into()
                } else {
                    Err(FastExcelErrorKind::InvalidParameters(format!(
                        "unsupported object type {object_type}",
                        object_type = py_any.get_type()
                    ))
                    .into())
                }
            }
            .with_context(|| {
                format!("could not determine selected columns from provided object: {py_any}")
            }),
        }
    }
}

#[pyclass(name = "_ExcelSheet")]
pub(crate) struct ExcelSheet {
    #[pyo3(get)]
    pub(crate) name: String,
    header: Header,
    pagination: Pagination,
    data: Range<CalData>,
    height: Option<usize>,
    total_height: Option<usize>,
    width: Option<usize>,
    schema_sample_rows: Option<usize>,
    selected_columns: SelectedColumns,
    available_columns: Vec<String>,
}

impl ExcelSheet {
    pub(crate) fn data(&self) -> &Range<CalData> {
        &self.data
    }

    pub(crate) fn try_new(
        name: String,
        data: Range<CalData>,
        header: Header,
        pagination: Pagination,
        schema_sample_rows: Option<usize>,
        selected_columns: SelectedColumns,
    ) -> FastExcelResult<Self> {
        let mut sheet = ExcelSheet {
            name,
            header,
            pagination,
            data,
            schema_sample_rows,
            selected_columns,
            height: None,
            total_height: None,
            width: None,
            // an empty vec as it will be replaced
            available_columns: Vec::with_capacity(0),
        };

        let available_columns = sheet.get_available_columns();

        // Ensuring selected columns are valid
        sheet
            .selected_columns
            .validate_columns(&available_columns)
            .with_context(|| {
                format!(
                    "selected columns are invalid, available columns are: {available_columns:?}"
                )
            })?;

        sheet.available_columns = available_columns;
        Ok(sheet)
    }

    fn get_available_columns(&self) -> Vec<String> {
        let width = self.data.width();
        match &self.header {
            Header::None => (0..width)
                .map(|col_idx| format!("__UNNAMED__{col_idx}"))
                .collect(),
            Header::At(row_idx) => (0..width)
                .map(|col_idx| {
                    self.data
                        .get((*row_idx, col_idx))
                        .and_then(|data| data.as_string())
                        .unwrap_or(format!("__UNNAMED__{col_idx}"))
                })
                .collect(),
            Header::With(names) => {
                let nameless_start_idx = names.len();
                names
                    .iter()
                    .map(ToOwned::to_owned)
                    .chain(
                        (nameless_start_idx..width).map(|col_idx| format!("__UNNAMED__{col_idx}")),
                    )
                    .collect()
            }
        }
    }

    pub(crate) fn limit(&self) -> usize {
        let upper_bound = self.data.height();
        if let Some(n_rows) = self.pagination.n_rows {
            let limit = self.offset() + n_rows;
            if limit < upper_bound {
                return limit;
            }
        }

        upper_bound
    }

    pub(crate) fn schema_sample_rows(&self) -> &Option<usize> {
        &self.schema_sample_rows
    }
}

fn create_boolean_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(BooleanArray::from_iter((offset..limit).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_bool())
    })))
}

fn create_int_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(Int64Array::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.get_int())),
    ))
}

fn create_float_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(Float64Array::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.as_f64())),
    ))
}

fn create_string_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(StringArray::from_iter((offset..limit).map(|row| {
        // NOTE: Not using cell.as_string() here because it matches the String variant last, which
        // is slower for columns containing mostly/only strings (which we expect to meet more often than
        // mixed dtype columns containing mostly numbers)
        data.get((row, col)).and_then(|cell| match cell {
            CalData::String(s) => Some(s.to_string()),
            CalData::Float(s) => Some(s.to_string()),
            CalData::Int(s) => Some(s.to_string()),
            _ => None,
        })
    })))
}

fn duration_type_to_i64(caldt: &CalData) -> Option<i64> {
    caldt.as_duration().map(|d| d.num_milliseconds())
}

fn create_date_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    Arc::new(Date32Array::from_iter((offset..limit).map(|row| {
        data.get((row, col))
            .and_then(|caldate| caldate.as_date())
            .and_then(|date| i32::try_from(date.signed_duration_since(epoch).num_days()).ok())
    })))
}

fn create_datetime_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(TimestampMillisecondArray::from_iter((offset..limit).map(
        |row| {
            data.get((row, col))
                .and_then(|caldt| caldt.as_datetime())
                .map(|dt| dt.timestamp_millis())
        },
    )))
}

fn create_duration_array(
    data: &Range<CalData>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(DurationMillisecondArray::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(duration_type_to_i64)),
    ))
}

impl TryFrom<&ExcelSheet> for Schema {
    type Error = FastExcelError;

    fn try_from(sheet: &ExcelSheet) -> Result<Self, Self::Error> {
        // Checking how many rows we want to use to determine the dtype for a column. If sample_rows is
        // not provided, we sample limit rows, i.e on the entire column
        let sample_rows = sheet.offset() + sheet.schema_sample_rows().unwrap_or(sheet.limit());

        arrow_schema_from_column_names_and_range(
            sheet.data(),
            &sheet.available_columns,
            sheet.offset(),
            // If sample_rows is higher than the sheet's limit, use the limit instead
            cmp::min(sample_rows, sheet.limit()),
            &sheet.selected_columns,
        )
    }
}

impl TryFrom<&ExcelSheet> for RecordBatch {
    type Error = FastExcelError;

    fn try_from(sheet: &ExcelSheet) -> FastExcelResult<Self> {
        let offset = sheet.offset();
        let limit = sheet.limit();

        let schema = Schema::try_from(sheet)
            .with_context(|| format!("could not build schema for sheet {}", sheet.name))?;

        let mut iter = sheet
            .available_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column_name)| {
                // checking if the current column has been selected
                if let Some(col_idx) = match sheet.selected_columns {
                    // All columns selected, return the current index
                    SelectedColumns::All => Some(idx),
                    // Otherwise, return its index. If None is found, it means the column was not
                    // selected, and we will just continue
                    _ => sheet.selected_columns.idx_for_column(
                        &sheet.available_columns,
                        column_name,
                        idx,
                    ),
                } {
                    // At this point, we know for sure that the column is in the schema so we can
                    // safely unwrap
                    let field = schema.field_with_name(column_name).unwrap();
                    Some((
                        field.name(),
                        match field.data_type() {
                            ArrowDataType::Boolean => {
                                create_boolean_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Int64 => {
                                create_int_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Float64 => {
                                create_float_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Utf8 => {
                                create_string_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
                                create_datetime_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Date32 => {
                                create_date_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Duration(TimeUnit::Millisecond) => {
                                create_duration_array(sheet.data(), col_idx, offset, limit)
                            }
                            ArrowDataType::Null => Arc::new(NullArray::new(limit - offset)),
                            _ => unreachable!(),
                        },
                    ))
                } else {
                    None
                }
            })
            .peekable();

        // If the iterable is empty, try_from_iter returns an Err
        if iter.peek().is_none() {
            Ok(RecordBatch::new_empty(Arc::new(schema)))
        } else {
            RecordBatch::try_from_iter(iter)
                .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
                .with_context(|| format!("could not convert sheet {} to RecordBatch", sheet.name))
        }
    }
}

#[pymethods]
impl ExcelSheet {
    #[getter]
    pub fn width(&mut self) -> usize {
        self.width.unwrap_or_else(|| {
            let width = self.data.width();
            self.width = Some(width);
            width
        })
    }

    #[getter]
    pub fn height(&mut self) -> usize {
        self.height.unwrap_or_else(|| {
            let height = self.limit() - self.offset();
            self.height = Some(height);
            height
        })
    }

    #[getter]
    pub fn total_height(&mut self) -> usize {
        self.total_height.unwrap_or_else(|| {
            let total_height = self.data.height() - self.header.offset();
            self.total_height = Some(total_height);
            total_height
        })
    }

    #[getter]
    pub fn offset(&self) -> usize {
        self.header.offset() + self.pagination.offset()
    }

    #[getter]
    pub fn selected_columns<'p>(&'p self, py: Python<'p>) -> Option<&PyList> {
        self.selected_columns.to_python(py)
    }

    #[getter]
    pub fn available_columns<'p>(&'p self, py: Python<'p>) -> &PyList {
        PyList::new(py, &self.available_columns)
    }

    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        RecordBatch::try_from(self)
            .with_context(|| format!("could not create RecordBatch from sheet \"{}\"", &self.name))
            .and_then(|rb| {
                rb.to_pyarrow(py)
                    .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
            })
            .with_context(|| {
                format!(
                    "could not convert RecordBatch to pyarrow for sheet \"{}\"",
                    self.name
                )
            })
            .into_pyresult()
    }

    pub fn __repr__(&self) -> String {
        format!("ExcelSheet<{}>", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    #[test]
    fn selected_columns_from_none() {
        assert_eq!(
            TryInto::<SelectedColumns>::try_into(None).unwrap(),
            SelectedColumns::All
        )
    }

    #[test]
    fn selected_columns_from_list_of_valid_ints() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, vec![0, 1, 2]).as_ref();
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap(),
                SelectedColumns::ByIndex(vec![0, 1, 2])
            )
        });
    }

    #[test]
    fn selected_columns_from_list_of_valid_strings() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, vec!["foo", "bar"]).as_ref();
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap(),
                SelectedColumns::ByName(vec!["foo".to_string(), "bar".to_string()])
            )
        });
    }

    #[test]
    fn selected_columns_from_invalid_ints() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, vec![0, 2, -1]).as_ref();
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_int_list() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, Vec::<usize>::new()).as_ref();
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_string_list() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, Vec::<String>::new()).as_ref();
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[rstest]
    // Standard unique columns
    #[case("A,B,D", vec![0, 1, 3])]
    // Standard unique columns + range
    #[case("A,B:E,Y", vec![0, 1, 2, 3, 24])]
    // Standard unique column + ranges with mixed case
    #[case("A:c,b:E,w,Y:z", vec![0, 1, 2, 3, 22, 24])]
    fn selected_columns_from_valid_ranges(#[case] raw: &str, #[case] expected: Vec<usize>) {
        Python::with_gil(|py| {
            let expected_range = SelectedColumns::ByIndex(expected);
            let input = PyString::new(py, raw).as_ref();

            let range = TryInto::<SelectedColumns>::try_into(Some(input))
                .expect("expected a valid column selection");

            assert_eq!(range, expected_range)
        })
    }

    #[rstest]
    // Standard unique columns
    #[case("", "exactly one character")]
    // empty range
    #[case("a:a,b:d,e", "empty range")]
    // end before start
    #[case("b:a", "end of range is before start")]
    // no start
    #[case(":a", "exactly one character, got 0")]
    // no end
    #[case("a:", "exactly one character, got 0")]
    // too many elements
    #[case("a:b:e", "exactly 2 elements, got 3")]
    fn selected_columns_from_invalid_ranges(#[case] raw: &str, #[case] message: &str) {
        Python::with_gil(|py| {
            let input = PyString::new(py, raw).as_ref();

            let err =
                TryInto::<SelectedColumns>::try_into(Some(input)).expect_err("expected an error");

            match err.kind {
                FastExcelErrorKind::InvalidParameters(detail) => {
                    if !detail.contains(message) {
                        panic!("expected \"{detail}\" to contain \"{message}\"")
                    }
                }
                _ => panic!("Expected error to be InvalidParameters, got {err:?}"),
            }
        })
    }
}
