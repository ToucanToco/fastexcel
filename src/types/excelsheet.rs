use std::{fmt::Debug, sync::Arc};

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
use calamine::{CellType, Data as CalData, DataType, Range};
use chrono::NaiveDate;

use pyo3::{
    prelude::{pyclass, pymethods, PyObject, Python},
    types::PyList,
    PyResult,
};

use crate::utils::{
    arrow::arrow_schema_from_column_names_and_range, schema::get_schema_sample_rows,
};

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
    pub(crate) fn new<CT: CellType>(
        skip_rows: usize,
        n_rows: Option<usize>,
        range: &Range<CT>,
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

    pub(crate) fn n_rows(&self) -> Option<usize> {
        self.n_rows
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

impl TryFrom<Option<&PyList>> for SelectedColumns {
    type Error = FastExcelError;

    fn try_from(value: Option<&PyList>) -> FastExcelResult<Self> {
        use FastExcelErrorKind::InvalidParameters;

        match value {
            None => Ok(Self::All),
            Some(py_list) => {
                if py_list.is_empty() {
                    Err(InvalidParameters("list of selected columns is empty".to_string()).into())
                } else if let Ok(name_vec) = py_list.extract::<Vec<String>>() {
                    Ok(Self::ByName(name_vec))
                } else if let Ok(index_vec) = py_list.extract::<Vec<usize>>() {
                    Ok(Self::ByIndex(index_vec))
                } else {
                    Err(InvalidParameters(format!(
                        "expected list[int] | list[str], got {py_list:?}"
                    ))
                    .into())
                }
            }
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

pub(crate) fn sheet_column_names_from_header_and_range<DT: CellType + DataType>(
    header: &Header,
    data: &Range<DT>,
) -> Vec<String> {
    let width = data.width();
    match header {
        Header::None => (0..width)
            .map(|col_idx| format!("__UNNAMED__{col_idx}"))
            .collect(),
        Header::At(row_idx) => (0..width)
            .map(|col_idx| {
                data.get((*row_idx, col_idx))
                    .and_then(|data| data.get_string())
                    .map(ToOwned::to_owned)
                    .unwrap_or(format!("__UNNAMED__{col_idx}"))
            })
            .collect(),
        Header::With(names) => {
            let nameless_start_idx = names.len();
            names
                .iter()
                .map(ToOwned::to_owned)
                .chain((nameless_start_idx..width).map(|col_idx| format!("__UNNAMED__{col_idx}")))
                .collect()
        }
    }
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

    pub(crate) fn schema_sample_rows(&self) -> usize {
        get_schema_sample_rows(self.schema_sample_rows, self.offset(), self.limit())
    }
}

fn create_boolean_array<DT: CellType + DataType>(
    data: &Range<DT>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(BooleanArray::from_iter((offset..limit).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_bool())
    })))
}

fn create_int_array<DT: CellType + DataType>(
    data: &Range<DT>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(Int64Array::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.get_int())),
    ))
}

fn create_float_array<DT: CellType + DataType>(
    data: &Range<DT>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(Float64Array::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.as_f64())),
    ))
}

fn create_string_array<DT: CellType + DataType>(
    data: &Range<DT>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(StringArray::from_iter((offset..limit).map(|row| {
        // NOTE: Not using cell.as_string() here because it matches the String variant last, which
        // is slower for columns containing mostly/only strings (which we expect to meet more often than
        // mixed dtype columns containing mostly numbers)
        data.get((row, col)).and_then(|cell| {
            if cell.is_string() {
                cell.get_string().map(str::to_string)
            } else {
                cell.as_string()
            }
        })
    })))
}

fn duration_type_to_i64<DT: CellType + DataType>(caldt: &DT) -> Option<i64> {
    caldt.as_duration().map(|d| d.num_milliseconds())
}

fn create_date_array<DT: CellType + DataType>(
    data: &Range<DT>,
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

fn create_datetime_array<DT: CellType + DataType>(
    data: &Range<DT>,
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

fn create_duration_array<DT: CellType + DataType>(
    data: &Range<DT>,
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
        arrow_schema_from_column_names_and_range(
            sheet.data(),
            &sheet.available_columns,
            sheet.offset(),
            sheet.schema_sample_rows(),
            &sheet.selected_columns,
        )
    }
}

pub(crate) fn record_batch_from_data_and_schema<DT: CellType + DataType + Debug>(
    schema: Schema,
    data: &Range<DT>,
    offset: usize,
    limit: usize,
) -> FastExcelResult<RecordBatch> {
    let mut iter = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            (
                field.name(),
                match field.data_type() {
                    ArrowDataType::Boolean => create_boolean_array(data, col_idx, offset, limit),
                    ArrowDataType::Int64 => create_int_array(data, col_idx, offset, limit),
                    ArrowDataType::Float64 => create_float_array(data, col_idx, offset, limit),
                    ArrowDataType::Utf8 => create_string_array(data, col_idx, offset, limit),
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
                        create_datetime_array(data, col_idx, offset, limit)
                    }
                    ArrowDataType::Date32 => create_date_array(data, col_idx, offset, limit),
                    ArrowDataType::Duration(TimeUnit::Millisecond) => {
                        create_duration_array(data, col_idx, offset, limit)
                    }
                    ArrowDataType::Null => Arc::new(NullArray::new(limit - offset)),
                    _ => unreachable!(),
                },
            )
        })
        .peekable();
    // If the iterable is empty, try_from_iter returns an Err
    if iter.peek().is_none() {
        Ok(RecordBatch::new_empty(Arc::new(schema)))
    } else {
        RecordBatch::try_from_iter(iter)
            .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
            .with_context(|| "could not create RecordBatch from iterable")
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
            let py_list = PyList::new(py, vec![0, 1, 2]);
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap(),
                SelectedColumns::ByIndex(vec![0, 1, 2])
            )
        });
    }

    #[test]
    fn selected_columns_from_list_of_valid_strings() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, vec!["foo", "bar"]);
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap(),
                SelectedColumns::ByName(vec!["foo".to_string(), "bar".to_string()])
            )
        });
    }

    #[test]
    fn selected_columns_from_invalid_ints() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, vec![0, 2, -1]);
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_int_list() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, Vec::<usize>::new());
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_string_list() {
        Python::with_gil(|py| {
            let py_list = PyList::new(py, Vec::<String>::new());
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list)).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }
}
