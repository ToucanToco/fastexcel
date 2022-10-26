use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{
        Array, BooleanArray, Float64Array, Int64Array, NullArray, StringArray,
        TimestampMillisecondArray,
    },
    datatypes::{DataType as ArrowDataType, Schema},
    record_batch::RecordBatch,
};
use calamine::{DataType as CalDataType, Range};

use pyo3::{pyclass, pymethods, PyObject, Python};

use crate::utils::arrow::{arrow_schema_from_column_names_and_range, record_batch_to_pybytes};

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

#[derive(Default)]
pub(crate) struct Pagination {
    skip_rows: usize,
    n_rows: Option<usize>,
}

impl Pagination {
    pub(crate) fn new(skip_rows: usize, n_rows: Option<usize>) -> Self {
        Self { skip_rows, n_rows }
    }

    pub(crate) fn offset(&self) -> usize {
        self.skip_rows
    }
}

#[pyclass(name = "_ExcelSheet")]
pub(crate) struct ExcelSheet {
    #[pyo3(get)]
    pub(crate) name: String,
    header: Header,
    pagination: Pagination,
    data: Range<CalDataType>,
    height: Option<usize>,
    width: Option<usize>,
}

impl ExcelSheet {
    pub(crate) fn data(&self) -> &Range<CalDataType> {
        &self.data
    }

    pub(crate) fn new(
        name: String,
        data: Range<CalDataType>,
        header: Header,
        pagination: Pagination,
    ) -> Self {
        ExcelSheet {
            name,
            header,
            pagination,
            data,
            height: None,
            width: None,
        }
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        let width = self.data.width();
        match &self.header {
            Header::None => (0..width)
                .map(|col_idx| format!("__UNNAMED__{col_idx}"))
                .collect(),
            Header::At(row_idx) => (0..width)
                .map(|col_idx| {
                    self.data
                        .get((*row_idx, col_idx))
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
                    .chain(
                        (nameless_start_idx..width).map(|col_idx| format!("__UNNAMED__{col_idx}")),
                    )
                    .collect()
            }
        }
    }

    pub(crate) fn limit(&self) -> usize {
        if let Some(n_rows) = self.pagination.n_rows {
            self.offset() + n_rows
        } else {
            self.data.height()
        }
    }
}

fn create_boolean_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(BooleanArray::from_iter((offset..limit).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_bool())
    })))
}

fn create_int_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(Int64Array::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(|cell| cell.get_int())),
    ))
}

fn create_float_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(Float64Array::from_iter((offset..limit).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_float())
    })))
}

fn create_string_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(StringArray::from_iter((offset..limit).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_string())
    })))
}

fn create_date_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(TimestampMillisecondArray::from_iter((offset..limit).map(
        |row| {
            data.get((row, col))
                .and_then(|cell| cell.as_datetime())
                .map(|dt| dt.timestamp_millis())
        },
    )))
}

impl TryFrom<&ExcelSheet> for Schema {
    type Error = anyhow::Error;

    fn try_from(value: &ExcelSheet) -> Result<Self, Self::Error> {
        arrow_schema_from_column_names_and_range(
            value.data(),
            &value.column_names(),
            value.offset(),
        )
    }
}

impl TryFrom<&ExcelSheet> for RecordBatch {
    type Error = anyhow::Error;

    fn try_from(value: &ExcelSheet) -> Result<Self, Self::Error> {
        let offset = value.offset();
        let limit = value.limit();
        let schema = Schema::try_from(value)
            .with_context(|| format!("Could not build schema for sheet {}", value.name))?;
        let iter = schema.fields().iter().enumerate().map(|(col_idx, field)| {
            (
                field.name(),
                match field.data_type() {
                    ArrowDataType::Boolean => {
                        create_boolean_array(value.data(), col_idx, offset, limit)
                    }
                    ArrowDataType::Int64 => create_int_array(value.data(), col_idx, offset, limit),
                    ArrowDataType::Float64 => {
                        create_float_array(value.data(), col_idx, offset, limit)
                    }
                    ArrowDataType::Utf8 => {
                        create_string_array(value.data(), col_idx, offset, limit)
                    }
                    ArrowDataType::Date64 => {
                        create_date_array(value.data(), col_idx, offset, limit)
                    }
                    ArrowDataType::Null => Arc::new(NullArray::new(limit - offset)),
                    _ => unreachable!(),
                },
            )
        });
        RecordBatch::try_from_iter(iter)
            .with_context(|| format!("Could not convert sheet {} to RecordBatch", value.name))
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
    pub fn offset(&self) -> usize {
        self.header.offset() + self.pagination.offset()
    }

    pub fn to_arrow(&self, py: Python<'_>) -> Result<PyObject> {
        let rb = RecordBatch::try_from(self)
            .with_context(|| format!("Could not create RecordBatch from sheet {}", self.name))?;
        record_batch_to_pybytes(py, &rb).map(|pybytes| pybytes.into())
    }

    pub fn __repr__(&self) -> String {
        format!("ExcelSheet<{}>", self.name)
    }
}
