use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::{
    array::{
        Array, BooleanArray, Date32Array, DurationMillisecondArray, Float64Array, Int64Array,
        NullArray, StringArray, TimestampMillisecondArray,
    },
    datatypes::{DataType as ArrowDataType, Schema, TimeUnit},
    pyarrow::PyArrowConvert,
    record_batch::RecordBatch,
};
use calamine::{DataType as CalDataType, Range};
use chrono::{NaiveDate, Timelike};

use pyo3::prelude::{pyclass, pymethods, PyObject, Python};

use crate::utils::arrow::arrow_schema_from_column_names_and_range;

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
        range: &Range<CalDataType>,
    ) -> Result<Self> {
        let max_height = range.height();
        if max_height < skip_rows {
            bail!("To many rows skipped. Max height is {max_height}");
        }
        Ok(Self { skip_rows, n_rows })
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
    total_height: Option<usize>,
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
            total_height: None,
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
        let upper_bound = self.data.height();
        if let Some(n_rows) = self.pagination.n_rows {
            let limit = self.offset() + n_rows;
            if limit < upper_bound {
                return limit;
            }
        }

        upper_bound
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

fn duration_type_to_i64(caldt: &CalDataType) -> Option<i64> {
    caldt
        .as_time()
        .map(|t| 1000 * i64::from(t.num_seconds_from_midnight()))
}

fn create_date_array(
    data: &Range<CalDataType>,
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
    data: &Range<CalDataType>,
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
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    limit: usize,
) -> Arc<dyn Array> {
    Arc::new(DurationMillisecondArray::from_iter(
        (offset..limit).map(|row| data.get((row, col)).and_then(duration_type_to_i64)),
    ))
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
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
                        create_datetime_array(value.data(), col_idx, offset, limit)
                    }
                    ArrowDataType::Date32 => {
                        create_date_array(value.data(), col_idx, offset, limit)
                    }
                    ArrowDataType::Duration(TimeUnit::Millisecond) => {
                        create_duration_array(value.data(), col_idx, offset, limit)
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

    pub fn to_arrow(&self, py: Python<'_>) -> Result<PyObject> {
        RecordBatch::try_from(self)
            .with_context(|| format!("Could not create RecordBatch from sheet {}", self.name))
            .and_then(|rb| match rb.to_pyarrow(py) {
                Err(e) => Err(anyhow!(
                    "Could not convert RecordBatch to pyarrow for sheet {}: {e}",
                    self.name
                )),
                Ok(pyobj) => Ok(pyobj),
            })
    }

    pub fn __repr__(&self) -> String {
        format!("ExcelSheet<{}>", self.name)
    }
}
