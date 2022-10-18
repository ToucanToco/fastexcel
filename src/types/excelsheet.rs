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

use crate::utils::arrow::record_batch_to_pybytes;

#[pyclass(name = "_ExcelSheet")]
pub(crate) struct ExcelSheet {
    #[pyo3(get)]
    name: String,
    schema: Schema,
    header_line: Option<usize>,
    data: Range<CalDataType>,
    height: Option<usize>,
    width: Option<usize>,
}

impl ExcelSheet {
    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn data(&self) -> &Range<CalDataType> {
        &self.data
    }

    pub(crate) fn new(
        name: String,
        schema: Schema,
        data: Range<CalDataType>,
        header_line: Option<usize>,
    ) -> Self {
        ExcelSheet {
            name,
            schema,
            header_line,
            data,
            height: None,
            width: None,
        }
    }
}

fn create_boolean_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    height: usize,
) -> Arc<dyn Array> {
    Arc::new(BooleanArray::from_iter((offset..height).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_bool())
    })))
}

fn create_int_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    height: usize,
) -> Arc<dyn Array> {
    Arc::new(Int64Array::from_iter(
        (offset..height).map(|row| data.get((row, col)).and_then(|cell| cell.get_int())),
    ))
}

fn create_float_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    height: usize,
) -> Arc<dyn Array> {
    Arc::new(Float64Array::from_iter((offset..height).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_float())
    })))
}

fn create_string_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    height: usize,
) -> Arc<dyn Array> {
    Arc::new(StringArray::from_iter((offset..height).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_string())
    })))
}

fn create_date_array(
    data: &Range<CalDataType>,
    col: usize,
    offset: usize,
    height: usize,
) -> Arc<dyn Array> {
    Arc::new(TimestampMillisecondArray::from_iter((offset..height).map(
        |row| {
            data.get((row, col))
                .and_then(|cell| cell.as_datetime())
                .map(|dt| dt.timestamp_millis())
        },
    )))
}

impl TryFrom<&ExcelSheet> for RecordBatch {
    type Error = anyhow::Error;

    fn try_from(value: &ExcelSheet) -> Result<Self, Self::Error> {
        let offset = value.offset();
        let height = value.data().height();
        let iter = value
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| {
                (
                    field.name(),
                    match field.data_type() {
                        ArrowDataType::Boolean => {
                            create_boolean_array(value.data(), col_idx, offset, height)
                        }
                        ArrowDataType::Int64 => {
                            create_int_array(value.data(), col_idx, offset, height)
                        }
                        ArrowDataType::Float64 => {
                            create_float_array(value.data(), col_idx, offset, height)
                        }
                        ArrowDataType::Utf8 => {
                            create_string_array(value.data(), col_idx, offset, height)
                        }
                        ArrowDataType::Date64 => {
                            create_date_array(value.data(), col_idx, offset, height)
                        }
                        ArrowDataType::Null => Arc::new(NullArray::new(height - offset)),
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
            let height = self.data.height() - self.offset();
            self.height = Some(height);
            height
        })
    }

    #[getter]
    pub fn offset(&self) -> usize {
        // If header_line.is_none() this means there are no headers so index should be 0
        // If header line is 0 than the dataset start at 1
        self.header_line.map(|h| h + 1).unwrap_or(0)
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
