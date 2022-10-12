use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, NullArray, StringArray},
    datatypes,
    record_batch::RecordBatch,
};
use calamine::{DataType, Range};
use pyo3::{pyclass, pymethods, PyObject, Python};

use crate::utils::arrow::record_batch_to_pybytes;

#[pyclass(name = "_ExcelSheet")]
pub struct ExcelSheet {
    #[pyo3(get)]
    name: String,
    schema: datatypes::Schema,
    data: Range<DataType>,
    height: Option<usize>,
    width: Option<usize>,
}

impl ExcelSheet {
    pub(crate) fn schema(&self) -> &datatypes::Schema {
        &self.schema
    }

    pub(crate) fn data(&self) -> &Range<DataType> {
        &self.data
    }

    pub(crate) fn new(name: String, schema: datatypes::Schema, data: Range<DataType>) -> Self {
        ExcelSheet {
            name,
            schema,
            data,
            height: None,
            width: None,
        }
    }
}

fn create_boolean_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    Arc::new(BooleanArray::from_iter((1..height).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_bool())
    })))
}

fn create_int_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    Arc::new(Int64Array::from_iter(
        (1..height).map(|row| data.get((row, col)).and_then(|cell| cell.get_int())),
    ))
}

fn create_float_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    Arc::new(Float64Array::from_iter((1..height).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_float())
    })))
}

fn create_string_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    Arc::new(StringArray::from_iter((1..height).map(|row| {
        data.get((row, col)).and_then(|cell| cell.get_string())
    })))
}

impl TryFrom<&ExcelSheet> for RecordBatch {
    type Error = anyhow::Error;

    fn try_from(value: &ExcelSheet) -> Result<Self, Self::Error> {
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
                        datatypes::DataType::Boolean => {
                            create_boolean_array(value.data(), col_idx, height)
                        }
                        datatypes::DataType::Int64 => {
                            create_int_array(value.data(), col_idx, height)
                        }
                        datatypes::DataType::Float64 => {
                            create_float_array(value.data(), col_idx, height)
                        }
                        datatypes::DataType::Utf8 => {
                            create_string_array(value.data(), col_idx, height)
                        }
                        datatypes::DataType::Null => Arc::new(NullArray::new(height - 1)),
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
        if let Some(width) = self.width {
            width
        } else {
            let width = self.schema.fields().len();
            self.width = Some(width);
            width
        }
    }

    #[getter]
    pub fn height(&mut self) -> usize {
        if let Some(height) = self.height {
            height
        } else {
            let height = self.data.height();
            self.height = Some(height);
            height
        }
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
