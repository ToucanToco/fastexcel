use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, NullArray, StringArray},
    datatypes,
    record_batch::RecordBatch,
};
use calamine::{DataType, Range};
use pyo3::{pyclass, pymethods};

#[pyclass]
pub struct ExcelSheet {
    name: String,
    schema: datatypes::Schema,
    data: Range<DataType>,
}

impl ExcelSheet {
    pub(crate) fn schema(&self) -> &datatypes::Schema {
        &self.schema
    }

    pub(crate) fn data(&self) -> &Range<DataType> {
        &self.data
    }

    pub(crate) fn new(name: String, schema: datatypes::Schema, data: Range<DataType>) -> Self {
        ExcelSheet { name, schema, data }
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
            .with_context(|| format!("Could not convert sheet {} to RecordBatch", value.name()))
    }
}

#[pymethods]
impl ExcelSheet {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn height(&self) -> usize {
        self.data.height()
    }
}
