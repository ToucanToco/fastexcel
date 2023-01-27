use anyhow::{anyhow, Context, Result};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType as ArrowDataType, Field, Schema},
    ffi::ArrowArray,
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use calamine::{DataType as CalDataType, Range};
use pyo3::prelude::*;
use pyo3::{ffi::Py_uintptr_t, types::PyBytes};

pub(crate) fn record_batch_to_bytes(rb: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = StreamWriter::try_new(Vec::new(), &rb.schema())
        .with_context(|| "Could not instantiate StreamWriter for RecordBatch")?;
    writer
        .write(rb)
        .with_context(|| "Could not write RecordBatch to writer")?;
    writer
        .into_inner()
        .with_context(|| "Could not complete Arrow stream")
}

fn get_arrow_column_type(
    data: &Range<CalDataType>,
    row: usize,
    col: usize,
) -> Result<ArrowDataType> {
    match data
        .get((row, col))
        .with_context(|| format!("Could not retrieve data at ({row},{col})"))?
    {
        CalDataType::Int(_) => Ok(ArrowDataType::Int64),
        CalDataType::Float(_) => Ok(ArrowDataType::Float64),
        CalDataType::String(_) => Ok(ArrowDataType::Utf8),
        CalDataType::Bool(_) => Ok(ArrowDataType::Boolean),
        CalDataType::DateTime(_) => Ok(ArrowDataType::Date64),
        CalDataType::Error(err) => Err(anyhow!("Error in calamine cell: {err:?}")),
        CalDataType::Empty => Ok(ArrowDataType::Null),
    }
}

fn alias_for_name(name: &str, fields: &[Field]) -> String {
    fn rec(name: &str, fields: &[Field], depth: usize) -> String {
        let alias = if depth == 0 {
            name.to_owned()
        } else {
            format!("{name}_{depth}")
        };
        match fields.iter().any(|f| f.name() == &alias) {
            true => rec(name, fields, depth + 1),
            false => alias,
        }
    }

    rec(name, fields, 0)
}

pub(crate) fn arrow_schema_from_column_names_and_range(
    range: &Range<CalDataType>,
    column_names: &[String],
    row_idx: usize,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(column_names.len());

    for (col_idx, name) in column_names.iter().enumerate() {
        let col_type = get_arrow_column_type(range, row_idx, col_idx)?;
        fields.push(Field::new(&alias_for_name(name, &fields), col_type, true));
    }

    Ok(Schema::new(fields))
}

pub(crate) fn record_batch_to_pybytes<'p>(py: Python<'p>, rb: &RecordBatch) -> Result<&'p PyBytes> {
    record_batch_to_bytes(rb).map(|bytes| PyBytes::new(py, bytes.as_slice()))
}

/// Arrow array to Python.
pub(crate) fn to_python_array(
    array: &ArrayRef,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let ffi_array = ArrowArray::try_new(array.data().to_owned()).with_context(|| "haha")?;
    let (array_ptr, schema_ptr) = ArrowArray::into_raw(ffi_array);
    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;
    Ok(array.to_object(py))
}

/// RecordBatch to Python.
pub(crate) fn to_python_record_batch(
    rb: &RecordBatch,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(rb.num_columns());
    for array in rb.columns() {
        let array_object = to_python_array(array, py, pyarrow)?;
        arrays.push(array_object);
    }

    let names: Vec<String> = rb
        .schema()
        .all_fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect();
    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names))?;
    Ok(record.to_object(py))
}
