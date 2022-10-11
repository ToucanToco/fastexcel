use anyhow::{Context, Result};
use arrow::{datatypes, ipc::writer::StreamWriter, record_batch::RecordBatch};
use calamine::Range;
use pyo3::{types::PyBytes, Python};

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

fn get_arrow_column_type(data: &Range<calamine::DataType>, col: usize) -> datatypes::DataType {
    // NOTE: Not handling dates here because they aren't really primitive types in Excel: We'd have
    // to try to cast them, which has a performance cost
    match data.get((1, col)) {
        Some(cell) => {
            if cell.is_int() {
                datatypes::DataType::Int64
            } else if cell.is_float() {
                datatypes::DataType::Float64
            } else if cell.is_bool() {
                datatypes::DataType::Boolean
            } else if cell.is_string() {
                datatypes::DataType::Utf8
            } else {
                datatypes::DataType::Null
            }
        }
        None => datatypes::DataType::Null,
    }
}

fn alias_for_name(name: &str, fields: &[datatypes::Field]) -> String {
    fn rec(name: &str, fields: &[datatypes::Field], depth: usize) -> String {
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

pub(crate) fn arrow_schema_from_range(
    range: &calamine::Range<calamine::DataType>,
) -> Result<datatypes::Schema> {
    let mut fields = Vec::with_capacity(range.width());
    for col_idx in 0..range.width() {
        let col_type = get_arrow_column_type(range, col_idx);
        let name = range
            .get((0, col_idx))
            .with_context(|| format!("could not get name of column {col_idx}"))?
            .get_string()
            .with_context(|| format!("could not convert data at col {col_idx} to string"))?;
        fields.push(datatypes::Field::new(
            &alias_for_name(name, &fields),
            col_type,
            true,
        ));
    }
    Ok(datatypes::Schema::new(fields))
}

pub(crate) fn record_batch_to_pybytes<'p>(py: Python<'p>, rb: &RecordBatch) -> Result<&'p PyBytes> {
    record_batch_to_bytes(rb).map(|bytes| PyBytes::new(py, bytes.as_slice()))
}
