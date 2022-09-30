use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, NullArray, StringArray},
    datatypes, ipc,
    record_batch::RecordBatch,
};
use calamine::{open_workbook, DataType, Range, Reader, Xlsx};

pub fn record_batch_to_bytes(rb: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = ipc::writer::StreamWriter::try_new(Vec::new(), &rb.schema())
        .with_context(|| "Could not instantiate StreamWriter for RecordBatch")?;
    writer
        .write(rb)
        .with_context(|| "Could not write RecordBatch to writer")?;
    writer
        .into_inner()
        .with_context(|| "Could not complete Arrow stream")
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

pub fn extract_sheets(path: &str) -> Result<Vec<RecordBatch>> {
    let mut workbook: Xlsx<_> = open_workbook(path).with_context(|| "Could not open workbook")?;
    let sheets = workbook.worksheets();
    let mut output = Vec::with_capacity(sheets.len());

    for (sheet, data) in sheets {
        let mut fields = vec![];
        let mut arrays = vec![];
        let height = data.height();
        let width = data.width();
        for col in 0..width {
            let col_type = get_column_type(&data, col);
            let array = match col_type {
                datatypes::DataType::Boolean => create_boolean_array(&data, col, height),
                datatypes::DataType::Int64 => create_int_array(&data, col, height),
                datatypes::DataType::Float64 => create_float_array(&data, col, height),
                datatypes::DataType::Utf8 => create_string_array(&data, col, height),
                datatypes::DataType::Null => Arc::new(NullArray::new(height - 1)),
                _ => panic!("Unreachable code"),
            };
            let name = data
                .get((0, col))
                .with_context(|| format!("could not get name of column {col} in sheet {sheet}"))?
                .get_string()
                .with_context(|| {
                    format!("could not convert data from sheet {sheet} at col {col} to string")
                })?;
            fields.push(datatypes::Field::new(
                &alias_for_name(name, &fields),
                col_type,
                true,
            ));
            arrays.push(array);
        }
        let schema = datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)
            .with_context(|| format!("Could not create record batch for sheet {sheet}"))?;

        output.push(batch);
    }
    Ok(output)
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

fn get_column_type(data: &Range<DataType>, col: usize) -> datatypes::DataType {
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
