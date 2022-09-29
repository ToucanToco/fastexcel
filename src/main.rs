use std::{fs::OpenOptions, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, NullArray, StringArray},
    csv, datatypes,
    record_batch::RecordBatch,
};
use calamine::{open_workbook, DataType, Range, Reader, Xlsx};

fn main() {
    let now = Instant::now();
    for (idx, sheet) in extract_sheets().unwrap().iter().enumerate() {
        dump_table(&format!("sheet{idx}.csv"), sheet).unwrap();
    }
    println!("{}", now.elapsed().as_secs_f32());
}

fn dump_table(filename: &str, table: &RecordBatch) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(filename)
        .with_context(|| format!("Could not open {filename} for writing"))?;
    csv::Writer::new(file)
        .write(table)
        .with_context(|| "Could not write RecordBatch to {filename}")?;
    Ok(())
}

fn extract_sheets() -> Result<Vec<RecordBatch>> {
    let path = format!("{}/TestExcel.xlsx", env!("CARGO_MANIFEST_DIR"));
    let mut workbook: Xlsx<_> = open_workbook(path)?;
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
                d => {
                    println!("{:?}", d);
                    todo!();
                }
            };
            let name = data
                .get((0, col))
                .with_context(|| format!("could not get name of column {col} in sheet {sheet}"))?
                .get_string()
                .with_context(|| {
                    format!("could not convert data from sheet {sheet} at col {col} to string")
                })?;
            fields.push(datatypes::Field::new(name, col_type, true));
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
    if let Some(cell) = data.get((1, col)) {
        if cell.is_int() {
            return datatypes::DataType::Int64;
        }
        if cell.is_float() {
            return datatypes::DataType::Float64;
        }
        if cell.is_bool() {
            return datatypes::DataType::Boolean;
        }
        if cell.is_string() {
            return datatypes::DataType::Utf8;
        }
    }
    datatypes::DataType::Null
}
