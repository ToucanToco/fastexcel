use std::{error::Error, sync::Arc, time::Instant};

use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, NullArray, StringBuilder},
    datatypes,
    record_batch::RecordBatch,
};
use calamine::{open_workbook, DataType, Range, Reader, Xlsx};

fn main() {
    let now = Instant::now();
    extract_sheet().unwrap();
    println!("{}", now.elapsed().as_secs_f32());
}

fn extract_sheet() -> Result<(), Box<dyn Error>> {
    let path = format!("{}/TestExcel.xlsx", env!("CARGO_MANIFEST_DIR"));
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let sheets = workbook.worksheets();

    for (_sheet, data) in sheets {
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
            let name = data.get((0, col)).unwrap().get_string().unwrap();
            fields.push(datatypes::Field::new(name, col_type, true));
            println!("{}", &array.len());
            arrays.push(array);
        }
        let schema = datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

        println!("{:?}", batch);
    }

    Ok(())
}

fn create_boolean_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    let mut builder = BooleanArray::builder(height);
    for row in 1..height {
        if let Some(cell) = data.get((row, col)) {
            builder.append_value(cell.get_bool().unwrap_or(false));
        } else {
            builder.append_value(false);
        }
    }
    Arc::new(builder.finish())
}

fn create_int_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    let mut builder = Int64Array::builder(height);
    for row in 1..height {
        if let Some(cell) = data.get((row, col)) {
            builder.append_value(cell.get_int().unwrap_or(0));
        } else {
            builder.append_value(0);
        }
    }
    Arc::new(builder.finish())
}

fn create_float_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    let mut builder = Float64Array::builder(height);
    for row in 1..height {
        if let Some(cell) = data.get((row, col)) {
            builder.append_value(cell.get_float().unwrap_or(0.0));
        } else {
            builder.append_value(0.0)
        }
    }
    Arc::new(builder.finish())
}

fn create_string_array(data: &Range<DataType>, col: usize, height: usize) -> Arc<dyn Array> {
    let mut builder = StringBuilder::new(height);
    for row in 1..height {
        if let Some(cell) = data.get((row, col)) {
            builder.append_value(cell.get_string().unwrap_or(""));
        } else {
            builder.append_value("");
        }
    }
    Arc::new(builder.finish())
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
