use std::{fs, io, sync::Arc, vec};

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

pub struct ExcelSheet {
    name: String,
    schema: datatypes::Schema,
    data: calamine::Range<DataType>,
}

impl ExcelSheet {
    pub fn try_from_workbook_and_name(
        wb: &mut Xlsx<io::BufReader<fs::File>>,
        name: String,
    ) -> Result<Self> {
        let data = wb
            .worksheet_range(&name)
            .with_context(|| format!("Sheet {name} not found"))?
            .with_context(|| format!("Error while loading sheet {name}"))?;
        let schema = arrow_schema_from_range(&data)
            .with_context(|| format!("Could not create Arrow schema for sheet {name}"))?;
        Ok(Self { name, schema, data })
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let height = self.data.height();
        let iter = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| {
                (
                    field.name(),
                    match field.data_type() {
                        datatypes::DataType::Boolean => {
                            create_boolean_array(&self.data, col_idx, height)
                        }
                        datatypes::DataType::Int64 => create_int_array(&self.data, col_idx, height),
                        datatypes::DataType::Float64 => {
                            create_float_array(&self.data, col_idx, height)
                        }
                        datatypes::DataType::Utf8 => {
                            create_string_array(&self.data, col_idx, height)
                        }
                        datatypes::DataType::Null => Arc::new(NullArray::new(height - 1)),
                        _ => unreachable!(),
                    },
                )
            });
        RecordBatch::try_from_iter(iter)
            .with_context(|| format!("Could not convert sheet {} to RecordBatch", &self.name))
    }
}

pub struct ExcelFile {
    workbook: Xlsx<io::BufReader<fs::File>>,
}

fn arrow_schema_from_range(range: &calamine::Range<DataType>) -> Result<datatypes::Schema> {
    let mut fields = Vec::with_capacity(range.width());
    for col_idx in 0..range.width() {
        let col_type = get_column_type(range, col_idx);
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

impl ExcelFile {
    pub fn try_from_path(path: &str) -> Result<Self> {
        let workbook: Xlsx<_> =
            open_workbook(path).with_context(|| format!("Could not open workbook at {path}"))?;
        Ok(Self { workbook })
    }
}

pub struct ExcelSheetIterator {
    file: ExcelFile,
    idx: usize,
}

impl ExcelSheetIterator {
    pub fn new(file: ExcelFile) -> Self {
        Self { file, idx: 0 }
    }
}

impl Iterator for ExcelSheetIterator {
    type Item = Result<ExcelSheet>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.file.workbook.sheet_names().get(self.idx) {
            Some(name) => {
                self.idx += 1;
                let name = name.to_owned();
                Some(ExcelSheet::try_from_workbook_and_name(
                    &mut self.file.workbook,
                    name,
                ))
            }
            None => None,
        }
    }
}

impl IntoIterator for ExcelFile {
    type Item = Result<ExcelSheet>;

    type IntoIter = ExcelSheetIterator;

    fn into_iter(self) -> Self::IntoIter {
        ExcelSheetIterator::new(self)
    }
}

pub fn extract_sheets_iter(path: &str) -> Result<ExcelSheetIterator> {
    Ok(ExcelFile::try_from_path(path)?.into_iter())
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
                _ => unreachable!(),
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
