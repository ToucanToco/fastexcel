use anyhow::{Context, Result};
use arrow::{datatypes, ipc, record_batch::RecordBatch};
use calamine::{open_workbook_auto, DataType, Range, Reader, Sheets};

use crate::types::ExcelSheet;

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

pub struct ExcelFile {
    sheets: Sheets,
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
    // NOTE: Not implementing TryFrom here, because we're aren't building the file from the passed
    // string, but rather from the file pointed by it. Semantically, try_from_path is clearer
    pub fn try_from_path(path: &str) -> Result<Self> {
        let sheets = open_workbook_auto(path)
            .with_context(|| format!("Could not open workbook at {path}"))?;
        Ok(Self { sheets })
    }

    pub fn try_new_excel_sheet_from_name(&mut self, name: &str) -> Result<ExcelSheet> {
        let data = self
            .sheets
            .worksheet_range(name)
            .with_context(|| format!("Sheet {name} not found"))?
            .with_context(|| format!("Error while loading sheet {name}"))?;
        let schema = arrow_schema_from_range(&data)
            .with_context(|| format!("Could not create Arrow schema for sheet {name}"))?;
        Ok(ExcelSheet::new(name.to_owned(), schema, data))
    }
}

pub struct ExcelSheetIterator {
    file: ExcelFile,
    idx: usize,
}

impl ExcelSheetIterator {
    pub(crate) fn new(file: ExcelFile) -> Self {
        Self { file, idx: 0 }
    }
}

impl Iterator for ExcelSheetIterator {
    type Item = Result<ExcelSheet>;

    fn next(&mut self) -> Option<Self::Item> {
        let name = self.file.sheets.sheet_names().get(self.idx)?.clone();
        self.idx += 1;
        Some(self.file.try_new_excel_sheet_from_name(&name))
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
