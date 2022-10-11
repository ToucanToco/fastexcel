use anyhow::{Context, Result};

use calamine::{open_workbook_auto, Reader, Sheets};

use crate::{types::ExcelSheet, utils::arrow::arrow_schema_from_range};

pub struct ExcelFile {
    sheets: Sheets,
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
