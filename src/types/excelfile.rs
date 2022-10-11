use anyhow::{Context, Result};
use calamine::{open_workbook_auto, Reader, Sheets};
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};

use crate::utils::arrow::arrow_schema_from_range;

use super::ExcelSheet;

pub(crate) struct ExcelFile {
    sheets: Sheets,
}

impl ExcelFile {
    // NOTE: Not implementing TryFrom here, because we're aren't building the file from the passed
    // string, but rather from the file pointed by it. Semantically, try_from_path is clearer
    pub(crate) fn try_from_path(path: &str) -> Result<Self> {
        let sheets = open_workbook_auto(path)
            .with_context(|| format!("Could not open workbook at {path}"))?;
        Ok(Self { sheets })
    }

    pub(crate) fn try_new_excel_sheet_from_name(&mut self, name: &str) -> Result<ExcelSheet> {
        let data = self
            .sheets
            .worksheet_range(name)
            .with_context(|| format!("Sheet {name} not found"))?
            .with_context(|| format!("Error while loading sheet {name}"))?;
        let schema = arrow_schema_from_range(&data)
            .with_context(|| format!("Could not create Arrow schema for sheet {name}"))?;
        Ok(ExcelSheet::new(name.to_owned(), schema, data))
    }

    pub(crate) fn sheet_names(&self) -> Vec<String> {
        self.sheets.sheet_names().to_owned()
    }
}

#[pyclass]
pub(crate) struct ExcelSheetIterator {
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

#[pymethods]
impl ExcelSheetIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Result<Option<ExcelSheet>> {
        match slf.next() {
            None => Ok(None),
            Some(sheet) => Ok(Some(sheet?)),
        }
    }
}
