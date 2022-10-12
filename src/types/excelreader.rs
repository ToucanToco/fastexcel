use anyhow::{Context, Result};
use calamine::{open_workbook_auto, DataType, Range, Reader, Sheets};
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};

use crate::utils::arrow::arrow_schema_from_range;

use super::ExcelSheet;

#[pyclass]
pub(crate) struct ExcelReader {
    sheets: Sheets,
    #[pyo3(get)]
    sheet_names: Vec<String>,
    path: String,
}

impl ExcelReader {
    // NOTE: Not implementing TryFrom here, because we're aren't building the file from the passed
    // string, but rather from the file pointed by it. Semantically, try_from_path is clearer
    pub(crate) fn try_from_path(path: &str) -> Result<Self> {
        let sheets = open_workbook_auto(path)
            .with_context(|| format!("Could not open workbook at {path}"))?;
        let sheet_names = sheets.sheet_names().to_owned();
        Ok(Self {
            sheets,
            sheet_names,
            path: path.to_owned(),
        })
    }

    fn try_new_excel_sheet_from_range(
        &mut self,
        name: String,
        sheet: Range<DataType>,
    ) -> Result<ExcelSheet> {
        let schema = arrow_schema_from_range(&sheet)
            .with_context(|| format!("Could not create Arrow schema for sheet {name}"))?;
        Ok(ExcelSheet::new(name, schema, sheet))
    }
}

#[pymethods]
impl ExcelReader {
    pub fn __repr__(&self) -> String {
        format!("ExcelReader<{}>", &self.path)
    }

    pub fn load_sheet_by_name(&mut self, name: String) -> Result<ExcelSheet> {
        let range = self
            .sheets
            .worksheet_range(&name)
            .with_context(|| format!("Sheet {name} not found"))?
            .with_context(|| format!("Error while loading sheet {name}"))?;

        self.try_new_excel_sheet_from_range(name, range)
    }

    pub fn load_sheet_by_idx(&mut self, idx: usize) -> Result<ExcelSheet> {
        let name = self
            .sheet_names
            .get(idx)
            .with_context(|| {
                format!(
                    "Sheet index {idx} is out of range. File has {} sheets",
                    self.sheet_names.len()
                )
            })?
            .to_owned();
        let range = self
            .sheets
            .worksheet_range_at(idx)
            .with_context(|| format!("Sheet at idx {idx} not found"))?
            .with_context(|| format!("Error while loading sheet at idx {idx}"))?;

        self.try_new_excel_sheet_from_range(name, range)
    }
}

#[pyclass]
pub(crate) struct ExcelSheetIterator {
    file: ExcelReader,
    idx: usize,
}

impl ExcelSheetIterator {
    pub(crate) fn new(file: ExcelReader) -> Self {
        Self { file, idx: 0 }
    }
}

impl Iterator for ExcelSheetIterator {
    type Item = Result<ExcelSheet>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.file.sheet_names.len() {
            Some(self.file.load_sheet_by_idx(self.idx))
        } else {
            None
        }
    }
}

impl IntoIterator for ExcelReader {
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
