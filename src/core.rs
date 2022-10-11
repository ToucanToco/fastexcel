use anyhow::Result;

use crate::types::{ExcelFile, ExcelSheetIterator};

pub(crate) fn extract_sheets_iter(path: &str) -> Result<ExcelSheetIterator> {
    Ok(ExcelFile::try_from_path(path)?.into_iter())
}
