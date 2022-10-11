use anyhow::{Context, Result};

use crate::types::{ExcelFile, ExcelSheetIterator};

// pyfunction can't take ownership of parameters since they're shared with python. This means that
// we cannot use "ExcelFile.into_iter", so passing an ExcelSheet to Python does not work. Using a
// proxy class would require cloning everywhere, which would be pretty heavy for
// ExcelSheetIterator. So a tuple it is
pub(crate) fn load_excel_file(path: &str) -> Result<(Vec<String>, ExcelSheetIterator)> {
    let file =
        ExcelFile::try_from_path(path).with_context(|| format!("could not load file at {path}"))?;
    Ok((file.sheet_names(), file.into_iter()))
}
