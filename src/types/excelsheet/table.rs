use crate::error::{FastExcelErrorKind, FastExcelResult};
use calamine::{Data, Sheets, Table};
use std::io::{Read, Seek};

pub(crate) fn extract_table_names<'a, RS: Read + Seek>(
    sheets: &'a mut Sheets<RS>,
    sheet_name: Option<&str>,
) -> FastExcelResult<Vec<&'a String>> {
    match sheets {
        Sheets::Xlsx(xlsx) => {
            // Internally checks if tables already loaded; is fast
            xlsx.load_tables()?;

            match sheet_name {
                None => Ok(xlsx.table_names()),
                Some(sn) => Ok(xlsx.table_names_in_sheet(sn)),
            }
        }
        _ => Err(FastExcelErrorKind::Internal(
            "Currently only XLSX files are supported for tables".to_string(),
        )
        .into()),
    }
}

pub(crate) fn extract_table_range<RS: Read + Seek>(
    name: &str,
    sheets: &mut Sheets<RS>,
) -> FastExcelResult<Table<Data>> {
    match sheets {
        Sheets::Xlsx(xlsx) => {
            // Internally checks if tables already loaded; is fast
            xlsx.load_tables()?;

            let table_result = xlsx.table_by_name(name);
            let table = table_result?;

            Ok(table)
        }
        _ => Err(FastExcelErrorKind::Internal(
            "Currently only XLSX files are supported for tables".to_string(),
        )
        .into()),
    }
}
