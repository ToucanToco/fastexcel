pub(crate) mod dtype;
pub(crate) mod excelreader;
pub(crate) mod excelsheet;
pub(crate) mod exceltable;
pub(crate) mod idx_or_name;

pub use dtype::DType;
pub use excelreader::{ExcelReader, LoadSheetOrTableOptions};
pub use excelsheet::{
    ExcelSheet, SheetVisible,
    column_info::{ColumnInfo, ColumnNameFrom, DTypeFrom},
};
pub use exceltable::ExcelTable;
