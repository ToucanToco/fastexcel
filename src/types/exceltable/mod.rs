#[cfg(feature = "python")]
mod python;

use calamine::{Data, Range, Table};
#[cfg(feature = "polars")]
use polars_core::frame::DataFrame;
#[cfg(feature = "python")]
use pyo3::pyclass;

use crate::{
    FastExcelColumn,
    error::FastExcelResult,
    types::{
        dtype::{DTypeCoercion, DTypes},
        excelsheet::{
            Header, Pagination, SelectedColumns,
            column_info::{
                AvailableColumns, ColumnInfo, build_available_columns_info, finalize_column_info,
            },
        },
    },
    utils::schema::get_schema_sample_rows,
};

/// A single table in an Excel file.
#[derive(Debug)]
#[cfg_attr(feature = "python", pyclass(name = "_ExcelTable"))]
pub struct ExcelTable {
    name: String,
    sheet_name: String,
    selected_columns: Vec<ColumnInfo>,
    available_columns: AvailableColumns,
    table: Table<Data>,
    header: Header,
    pagination: Pagination,
    dtypes: Option<DTypes>,
    dtype_coercion: DTypeCoercion,
    height: Option<usize>,
    total_height: Option<usize>,
    width: Option<usize>,
}

impl ExcelTable {
    pub(crate) fn try_new(
        table: Table<Data>,
        header: Header,
        pagination: Pagination,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        selected_columns: SelectedColumns,
        dtypes: Option<DTypes>,
    ) -> FastExcelResult<Self> {
        let available_columns_info =
            build_available_columns_info(table.data(), &selected_columns, &header)?;
        let selected_columns_info = selected_columns.select_columns(available_columns_info)?;

        let mut excel_table = ExcelTable {
            name: table.name().to_owned(),
            sheet_name: table.sheet_name().to_owned(),
            available_columns: AvailableColumns::Pending(selected_columns),
            // Empty vec as it'll be replaced
            selected_columns: Vec::with_capacity(0),
            table,
            header,
            pagination,
            dtypes,
            dtype_coercion,
            height: None,
            total_height: None,
            width: None,
        };

        let row_limit = get_schema_sample_rows(
            schema_sample_rows,
            excel_table.offset(),
            excel_table.limit(),
        );

        // Finalizing column info
        let selected_columns = finalize_column_info(
            selected_columns_info,
            excel_table.data(),
            excel_table.offset(),
            row_limit,
            excel_table.dtypes.as_ref(),
            &excel_table.dtype_coercion,
        )?;

        // Figure out dtype for every column
        excel_table.selected_columns = selected_columns;

        Ok(excel_table)
    }

    pub(crate) fn data(&self) -> &Range<Data> {
        self.table.data()
    }

    fn ensure_available_columns_loaded(&mut self) -> FastExcelResult<()> {
        let available_columns = match &self.available_columns {
            AvailableColumns::Pending(selected_columns) => {
                let available_columns_info = build_available_columns_info(
                    self.table.data(),
                    selected_columns,
                    &self.header,
                )?;
                let final_info = finalize_column_info(
                    available_columns_info,
                    self.data(),
                    self.offset(),
                    self.limit(),
                    self.dtypes.as_ref(),
                    &self.dtype_coercion,
                )?;
                AvailableColumns::Loaded(final_info)
            }
            AvailableColumns::Loaded(_) => return Ok(()),
        };

        self.available_columns = available_columns;
        Ok(())
    }

    fn load_available_columns(&mut self) -> FastExcelResult<&[ColumnInfo]> {
        self.ensure_available_columns_loaded()?;
        self.available_columns.as_loaded()
    }

    pub fn offset(&self) -> usize {
        self.header.offset() + self.pagination.offset()
    }

    pub fn limit(&self) -> usize {
        let upper_bound = self.data().height();
        if let Some(n_rows) = self.pagination.n_rows() {
            let limit = self.offset() + n_rows;
            if limit < upper_bound {
                return limit;
            }
        }

        upper_bound
    }

    pub fn selected_columns(&self) -> Vec<ColumnInfo> {
        self.selected_columns.clone()
    }

    pub fn available_columns(&mut self) -> FastExcelResult<Vec<ColumnInfo>> {
        self.load_available_columns().map(|cols| cols.to_vec())
    }

    pub fn specified_dtypes(&self) -> Option<&DTypes> {
        self.dtypes.as_ref()
    }

    pub fn width(&mut self) -> usize {
        self.width.unwrap_or_else(|| {
            let width = self.data().width();
            self.width = Some(width);
            width
        })
    }

    pub fn height(&mut self) -> usize {
        self.height.unwrap_or_else(|| {
            let height = self.limit() - self.offset();
            self.height = Some(height);
            height
        })
    }

    pub fn total_height(&mut self) -> usize {
        self.total_height.unwrap_or_else(|| {
            let total_height = self.data().height() - self.header.offset();
            self.total_height = Some(total_height);
            total_height
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sheet_name(&self) -> &str {
        &self.sheet_name
    }

    pub fn to_columns(&self) -> FastExcelResult<Vec<FastExcelColumn>> {
        self.selected_columns
            .iter()
            .map(|column_info| {
                FastExcelColumn::try_from_column_info(
                    column_info,
                    self.table.data(),
                    self.offset(),
                    self.limit(),
                )
            })
            .collect()
    }

    #[cfg(feature = "polars")]
    pub fn to_polars(&self) -> FastExcelResult<DataFrame> {
        use crate::error::FastExcelErrorKind;

        let pl_columns = self.to_columns()?.into_iter().map(Into::into).collect();
        DataFrame::new(pl_columns).map_err(|err| {
            FastExcelErrorKind::Internal(format!("could not create DataFrame: {err:?}")).into()
        })
    }
}
