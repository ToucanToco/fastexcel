#[cfg(feature = "python")]
mod python;

use calamine::{Data, Range, Table};
#[cfg(feature = "polars")]
use polars_core::frame::DataFrame;
#[cfg(feature = "python")]
use pyo3::pyclass;

use crate::{
    FastExcelColumn, FastExcelErrorKind, IdxOrName, LoadSheetOrTableOptions, SelectedColumns,
    data::height_without_tail_whitespace,
    error::{ErrorContext, FastExcelResult},
    types::{
        dtype::DTypes,
        excelsheet::{
            Header, Pagination,
            column_info::{
                AvailableColumns, ColumnInfo, build_available_columns_info, finalize_column_info,
            },
            deferred_selection_to_concrete,
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
    opts: LoadSheetOrTableOptions,
    height: Option<usize>,
    total_height: Option<usize>,
    width: Option<usize>,
    limit: usize,
}

impl ExcelTable {
    fn extract_selected_columns_and_table_columns(
        table: &Table<Data>,
        selected_columns: &[IdxOrName],
    ) -> FastExcelResult<(Vec<String>, Vec<IdxOrName>)> {
        let table_columns: Vec<String> = table.columns().into();
        let column_offset = table.data().start().map_or(0, |(_row, col)| col as usize);
        let selected_column_indices = selected_columns
            .iter()
            .map(|idx_or_name| match idx_or_name {
                IdxOrName::Idx(idx) => Ok(*idx),
                IdxOrName::Name(name) => table_columns
                    .iter()
                    .enumerate()
                    .find_map(|(idx, col_name)| {
                        (col_name.as_str() == name.as_str()).then_some(idx + column_offset)
                    })
                    .ok_or_else(|| FastExcelErrorKind::ColumnNotFound(name.clone().into()).into())
                    .with_context(|| format!("available columns are: {table_columns:?}")),
            })
            .collect::<FastExcelResult<Vec<usize>>>()?;

        let table_columns = table_columns
            .into_iter()
            .enumerate()
            .filter_map(|(idx, col_name)| {
                selected_column_indices
                    .contains(&(idx + column_offset))
                    .then_some(col_name)
            })
            .collect();

        let selected_columns = selected_column_indices
            .into_iter()
            .map(Into::into)
            .collect();

        Ok((table_columns, selected_columns))
    }

    /// Builds a `Header` for a table. This might update the column selection, if provided
    fn build_header_and_update_selection(
        table: &Table<Data>,
        opts: LoadSheetOrTableOptions,
    ) -> FastExcelResult<(Header, LoadSheetOrTableOptions)> {
        Ok(match (&opts.column_names, opts.header_row) {
            (None, None) => {
                // If there is a column selection, we need to convert all elements to column
                // indices. This is required because we will be providing the header, and it
                // it is required to use an index-based selection when custom column names are provided
                match &opts.selected_columns {
                    SelectedColumns::Selection(selected_columns) => {
                        let (table_columns, selected_columns) =
                            Self::extract_selected_columns_and_table_columns(
                                table,
                                selected_columns,
                            )?;
                        let opts =
                            opts.selected_columns(SelectedColumns::Selection(selected_columns));
                        (Header::With(table_columns), opts)
                    }
                    SelectedColumns::DeferredSelection(deferred_selection) => {
                        let concrete_columns = deferred_selection_to_concrete(
                            deferred_selection,
                            table.data().end().map_or(0, |(_row, col)| col as usize),
                        );
                        let (table_columns, selected_columns) =
                            Self::extract_selected_columns_and_table_columns(
                                table,
                                &concrete_columns,
                            )?;
                        let opts =
                            opts.selected_columns(SelectedColumns::Selection(selected_columns));
                        (Header::With(table_columns), opts)
                    }
                    _ => (Header::With(table.columns().into()), opts),
                }
            }
            (None, Some(row)) => (Header::At(row), opts),
            (Some(column_names), _) => (Header::With(column_names.clone()), opts),
        })
    }

    pub(crate) fn try_new(
        table: Table<Data>,
        opts: LoadSheetOrTableOptions,
    ) -> FastExcelResult<Self> {
        let pagination = Pagination::try_new(opts.skip_rows.clone(), opts.n_rows, table.data())?;

        let (header, opts) = Self::build_header_and_update_selection(&table, opts)?;

        let available_columns_info =
            build_available_columns_info(table.data(), &opts.selected_columns, &header)?;
        let selected_columns_info = opts
            .selected_columns
            .select_columns(available_columns_info)?;

        let mut excel_table = ExcelTable {
            name: table.name().to_owned(),
            sheet_name: table.sheet_name().to_owned(),
            available_columns: AvailableColumns::Pending,
            // Empty vec as it'll be replaced
            selected_columns: Vec::with_capacity(0),
            table,
            header,
            pagination,
            opts,
            height: None,
            total_height: None,
            width: None,
            // Will be replaced
            limit: 0,
        };
        excel_table.limit = excel_table.compute_limit();

        let row_limit = get_schema_sample_rows(
            excel_table.opts.schema_sample_rows,
            excel_table.offset(),
            excel_table.limit(),
        );

        // Finalizing column info
        let selected_columns = finalize_column_info(
            selected_columns_info,
            excel_table.data(),
            excel_table.offset(),
            row_limit,
            excel_table.opts.dtypes.as_ref(),
            &excel_table.opts.dtype_coercion,
            excel_table.opts.whitespace_as_null,
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
            AvailableColumns::Pending => {
                let available_columns_info = build_available_columns_info(
                    self.table.data(),
                    &self.opts.selected_columns,
                    &self.header,
                )?;
                let final_info = finalize_column_info(
                    available_columns_info,
                    self.data(),
                    self.offset(),
                    self.limit(),
                    self.opts.dtypes.as_ref(),
                    &self.opts.dtype_coercion,
                    self.opts.whitespace_as_null,
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

    fn compute_limit(&self) -> usize {
        let upper_bound = if self.opts.skip_whitespace_tail_rows {
            height_without_tail_whitespace(self.data()).unwrap_or_else(|| self.data().height())
        } else {
            self.data().height()
        };
        if let Some(n_rows) = self.pagination.n_rows() {
            let limit = self.offset() + n_rows;
            if limit < upper_bound {
                return limit;
            }
        }
        upper_bound
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn selected_columns(&self) -> Vec<ColumnInfo> {
        self.selected_columns.clone()
    }

    pub fn available_columns(&mut self) -> FastExcelResult<Vec<ColumnInfo>> {
        self.load_available_columns().map(|cols| cols.to_vec())
    }

    pub fn specified_dtypes(&self) -> Option<&DTypes> {
        self.opts.dtypes.as_ref()
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
                    self.opts.whitespace_as_null,
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
