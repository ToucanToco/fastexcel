use std::sync::Arc;

use arrow::{
    array::{NullArray, RecordBatch},
    pyarrow::ToPyArrow,
};
use calamine::{Data, Range, Table};
use pyo3::{PyObject, Python, pyclass, pymethods};

use crate::{
    data::{
        create_boolean_array_from_range, create_date_array_from_range,
        create_datetime_array_from_range, create_duration_array_from_range,
        create_float_array_from_range, create_int_array_from_range, create_string_array_from_range,
        record_batch_from_name_array_iterator, selected_columns_to_schema,
    },
    error::{ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DType, DTypeCoercion, DTypes},
        python::excelsheet::column_info::finalize_column_info,
    },
    utils::schema::get_schema_sample_rows,
};

use super::excelsheet::{
    Header, Pagination, SelectedColumns,
    column_info::{AvailableColumns, ColumnInfo, build_available_columns_info},
};

#[pyclass(name = "_ExcelTable")]
pub(crate) struct ExcelTable {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
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
}

impl TryFrom<&ExcelTable> for RecordBatch {
    type Error = FastExcelError;

    fn try_from(table: &ExcelTable) -> FastExcelResult<Self> {
        let offset = table.offset();
        let limit = table.limit();

        let iter = table.selected_columns.iter().map(|column_info| {
            (
                column_info.name(),
                match column_info.dtype() {
                    DType::Bool => create_boolean_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::Int => create_int_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::Float => create_float_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::String => create_string_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::DateTime => create_datetime_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::Date => create_date_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::Duration => create_duration_array_from_range(
                        table.data(),
                        column_info.index(),
                        offset,
                        limit,
                    ),
                    DType::Null => Arc::new(NullArray::new(limit - offset)),
                },
            )
        });

        let schema = selected_columns_to_schema(&table.selected_columns);

        record_batch_from_name_array_iterator(iter, schema).with_context(|| {
            format!(
                "could not convert table {table} in sheet {sheet} to RecordBatch",
                table = &table.name,
                sheet = &table.sheet_name
            )
        })
    }
}

#[pymethods]
impl ExcelTable {
    #[getter]
    pub fn offset(&self) -> usize {
        self.header.offset() + self.pagination.offset()
    }

    #[getter]
    pub(crate) fn limit(&self) -> usize {
        let upper_bound = self.data().height();
        if let Some(n_rows) = self.pagination.n_rows() {
            let limit = self.offset() + n_rows;
            if limit < upper_bound {
                return limit;
            }
        }

        upper_bound
    }

    #[getter]
    pub fn selected_columns(&self) -> Vec<ColumnInfo> {
        self.selected_columns.clone()
    }

    pub fn available_columns<'p>(
        &'p mut self,
        _py: Python<'p>,
    ) -> FastExcelResult<Vec<ColumnInfo>> {
        self.load_available_columns().map(|cols| cols.to_vec())
    }

    #[getter]
    pub fn specified_dtypes(&self, _py: Python<'_>) -> Option<&DTypes> {
        self.dtypes.as_ref()
    }

    #[getter]
    pub fn width(&mut self) -> usize {
        self.width.unwrap_or_else(|| {
            let width = self.data().width();
            self.width = Some(width);
            width
        })
    }

    #[getter]
    pub fn height(&mut self) -> usize {
        self.height.unwrap_or_else(|| {
            let height = self.limit() - self.offset();
            self.height = Some(height);
            height
        })
    }

    #[getter]
    pub fn total_height(&mut self) -> usize {
        self.total_height.unwrap_or_else(|| {
            let total_height = self.data().height() - self.header.offset();
            self.total_height = Some(total_height);
            total_height
        })
    }

    pub fn to_arrow(&self, py: Python<'_>) -> FastExcelResult<PyObject> {
        RecordBatch::try_from(self)
            .with_context(|| {
                format!(
                    "could not create RecordBatch from sheet \"{}\"",
                    self.name
                )
            })
            .and_then(|rb| {
                rb.to_pyarrow(py)
                    .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
            })
            .with_context(|| {
                format!(
                    "could not convert RecordBatch to pyarrow for table \"{table}\" in sheet \"{sheet}\"",
                    table = self.name, sheet = self.sheet_name
                )
            })
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ExcelTable<{sheet}/{name}>",
            sheet = self.sheet_name,
            name = self.name
        )
    }
}
