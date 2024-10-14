use std::sync::Arc;

use arrow::{
    array::{NullArray, RecordBatch},
    pyarrow::ToPyArrow,
};
use calamine::{Data, Range, Table};
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python, ToPyObject};

use crate::{
    data::{
        create_boolean_array_from_range, create_date_array_from_range,
        create_datetime_array_from_range, create_duration_array_from_range,
        create_float_array_from_range, create_int_array_from_range, create_string_array_from_range,
        record_batch_from_name_array_iterator, selected_columns_to_schema,
    },
    error::{
        py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    },
    types::{
        dtype::{DType, DTypeCoercion, DTypes},
        python::excelsheet::column_info::build_available_columns,
    },
    utils::schema::get_schema_sample_rows,
};

use super::excelsheet::{
    column_info::{build_available_columns_info, ColumnInfo},
    Header, Pagination, SelectedColumns,
};

#[pyclass(name = "_ExcelTable")]
pub(crate) struct ExcelTable {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    sheet_name: String,
    selected_columns: Vec<ColumnInfo>,
    available_columns: Vec<ColumnInfo>,
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

        let mut excel_table = ExcelTable {
            name: table.name().to_owned(),
            sheet_name: table.sheet_name().to_owned(),
            // Empty vecs as they'll be replaced
            available_columns: Vec::with_capacity(0),
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
        let available_columns = build_available_columns(
            available_columns_info,
            excel_table.data(),
            excel_table.offset(),
            row_limit,
            excel_table.dtypes.as_ref(),
            &excel_table.dtype_coercion,
        )?;

        // Figure out dtype for every column
        let selected_columns = selected_columns.select_columns(&available_columns)?;
        excel_table.available_columns = available_columns;
        excel_table.selected_columns = selected_columns;

        Ok(excel_table)
    }

    pub(crate) fn data(&self) -> &Range<Data> {
        self.table.data()
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

    #[getter]
    pub fn available_columns(&self) -> Vec<ColumnInfo> {
        self.available_columns.clone()
    }

    #[getter]
    pub fn specified_dtypes<'p>(&'p self, py: Python<'p>) -> Option<PyObject> {
        self.dtypes.as_ref().map(|dtypes| dtypes.to_object(py))
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

    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
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
            .into_pyresult()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ExcelTable<{sheet}/{name}>",
            sheet = self.sheet_name,
            name = self.name
        )
    }
}
