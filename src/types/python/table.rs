use std::sync::Arc;

#[cfg(feature = "python")]
use arrow_array::{NullArray, RecordBatch, StructArray};
#[cfg(feature = "pyarrow")]
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Field;
use calamine::{Data, Range, Table};
#[cfg(feature = "python")]
use pyo3::{
    Bound, PyObject, PyResult, Python, pyclass, pymethods,
    types::{PyCapsule, PyTuple},
};
#[cfg(feature = "python")]
use pyo3_arrow::ffi::{to_array_pycapsules, to_schema_pycapsule};

#[cfg(feature = "python")]
use crate::{
    data::{
        create_boolean_array_from_range, create_date_array_from_range,
        create_datetime_array_from_range, create_duration_array_from_range,
        create_float_array_from_range, create_int_array_from_range, create_string_array_from_range,
        record_batch_from_name_array_iterator, selected_columns_to_schema,
    },
    error::py_errors::IntoPyResult,
};

use crate::{
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
}

#[cfg(feature = "python")]
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

impl ExcelTable {
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
}

// NOTE: These proxy python implems are required because `#[getter]` does not play well with `cfg_attr`:
// * https://github.com/PyO3/pyo3/issues/1003
// * https://github.com/PyO3/pyo3/issues/780
#[cfg(feature = "python")]
#[pymethods]
impl ExcelTable {
    #[getter("name")]
    pub fn py_name(&self) -> &str {
        &self.name
    }

    #[getter("sheet_name")]
    pub fn py_sheet_name(&self) -> &str {
        &self.sheet_name
    }

    #[getter("offset")]
    pub fn py_offset(&self) -> usize {
        self.offset()
    }

    #[getter("limit")]
    pub fn py_limit(&self) -> usize {
        self.limit()
    }

    #[getter("selected_columns")]
    pub fn py_selected_columns(&self) -> Vec<ColumnInfo> {
        self.selected_columns()
    }

    #[pyo3(name = "available_columns")]
    pub fn py_available_columns(&mut self) -> FastExcelResult<Vec<ColumnInfo>> {
        self.available_columns()
    }

    #[getter("specified_dtypes")]
    pub fn py_specified_dtypes(&self) -> Option<&DTypes> {
        self.specified_dtypes()
    }

    #[getter("width")]
    pub fn py_width(&mut self) -> usize {
        self.width()
    }

    #[getter("height")]
    pub fn py_height(&mut self) -> usize {
        self.height()
    }

    #[getter("total_height")]
    pub fn py_total_height(&mut self) -> usize {
        self.total_height()
    }

    #[cfg(feature = "pyarrow")]
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

    /// Export the schema as an [`ArrowSchema`] [`PyCapsule`].
    ///
    /// <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowschema-export>
    ///
    /// [`ArrowSchema`]: arrow_array::ffi::FFI_ArrowSchema
    /// [`PyCapsule`]: pyo3::types::PyCapsule
    pub fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = selected_columns_to_schema(&self.selected_columns);
        Ok(to_schema_pycapsule(py, &schema)?)
    }

    /// Export the schema and data as a pair of [`ArrowSchema`] and [`ArrowArray`] [`PyCapsules`]
    ///
    /// The optional `requested_schema` parameter allows for potential schema conversion.
    ///
    /// <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowarray-export>
    ///
    /// [`ArrowSchema`]: arrow_array::ffi::FFI_ArrowSchema
    /// [`ArrowArray`]: arrow_array::ffi::FFI_ArrowArray
    /// [`PyCapsules`]: pyo3::types::PyCapsule
    pub fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let record_batch = RecordBatch::try_from(self)
            .with_context(|| format!("could not create RecordBatch from table \"{}\"", self.name))
            .into_pyresult()?;

        let field = Field::new_struct("", record_batch.schema_ref().fields().clone(), false);
        let array = Arc::new(StructArray::from(record_batch));
        Ok(to_array_pycapsules(
            py,
            field.into(),
            array.as_ref(),
            requested_schema,
        )?)
    }

    #[cfg(feature = "python")]
    pub fn __repr__(&self) -> String {
        format!(
            "ExcelTable<{sheet}/{name}>",
            sheet = self.sheet_name,
            name = self.name
        )
    }
}
