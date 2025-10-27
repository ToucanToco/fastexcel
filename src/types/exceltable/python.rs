use std::sync::Arc;

use arrow_array::{RecordBatch, StructArray};
use arrow_schema::Field;
use pyo3::{
    Bound, PyResult, pymethods,
    types::{PyCapsule, PyTuple},
};
#[cfg(feature = "pyarrow")]
use pyo3::{PyAny, Python};
use pyo3_arrow::ffi::{to_array_pycapsules, to_schema_pycapsule};

use crate::{
    ExcelTable,
    data::{record_batch_from_data_and_columns_with_skip_rows, selected_columns_to_schema},
    error::{ErrorContext, FastExcelError, FastExcelResult, py_errors::IntoPyResult},
    types::{dtype::DTypes, excelsheet::column_info::ColumnInfo},
};

impl TryFrom<&ExcelTable> for RecordBatch {
    type Error = FastExcelError;

    fn try_from(table: &ExcelTable) -> FastExcelResult<Self> {
        record_batch_from_data_and_columns_with_skip_rows(
            &table.selected_columns,
            table.data(),
            table.pagination.skip_rows(),
            table.offset(),
            table.limit(),
        )
        .with_context(|| {
            format!(
                "could not convert table {table} in sheet {sheet} to RecordBatch",
                table = &table.name,
                sheet = &table.sheet_name
            )
        })
    }
}

// NOTE: These proxy python implems are required because `#[getter]` does not play well with `cfg_attr`:
// * https://github.com/PyO3/pyo3/issues/1003
// * https://github.com/PyO3/pyo3/issues/780
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
    pub fn to_arrow<'py>(&self, py: Python<'py>) -> FastExcelResult<Bound<'py, PyAny>> {
        RecordBatch::try_from(self)
            .with_context(|| {
                format!(
                    "could not create RecordBatch from sheet \"{}\"",
                    self.name
                )
            })
            .and_then(|rb| {
                use arrow_pyarrow::ToPyArrow;

                use crate::error::FastExcelErrorKind;

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

    pub fn __repr__(&self) -> String {
        format!(
            "ExcelTable<{sheet}/{name}>",
            sheet = self.sheet_name,
            name = self.name
        )
    }
}
