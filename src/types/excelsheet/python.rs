use std::{collections::HashSet, sync::Arc};

use arrow_array::{RecordBatch, StructArray};
use arrow_schema::Field;
#[cfg(feature = "pyarrow")]
use pyo3::PyResult;
use pyo3::{
    Bound, FromPyObject, IntoPyObject, Py, PyAny, Python, pyclass, pymethods,
    types::{PyAnyMethods, PyCapsule, PyList, PyListMethods, PyString, PyTuple},
};
use pyo3_arrow::ffi::{to_array_pycapsules, to_schema_pycapsule};

use crate::{
    ExcelSheet,
    data::{
        ExcelSheetData, record_batch_from_data_and_columns_with_skip_rows,
        selected_columns_to_schema,
    },
    error::{
        ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult, py_errors::IntoPyResult,
    },
    types::{
        dtype::DTypes,
        excelsheet::{SelectedColumns, SheetVisible, SkipRows, column_info::ColumnInfo},
        idx_or_name::IdxOrName,
    },
};

impl TryFrom<&Bound<'_, PyList>> for SelectedColumns {
    type Error = FastExcelError;

    fn try_from(py_list: &Bound<'_, PyList>) -> FastExcelResult<Self> {
        use FastExcelErrorKind::InvalidParameters;

        if py_list.is_empty() {
            Err(InvalidParameters("list of selected columns is empty".to_string()).into())
        } else if let Ok(selection) = py_list.extract::<Vec<IdxOrName>>() {
            Ok(Self::Selection(selection))
        } else {
            Err(
                InvalidParameters(format!("expected list[int] | list[str], got {py_list:?}"))
                    .into(),
            )
        }
    }
}

impl TryFrom<Option<&Bound<'_, PyAny>>> for SelectedColumns {
    type Error = FastExcelError;

    fn try_from(py_any_opt: Option<&Bound<'_, PyAny>>) -> FastExcelResult<Self> {
        match py_any_opt {
            None => Ok(Self::All),
            Some(py_any) => {
                // Not trying to downcast to PyNone here as we assume that this would result in
                // py_any_opt being None
                if let Ok(py_str) = py_any.extract::<String>() {
                    py_str.parse()
                } else if let Ok(py_list) = py_any.downcast::<PyList>() {
                    py_list.try_into()
                } else if let Ok(py_function) = py_any.extract::<Py<PyAny>>() {
                    Ok(Self::DynamicSelection(py_function))
                } else {
                    Err(FastExcelErrorKind::InvalidParameters(format!(
                        "unsupported object type {object_type}",
                        object_type = py_any.get_type()
                    ))
                    .into())
                }
            }
            .with_context(|| {
                format!("could not determine selected columns from provided object: {py_any}")
            }),
        }
    }
}

impl<'py> IntoPyObject<'py> for &SheetVisible {
    type Target = PyString;

    type Output = Bound<'py, Self::Target>;

    type Error = FastExcelError;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(
            py,
            match self {
                SheetVisible::Visible => "visible",
                SheetVisible::Hidden => "hidden",
                SheetVisible::VeryHidden => "veryhidden",
            },
        ))
    }
}

impl SkipRows {
    pub(crate) fn should_skip_row(&self, row_idx: usize, py: Python) -> FastExcelResult<bool> {
        match self {
            SkipRows::Simple(offset) => Ok(row_idx < *offset),
            SkipRows::List(skip_set) => Ok(skip_set.contains(&row_idx)),
            SkipRows::Callable(func) => {
                let result = func.call1(py, (row_idx,)).map_err(|e| {
                    FastExcelErrorKind::InvalidParameters(format!(
                        "Error calling skip_rows function for row {row_idx}: {e}"
                    ))
                })?;
                result.extract::<bool>(py).map_err(|e| {
                    FastExcelErrorKind::InvalidParameters(format!(
                        "skip_rows callable must return bool, got error: {e}"
                    ))
                    .into()
                })
            }
            SkipRows::SkipEmptyRowsAtBeginning => {
                // This is handled by calamine's FirstNonEmptyRow in the header logic
                // For array creation, we don't need additional filtering
                Ok(false)
            }
        }
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub(crate) struct CellError {
    /// `(int, int)`. The original row and column of the error
    #[pyo3(get)]
    pub position: (usize, usize),
    /// `int`. The row offset
    #[pyo3(get)]
    pub row_offset: usize,
    /// `str`. The error message
    #[pyo3(get)]
    pub detail: String,
}

#[pymethods]
impl CellError {
    #[getter]
    pub fn offset_position(&self) -> (usize, usize) {
        let (row, col) = self.position;
        (row - self.row_offset, col)
    }
}

#[pyclass]
pub(crate) struct CellErrors {
    pub errors: Vec<CellError>,
}

#[pymethods]
impl CellErrors {
    #[getter]
    pub fn errors<'p>(&'p self, _py: Python<'p>) -> Vec<CellError> {
        self.errors.clone()
    }
}

impl FromPyObject<'_> for SkipRows {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Handle None case
        if obj.is_none() {
            return Ok(SkipRows::SkipEmptyRowsAtBeginning);
        }

        // Try to extract as int first
        if let Ok(skip_count) = obj.extract::<usize>() {
            return Ok(SkipRows::Simple(skip_count));
        }

        // Try to extract as list of integers
        if let Ok(skip_list) = obj.extract::<Vec<usize>>() {
            let skip_set: HashSet<usize> = skip_list.into_iter().collect();
            return Ok(SkipRows::List(skip_set));
        }

        // Check if it's callable
        if obj.hasattr("__call__").unwrap_or(false) {
            return Ok(SkipRows::Callable(obj.clone().into()));
        }

        Err(FastExcelErrorKind::InvalidParameters(
            "skip_rows must be int, list of int, callable, or None".to_string(),
        )
        .into())
        .into_pyresult()
    }
}

impl TryFrom<&ExcelSheet> for RecordBatch {
    type Error = FastExcelError;

    fn try_from(sheet: &ExcelSheet) -> FastExcelResult<Self> {
        let offset = sheet.offset();
        let limit = sheet.limit();

        match &sheet.data {
            ExcelSheetData::Owned(range) => record_batch_from_data_and_columns_with_skip_rows(
                &sheet.selected_columns,
                range,
                sheet.pagination.skip_rows(),
                offset,
                limit,
            ),
            ExcelSheetData::Ref(range) => record_batch_from_data_and_columns_with_skip_rows(
                &sheet.selected_columns,
                range,
                sheet.pagination.skip_rows(),
                offset,
                limit,
            ),
        }
        .with_context(|| format!("could not convert sheet {} to RecordBatch", sheet.name()))
    }
}

// NOTE: These proxy python implems are required because `#[getter]` does not play well with `cfg_attr`:
// * https://github.com/PyO3/pyo3/issues/1003
// * https://github.com/PyO3/pyo3/issues/780
#[pymethods]
impl ExcelSheet {
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

    #[getter("offset")]
    pub fn py_offset(&self) -> usize {
        self.offset()
    }

    #[getter("selected_columns")]
    pub fn py_selected_columns(&self) -> Vec<ColumnInfo> {
        self.selected_columns().to_owned()
    }

    #[pyo3(name = "available_columns")]
    pub fn py_available_columns(&mut self) -> FastExcelResult<Vec<ColumnInfo>> {
        self.available_columns()
    }

    #[getter("specified_dtypes")]
    pub fn py_specified_dtypes(&self) -> Option<&DTypes> {
        self.specified_dtypes()
    }

    #[getter("name")]
    pub fn py_name(&self) -> &str {
        self.name()
    }

    #[getter("visible")]
    pub fn py_visible<'py>(&'py self, py: Python<'py>) -> FastExcelResult<Bound<'py, PyString>> {
        let visible: SheetVisible = self.visible();
        (&visible).into_pyobject(py)
    }

    #[cfg(feature = "pyarrow")]
    pub fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use pyo3::IntoPyObjectExt;

        use crate::error::py_errors::IntoPyResult;

        py.detach(|| RecordBatch::try_from(self))
            .with_context(|| {
                format!(
                    "could not create RecordBatch from sheet \"{}\"",
                    self.name()
                )
            })
            .and_then(|rb| {
                use arrow_pyarrow::ToPyArrow;

                rb.to_pyarrow(py)
                    .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
            })
            .with_context(|| {
                format!(
                    "could not convert RecordBatch to pyarrow for sheet \"{}\"",
                    self.name()
                )
            })
            .into_pyresult()
            .and_then(|obj| obj.into_bound_py_any(py))
    }

    #[cfg(feature = "pyarrow")]
    pub fn to_arrow_with_errors<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use arrow_pyarrow::IntoPyArrow;
        use pyo3::IntoPyObjectExt;

        use crate::data::record_batch_from_data_and_columns_with_errors;

        let offset = self.offset();
        let limit = self.limit();

        let (rb, errors) = py
            .detach(|| {
                record_batch_from_data_and_columns_with_errors(
                    &self.selected_columns,
                    self.data(),
                    offset,
                    limit,
                )
            })
            .with_context(|| {
                format!(
                    "could not create RecordBatch from sheet \"{}\"",
                    self.name()
                )
            })?;

        let rb = rb
            .into_pyarrow(py)
            .map_err(|err| FastExcelErrorKind::ArrowError(err.to_string()).into())
            .with_context(|| {
                format!(
                    "could not convert RecordBatch to pyarrow for sheet \"{}\"",
                    self.name()
                )
            })?;
        (rb, errors).into_bound_py_any(py)
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
            .with_context(|| {
                format!(
                    "could not create RecordBatch from sheet \"{}\"",
                    self.name()
                )
            })
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
        format!("ExcelSheet<{}>", self.name())
    }
}
