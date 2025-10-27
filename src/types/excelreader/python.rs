use arrow_array::RecordBatch;
use pyo3::{Bound, IntoPyObjectExt, PyAny, PyResult, Python, pymethods, types::PyString};

use super::{DefinedName, ExcelReader};

use crate::{
    ExcelSheet,
    data::{ExcelSheetData, record_batch_from_data_and_columns},
    error::{ErrorContext, FastExcelErrorKind, FastExcelResult, py_errors::IntoPyResult},
    types::{
        dtype::{DTypeCoercion, DTypes},
        excelreader::LoadSheetOrTableOptions,
        excelsheet::{
            Header, Pagination, SelectedColumns, SkipRows,
            column_info::{build_available_columns_info, finalize_column_info},
        },
        idx_or_name::IdxOrName,
    },
    utils::schema::get_schema_sample_rows,
};

impl ExcelReader {
    fn build_selected_columns(
        use_columns: Option<&Bound<'_, PyAny>>,
    ) -> FastExcelResult<SelectedColumns> {
        use_columns.try_into().with_context(|| format!("expected selected columns to be list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None, got {use_columns:?}"))
    }

    fn load_sheet_eager(
        data: &ExcelSheetData,
        pagination: Pagination,
        header: Header,
        sample_rows: Option<usize>,
        selected_columns: &SelectedColumns,
        dtypes: Option<&DTypes>,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<RecordBatch> {
        let offset = header.offset() + pagination.offset();
        let limit = {
            let upper_bound = data.height();
            if let Some(n_rows) = pagination.n_rows() {
                // minimum value between (offset+n_rows) and the data's height
                std::cmp::min(offset + n_rows, upper_bound)
            } else {
                upper_bound
            }
        };

        let sample_rows_limit = get_schema_sample_rows(sample_rows, offset, limit);
        let available_columns_info = build_available_columns_info(data, selected_columns, &header)?;
        let final_columns_info = selected_columns.select_columns(available_columns_info)?;

        let available_columns = finalize_column_info(
            final_columns_info,
            data,
            offset,
            sample_rows_limit,
            dtypes,
            dtype_coercion,
        )?;

        match data {
            ExcelSheetData::Owned(data) => {
                record_batch_from_data_and_columns(&available_columns, data, offset, limit)
            }
            ExcelSheetData::Ref(data) => {
                record_batch_from_data_and_columns(&available_columns, data, offset, limit)
            }
        }
    }

    fn build_sheet<'py>(
        &mut self,
        idx_or_name: IdxOrName,
        opts: LoadSheetOrTableOptions,
        eager: bool,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let calamine_header_row = opts.calamine_header_row();
        let data_header_row = opts.data_header_row();

        let sheet_meta = self
            .find_sheet_meta(idx_or_name)
            .into_pyresult()?
            .to_owned();

        if eager && self.sheets.supports_by_ref() {
            let range = py
                .detach(|| {
                    self.sheets
                        .with_header_row(calamine_header_row)
                        .worksheet_range_ref(&sheet_meta.name)
                })
                .into_pyresult()?;
            let pagination =
                Pagination::try_new(opts.skip_rows, opts.n_rows, &range).into_pyresult()?;
            let header = Header::new(data_header_row, opts.column_names);
            let rb = py
                .detach(|| {
                    Self::load_sheet_eager(
                        &range.into(),
                        pagination,
                        header,
                        opts.schema_sample_rows,
                        &opts.selected_columns,
                        opts.dtypes.as_ref(),
                        &opts.dtype_coercion,
                    )
                })
                .into_pyresult()?;

            #[cfg(feature = "pyarrow")]
            {
                use arrow_pyarrow::ToPyArrow;
                rb.to_pyarrow(py)
            }
            #[cfg(not(feature = "pyarrow"))]
            {
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Eager loading requires pyarrow feature. Use eager=False for PyCapsule interface.",
                ))
            }
        } else {
            let range = py
                .detach(|| {
                    self.sheets
                        .with_header_row(calamine_header_row)
                        .worksheet_range(&sheet_meta.name)
                })
                .into_pyresult()?;
            let pagination =
                Pagination::try_new(opts.skip_rows, opts.n_rows, &range).into_pyresult()?;
            let header = Header::new(data_header_row, opts.column_names);
            let sheet = ExcelSheet::try_new(
                sheet_meta,
                range.into(),
                header,
                pagination,
                opts.schema_sample_rows,
                opts.dtype_coercion,
                opts.selected_columns,
                opts.dtypes,
            )
            .into_pyresult()?;

            if eager {
                #[cfg(feature = "pyarrow")]
                {
                    sheet.to_arrow(py)
                }
                #[cfg(not(feature = "pyarrow"))]
                {
                    Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "Eager loading requires pyarrow feature. Use eager=False for PyCapsule interface.",
                    ))
                }
            } else {
                sheet.into_bound_py_any(py)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_table<'py>(
        &mut self,
        name: &str,
        opts: LoadSheetOrTableOptions,
        eager: bool,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let excel_table = py.detach(|| self.load_table(name, opts)).into_pyresult()?;

        if eager {
            #[cfg(feature = "pyarrow")]
            {
                Ok(excel_table.to_arrow(py)?)
            }
            #[cfg(not(feature = "pyarrow"))]
            {
                Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "Eager loading requires pyarrow feature. Use eager=False for PyCapsule interface.",
                ))
            }
        } else {
            excel_table.into_bound_py_any(py)
        }
    }
}

#[pymethods]
impl ExcelReader {
    pub fn __repr__(&self) -> String {
        format!("ExcelReader<{}>", &self.source)
    }

    #[pyo3(name = "table_names", signature = (sheet_name = None))]
    pub(crate) fn py_table_names(&mut self, sheet_name: Option<&str>) -> PyResult<Vec<&str>> {
        self.sheets.table_names(sheet_name).into_pyresult()
    }

    #[pyo3(name = "defined_names")]
    pub(crate) fn py_defined_names(&mut self) -> PyResult<Vec<DefinedName>> {
        self.defined_names().into_pyresult()
    }

    #[pyo3(name = "load_sheet", signature = (
        idx_or_name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = SkipRows::SkipEmptyRowsAtBeginning,
        n_rows = None,
        schema_sample_rows = 1_000,
        dtype_coercion = DTypeCoercion::Coerce,
        use_columns = None,
        dtypes = None,
        eager = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn py_load_sheet<'py>(
        &mut self,
        idx_or_name: &Bound<'py, PyAny>,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: SkipRows,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        use_columns: Option<&Bound<'py, PyAny>>,
        dtypes: Option<DTypes>,
        eager: bool,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Cannot use NonZeroUsize in the parameters, as it is not supported by pyo3
        if let Some(0) = schema_sample_rows {
            return Err(FastExcelErrorKind::InvalidParameters(
                "schema_sample_rows cannot be 0, as it would prevent dtype inferring".to_string(),
            )
            .into())
            .into_pyresult();
        }
        let idx_or_name = idx_or_name.try_into().into_pyresult()?;
        let selected_columns = Self::build_selected_columns(use_columns).into_pyresult()?;
        let opts = LoadSheetOrTableOptions {
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            dtype_coercion,
            selected_columns,
            dtypes,
        };

        self.build_sheet(idx_or_name, opts, eager, py)
    }

    #[pyo3(name = "load_table", signature = (
        name,
        *,
        header_row = 0,
        column_names = None,
        skip_rows = SkipRows::SkipEmptyRowsAtBeginning,
        n_rows = None,
        schema_sample_rows = 1_000,
        dtype_coercion = DTypeCoercion::Coerce,
        use_columns = None,
        dtypes = None,
        eager = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn py_load_table<'py>(
        &mut self,
        name: &Bound<'py, PyString>,
        header_row: Option<usize>,
        column_names: Option<Vec<String>>,
        skip_rows: SkipRows,
        n_rows: Option<usize>,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        use_columns: Option<&Bound<'py, PyAny>>,
        dtypes: Option<DTypes>,
        eager: bool,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Cannot use NonZeroUsize in the parameters, as it is not supported by pyo3
        if let Some(0) = schema_sample_rows {
            return Err(FastExcelErrorKind::InvalidParameters(
                "schema_sample_rows cannot be 0, as it would prevent dtype inferring".to_string(),
            )
            .into())
            .into_pyresult();
        }

        let selected_columns = Self::build_selected_columns(use_columns).into_pyresult()?;
        let opts = LoadSheetOrTableOptions {
            header_row,
            column_names,
            skip_rows,
            n_rows,
            schema_sample_rows,
            dtype_coercion,
            selected_columns,
            dtypes,
        };

        self.build_table(&name.to_string(), opts, eager, py)
    }

    #[getter("sheet_names")]
    pub(crate) fn py_sheet_names(&self) -> Vec<&str> {
        self.sheet_names()
    }
}

#[pymethods]
impl DefinedName {
    #[getter("name")]
    pub fn py_name(&self) -> &str {
        &self.name
    }

    #[getter("formula")]
    pub fn py_formula(&self) -> &str {
        &self.formula
    }
}
