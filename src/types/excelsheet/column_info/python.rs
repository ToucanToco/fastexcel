use arrow_schema::Field;
use pyo3::{PyResult, pymethods};

use crate::{
    error::py_errors::IntoPyResult,
    types::excelsheet::column_info::{ColumnInfo, ColumnInfoNoDtype},
};

impl From<&ColumnInfo> for Field {
    fn from(col_info: &ColumnInfo) -> Self {
        Field::new(&col_info.name, (&col_info.dtype).into(), true)
    }
}

#[pymethods]
impl ColumnInfo {
    /// Creates a new ColumnInfo object.
    ///
    /// - `name`: `str`. The name of the column
    /// - `index`: `int`. The index of the column. Must be >=0
    /// - `column_name_from`: `fastexcel.ColumnNameFrom`. The origin of the column name
    /// - `dtype`: `fastexcel.DType`. The dtype of the column
    /// - `dtype_from`: `fastexcel.DTypeFrom`. The origin of the dtype for the column
    #[new]
    pub(crate) fn py_new(
        name: String,
        index: usize,
        column_name_from: &str,
        dtype: &str,
        dtype_from: &str,
    ) -> PyResult<Self> {
        Ok(Self::new(
            name,
            index,
            column_name_from.parse().into_pyresult()?,
            dtype.parse().into_pyresult()?,
            dtype_from.parse().into_pyresult()?,
        ))
    }

    /// `fastexcel.DType`. The dtype of the column
    #[getter(dtype)]
    fn get_dtype(&self) -> String {
        self.dtype.to_string()
    }

    #[getter("name")]
    /// `str`. The name of the column
    pub fn py_name(&self) -> &str {
        &self.name
    }

    #[getter("index")]
    /// `int`. The index of the column
    pub fn py_index(&self) -> usize {
        self.index
    }

    /// `fastexcel.ColumnNameFrom`. How the name of the column was determined.
    ///
    /// One of three possible values:
    /// - `"provided"`: The column name was provided via the `use_columns` parameter
    /// - `"looked_up"`: The column name was looked up from the data found in the sheet
    /// - `"generated"`: The column name was generated from the column index, either because
    ///                  `header_row` was `None`, or because it could not be looked up
    #[getter(column_name_from)]
    fn get_colum_name_from(&self) -> String {
        self.column_name_from.to_string()
    }

    /// `fastexcel.DTypeFrom`. How the dtype of the column was determined.
    ///
    /// One of three possible values:
    /// - `"provided_by_index"`: The dtype was specified via the column index
    /// - `"provided_by_name"`: The dtype was specified via the column name
    /// - `"guessed"`: The dtype was determined from the content of the column
    #[getter(dtype_from)]
    fn get_dtype_from(&self) -> String {
        self.dtype_from.to_string()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ColumnInfo(name=\"{name}\", index={index}, dtype=\"{dtype}\", dtype_from=\"{dtype_from}\", column_name_from=\"{column_name_from}\" )",
            name = self.name,
            index = self.index,
            dtype = self.dtype,
            dtype_from = self.dtype_from,
            column_name_from = self.column_name_from
        )
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}

#[pymethods]
impl ColumnInfoNoDtype {
    #[getter("name")]
    /// `str`. The name of the column
    pub fn py_name(&self) -> &str {
        &self.name
    }

    #[getter("index")]
    /// `int`. The index of the column
    pub fn py_index(&self) -> usize {
        self.index
    }
}
