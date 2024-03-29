use std::{str::FromStr, usize};

use calamine::{Data as CalData, Range};
use pyo3::{pyclass, pymethods, PyResult};

use crate::{
    error::{
        py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    },
    types::{
        dtype::{get_dtype_for_column, DType, DTypeMap},
        idx_or_name::IdxOrName,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColumnNameFrom {
    Provided,
    LookedUp,
    Generated,
}

impl FromStr for ColumnNameFrom {
    type Err = FastExcelError;

    fn from_str(s: &str) -> FastExcelResult<Self> {
        match s {
            "provided" => Ok(Self::Provided),
            "looked_up" => Ok(Self::LookedUp),
            "generated" => Ok(Self::Generated),
            _ => Err(
                FastExcelErrorKind::InvalidParameters(format!("invalid ColumnNameFrom: {s}"))
                    .into(),
            ),
        }
    }
}

impl ToString for ColumnNameFrom {
    fn to_string(&self) -> String {
        match self {
            ColumnNameFrom::Provided => "provided",
            ColumnNameFrom::LookedUp => "looked_up",
            ColumnNameFrom::Generated => "generated",
        }
        .to_string()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DTypeFrom {
    ProvidedByIndex,
    ProvidedByName,
    Guessed,
}

impl ToString for DTypeFrom {
    fn to_string(&self) -> String {
        match self {
            DTypeFrom::ProvidedByIndex => "provided_by_index",
            DTypeFrom::ProvidedByName => "provided_by_name",
            DTypeFrom::Guessed => "guessed",
        }
        .to_string()
    }
}

impl FromStr for DTypeFrom {
    type Err = FastExcelError;

    fn from_str(s: &str) -> FastExcelResult<Self> {
        match s {
            "provided_by_index" => Ok(Self::ProvidedByIndex),
            "provided_by_name" => Ok(Self::ProvidedByName),
            "guessed" => Ok(Self::Guessed),
            _ => Err(
                FastExcelErrorKind::InvalidParameters(format!("invalid DTypesFrom: {s}")).into(),
            ),
        }
    }
}

// NOTE: The types for properties unfortunately do not appear in the docs for this class, so we had
// to specify them via docstrings
/// This class provides information about a single column in a sheet
#[derive(Debug, Clone, PartialEq)]
#[pyclass(name = "ColumnInfo")]
pub(crate) struct ColumnInfo {
    /// `str`. The name of the column
    #[pyo3(get)]
    name: String,
    /// `int`. The index of the column
    #[pyo3(get)]
    index: usize,
    dtype: DType,
    column_name_from: ColumnNameFrom,
    dtype_from: DTypeFrom,
}

impl ColumnInfo {
    pub(crate) fn new(
        name: String,
        index: usize,
        column_name_from: ColumnNameFrom,
        dtype: DType,
        dtype_from: DTypeFrom,
    ) -> Self {
        Self {
            name,
            index,
            dtype,
            column_name_from,
            dtype_from,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn dtype(&self) -> &DType {
        &self.dtype
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
        format!("ColumnInfo(name=\"{name}\", index={index}, dtype=\"{dtype}\", dtype_from=\"{dtype_from}\", column_name_from=\"{column_name_from}\" )", name=self.name, index=self.index, dtype=self.dtype.to_string(), dtype_from=self.dtype_from.to_string(), column_name_from=self.column_name_from.to_string())
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Debug)]
pub(super) struct ColumnInfoBuilder {
    name: String,
    index: usize,
    column_name_from: ColumnNameFrom,
}

// Allows us to easily compare ourselves to a column index or name
impl PartialEq<IdxOrName> for ColumnInfoBuilder {
    fn eq(&self, other: &IdxOrName) -> bool {
        match other {
            IdxOrName::Idx(index) => index == &self.index,
            IdxOrName::Name(name) => name == &self.name,
        }
    }
}

impl ColumnInfoBuilder {
    pub(super) fn new(name: String, index: usize, column_name_from: ColumnNameFrom) -> Self {
        Self {
            name,
            index,
            column_name_from,
        }
    }

    pub(super) fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    fn dtype_info(
        &self,
        data: &Range<CalData>,
        start_row: usize,
        end_row: usize,
        specified_dtypes: Option<&DTypeMap>,
    ) -> FastExcelResult<(DType, DTypeFrom)> {
        specified_dtypes
            .and_then(|dtypes| {
                // if we have dtypes, look the dtype up by index, and fall back on a lookup by name
                // (done in this order because copying an usize is cheaper than cloning a string)
                if let Some(dtype) = dtypes.get(&self.index.into()) {
                    Some((*dtype, DTypeFrom::ProvidedByIndex))
                } else {
                    dtypes
                        .get(&self.name.clone().into())
                        .map(|dtype| (*dtype, DTypeFrom::ProvidedByName))
                }
            })
            .map(FastExcelResult::Ok)
            // If we could not look up a dtype, guess it from the data
            .unwrap_or_else(|| {
                get_dtype_for_column(data, start_row, end_row, self.index)
                    .map(|dtype| (dtype, DTypeFrom::Guessed))
            })
    }

    pub(super) fn finish(
        self,
        data: &Range<CalData>,
        start_row: usize,
        end_row: usize,
        specified_dtypes: Option<&DTypeMap>,
    ) -> FastExcelResult<ColumnInfo> {
        let (dtype, dtype_from) = self
            .dtype_info(data, start_row, end_row, specified_dtypes)
            .with_context(|| format!("could not determine dtype for column {}", self.name))?;
        Ok(ColumnInfo::new(
            self.name,
            self.index,
            self.column_name_from,
            dtype,
            dtype_from,
        ))
    }
}
