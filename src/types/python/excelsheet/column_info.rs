use std::{fmt::Display, str::FromStr};

use arrow::datatypes::Field;
use calamine::DataType;
use pyo3::{pyclass, pymethods, PyResult};

use crate::{
    data::ExcelSheetData,
    error::{
        py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    },
    types::{
        dtype::{get_dtype_for_column, DType, DTypeCoercion, DTypes},
        idx_or_name::IdxOrName,
    },
};

use super::{Header, SelectedColumns};

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

impl Display for ColumnNameFrom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ColumnNameFrom::Provided => "provided",
            ColumnNameFrom::LookedUp => "looked_up",
            ColumnNameFrom::Generated => "generated",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DTypeFrom {
    ProvidedForAll,
    ProvidedByIndex,
    ProvidedByName,
    Guessed,
}

impl Display for DTypeFrom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DTypeFrom::ProvidedForAll => "provided_for_all",
            DTypeFrom::ProvidedByIndex => "provided_by_index",
            DTypeFrom::ProvidedByName => "provided_by_name",
            DTypeFrom::Guessed => "guessed",
        })
    }
}

impl FromStr for DTypeFrom {
    type Err = FastExcelError;

    fn from_str(s: &str) -> FastExcelResult<Self> {
        match s {
            "provided_for_all" => Ok(Self::ProvidedForAll),
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
    pub name: String,
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

impl From<&ColumnInfo> for Field {
    fn from(col_info: &ColumnInfo) -> Self {
        Field::new(col_info.name(), col_info.dtype().into(), true)
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
        format!("ColumnInfo(name=\"{name}\", index={index}, dtype=\"{dtype}\", dtype_from=\"{dtype_from}\", column_name_from=\"{column_name_from}\" )", name=self.name, index=self.index, dtype=self.dtype, dtype_from=self.dtype_from, column_name_from=self.column_name_from)
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Debug)]
pub(crate) struct ColumnInfoBuilder {
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

    fn dtype_info<D: CalamineDataProvider>(
        &self,
        data: &D,
        start_row: usize,
        end_row: usize,
        specified_dtypes: Option<&DTypes>,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<(DType, DTypeFrom)> {
        specified_dtypes
            .and_then(|dtypes| {
                match dtypes {
                    DTypes::All(dtype) => Some((*dtype, DTypeFrom::ProvidedForAll)),
                    DTypes::Map(dtypes) => {
                        // if we have dtypes, look the dtype up by index, and fall back on a lookup by name
                        // (done in this order because copying an usize is cheaper than cloning a string)
                        if let Some(dtype) = dtypes.get(&self.index.into()) {
                            Some((*dtype, DTypeFrom::ProvidedByIndex))
                        } else {
                            dtypes
                                .get(&self.name.clone().into())
                                .map(|dtype| (*dtype, DTypeFrom::ProvidedByName))
                        }
                    }
                }
            })
            .map(FastExcelResult::Ok)
            // If we could not look up a dtype, guess it from the data
            .unwrap_or_else(|| {
                data.dtype_for_column(start_row, end_row, self.index, dtype_coercion)
                    .map(|dtype| (dtype, DTypeFrom::Guessed))
            })
    }

    pub(super) fn finish<D: CalamineDataProvider>(
        self,
        data: &D,
        start_row: usize,
        end_row: usize,
        specified_dtypes: Option<&DTypes>,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<ColumnInfo> {
        let (dtype, dtype_from) = self
            .dtype_info(data, start_row, end_row, specified_dtypes, dtype_coercion)
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

pub(crate) trait CalamineDataProvider {
    fn width(&self) -> usize;
    fn get_as_string(&self, pos: (usize, usize)) -> Option<String>;
    fn dtype_for_column(
        &self,
        start_row: usize,
        end_row: usize,
        col: usize,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<DType>;
}

impl CalamineDataProvider for ExcelSheetData<'_> {
    fn width(&self) -> usize {
        self.width()
    }

    fn get_as_string(&self, pos: (usize, usize)) -> Option<String> {
        self.get_as_string(pos)
    }

    fn dtype_for_column(
        &self,
        start_row: usize,
        end_row: usize,
        col: usize,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<DType> {
        self.dtype_for_column(start_row, end_row, col, dtype_coercion)
    }
}

impl CalamineDataProvider for calamine::Range<calamine::Data> {
    fn width(&self) -> usize {
        self.width()
    }

    fn get_as_string(&self, pos: (usize, usize)) -> Option<String> {
        self.get(pos).and_then(|data| data.as_string())
    }

    fn dtype_for_column(
        &self,
        start_row: usize,
        end_row: usize,
        col: usize,
        dtype_coercion: &DTypeCoercion,
    ) -> FastExcelResult<DType> {
        get_dtype_for_column(self, start_row, end_row, col, dtype_coercion)
    }
}

pub(crate) fn build_available_columns_info<D: CalamineDataProvider>(
    data: &D,
    selected_columns: &SelectedColumns,
    header: &Header,
) -> FastExcelResult<Vec<ColumnInfoBuilder>> {
    let width = data.width();
    match header {
        Header::None => Ok((0..width)
            .map(|col_idx| {
                ColumnInfoBuilder::new(
                    format!("__UNNAMED__{col_idx}"),
                    col_idx,
                    ColumnNameFrom::Generated,
                )
            })
            .collect()),
        Header::At(row_idx) => Ok((0..width)
            .map(|col_idx| {
                data.get_as_string((*row_idx, col_idx))
                    .map(|col_name| {
                        ColumnInfoBuilder::new(col_name, col_idx, ColumnNameFrom::LookedUp)
                    })
                    .unwrap_or_else(|| {
                        ColumnInfoBuilder::new(
                            format!("__UNNAMED__{col_idx}"),
                            col_idx,
                            ColumnNameFrom::Generated,
                        )
                    })
            })
            .collect()),
        Header::With(names) => {
            if let SelectedColumns::Selection(column_selection) = selected_columns {
                if column_selection.len() != names.len() {
                    return Err(FastExcelErrorKind::InvalidParameters(
                        "column_names and use_columns must have the same length".to_string(),
                    )
                    .into());
                }
                let selected_indices = column_selection
                        .iter()
                        .map(|idx_or_name| {
                            match idx_or_name {
                        IdxOrName::Idx(idx) => Ok(*idx),
                        IdxOrName::Name(name) => Err(FastExcelErrorKind::InvalidParameters(
                            format!("use_columns can only contain integers when used with columns_names, got \"{name}\"")
                        )
                        .into()),
                    }
                        })
                        .collect::<FastExcelResult<Vec<_>>>()?;

                Ok((0..width)
                    .map(|col_idx| {
                        let provided_name_opt = if let Some(pos_in_names) =
                            selected_indices.iter().position(|idx| idx == &col_idx)
                        {
                            names.get(pos_in_names).cloned()
                        } else {
                            None
                        };

                        match provided_name_opt {
                            Some(provided_name) => ColumnInfoBuilder::new(
                                provided_name,
                                col_idx,
                                ColumnNameFrom::Provided,
                            ),
                            None => ColumnInfoBuilder::new(
                                format!("__UNNAMED__{col_idx}"),
                                col_idx,
                                ColumnNameFrom::Generated,
                            ),
                        }
                    })
                    .collect())
            } else {
                let nameless_start_idx = names.len();
                Ok(names
                    .iter()
                    .enumerate()
                    .map(|(col_idx, name)| {
                        ColumnInfoBuilder::new(name.to_owned(), col_idx, ColumnNameFrom::Provided)
                    })
                    .chain((nameless_start_idx..width).map(|col_idx| {
                        ColumnInfoBuilder::new(
                            format!("__UNNAMED__{col_idx}"),
                            col_idx,
                            ColumnNameFrom::Generated,
                        )
                    }))
                    .collect())
            }
        }
    }
}

fn alias_for_name(name: &str, existing_names: &[String]) -> String {
    #[inline]
    fn rec(name: &str, existing_names: &[String], depth: usize) -> String {
        let alias = if depth == 0 {
            name.to_owned()
        } else {
            format!("{name}_{depth}")
        };
        match existing_names
            .iter()
            .any(|existing_name| existing_name == &alias)
        {
            true => rec(name, existing_names, depth + 1),
            false => alias,
        }
    }

    rec(name, existing_names, 0)
}

pub(crate) fn build_available_columns<D: CalamineDataProvider>(
    available_columns_info: Vec<ColumnInfoBuilder>,
    data: &D,
    start_row: usize,
    end_row: usize,
    specified_dtypes: Option<&DTypes>,
    dtype_coercion: &DTypeCoercion,
) -> FastExcelResult<Vec<ColumnInfo>> {
    let mut aliased_available_columns = Vec::with_capacity(available_columns_info.len());

    available_columns_info
        .into_iter()
        .map(|mut column_info_builder| {
            // Setting the right alias for every column
            let alias = alias_for_name(column_info_builder.name(), &aliased_available_columns);
            if alias != column_info_builder.name() {
                column_info_builder = column_info_builder.with_name(alias.clone());
            }
            aliased_available_columns.push(alias);
            // Setting the dtype info
            column_info_builder.finish(data, start_row, end_row, specified_dtypes, dtype_coercion)
        })
        .collect()
}
