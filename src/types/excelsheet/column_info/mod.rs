#[cfg(feature = "python")]
mod python;

use std::{fmt::Display, str::FromStr};

use calamine::DataType;
#[cfg(feature = "python")]
use pyo3::pyclass;

use crate::{
    data::ExcelSheetData,
    error::{ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult},
    types::{
        dtype::{DType, DTypeCoercion, DTypes, get_dtype_for_column},
        idx_or_name::IdxOrName,
    },
};

use super::{Header, SelectedColumns};

/// How the column name was determined
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnNameFrom {
    /// The column name was provided by the user.
    Provided,
    /// The column name was looked up in the sheet or table.
    LookedUp,
    /// The column name was generated based on the column index.
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

/// How the data type was determined.
#[derive(Debug, Clone, PartialEq)]
pub enum DTypeFrom {
    /// The data type was provided for all columns.
    ProvidedForAll,
    /// The data type was provided via the column's index.
    ProvidedByIndex,
    /// The data type was provided via the column's name.
    ProvidedByName,
    /// The data type was guessed based on the column's data.
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
/// Metadata about a single column in a sheet.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "python", pyclass(name = "ColumnInfo"))]
pub struct ColumnInfo {
    /// The column's name
    pub name: String,
    /// The column's index
    pub index: usize,
    /// The column's data type
    pub dtype: DType,
    /// How the column name was determined
    pub column_name_from: ColumnNameFrom,
    /// How the column data type was determined
    pub dtype_from: DTypeFrom,
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
}

/// This class provides information about a single column in a sheet, without associated type
/// information
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "python", pyclass(name = "ColumnInfoNoDtype"))]
pub(crate) struct ColumnInfoNoDtype {
    name: String,
    index: usize,
    column_name_from: ColumnNameFrom,
}

// Allows us to easily compare ourselves to a column index or name
impl PartialEq<IdxOrName> for ColumnInfoNoDtype {
    fn eq(&self, other: &IdxOrName) -> bool {
        match other {
            IdxOrName::Idx(index) => index == &self.index,
            IdxOrName::Name(name) => name == &self.name,
        }
    }
}

impl ColumnInfoNoDtype {
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

    pub(super) fn index(&self) -> usize {
        self.index
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

fn column_info_from_header<D: CalamineDataProvider>(
    data: &D,
    selected_columns: &SelectedColumns,
    header: &Header,
) -> FastExcelResult<Vec<ColumnInfoNoDtype>> {
    let width = data.width();
    match header {
        Header::None => Ok((0..width)
            .map(|col_idx| {
                ColumnInfoNoDtype::new(
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
                        // Remove null bytes from column names to avoid CString panics in Arrow FFI.
                        //
                        // Excel strings (especially UTF-16 in .xls) may contain embedded nulls (`\0`) after
                        // conversion to Rust `String`. Arrow’s C FFI uses `CString::new()`, which fails on
                        // null bytes, causing panics.
                        //
                        // This strips nulls while keeping the readable content.
                        let sanitized_col_name = col_name.replace('\0', "");
                        ColumnInfoNoDtype::new(
                            sanitized_col_name,
                            col_idx,
                            ColumnNameFrom::LookedUp,
                        )
                    })
                    .unwrap_or_else(|| {
                        ColumnInfoNoDtype::new(
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
                            Some(provided_name) => ColumnInfoNoDtype::new(
                                provided_name,
                                col_idx,
                                ColumnNameFrom::Provided,
                            ),
                            None => ColumnInfoNoDtype::new(
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
                        ColumnInfoNoDtype::new(name.to_owned(), col_idx, ColumnNameFrom::Provided)
                    })
                    .chain((nameless_start_idx..width).map(|col_idx| {
                        ColumnInfoNoDtype::new(
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

/// Loads available columns and sets aliases in case of name conflicts
pub(crate) fn build_available_columns_info<D: CalamineDataProvider>(
    data: &D,
    selected_columns: &SelectedColumns,
    header: &Header,
) -> FastExcelResult<Vec<ColumnInfoNoDtype>> {
    column_info_from_header(data, selected_columns, header).map(set_aliases_for_columns_info)
}

fn set_aliases_for_columns_info(columns_info: Vec<ColumnInfoNoDtype>) -> Vec<ColumnInfoNoDtype> {
    let mut aliased_column_names = Vec::with_capacity(columns_info.len());
    columns_info
        .into_iter()
        .map(|mut column_info_builder| {
            // Setting the right alias for every column
            let alias = alias_for_name(column_info_builder.name(), &aliased_column_names);
            if alias != column_info_builder.name() {
                column_info_builder = column_info_builder.with_name(alias.clone());
            }
            aliased_column_names.push(alias);
            column_info_builder
        })
        .collect()
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

/// Turns `ColumnInfoNoDtype` into `ColumnInfo`. This will determine the right dtype when needed
pub(crate) fn finalize_column_info<D: CalamineDataProvider>(
    available_columns_info: Vec<ColumnInfoNoDtype>,
    data: &D,
    start_row: usize,
    end_row: usize,
    specified_dtypes: Option<&DTypes>,
    dtype_coercion: &DTypeCoercion,
) -> FastExcelResult<Vec<ColumnInfo>> {
    available_columns_info
        .into_iter()
        .map(|column_info_builder| {
            column_info_builder.finish(data, start_row, end_row, specified_dtypes, dtype_coercion)
        })
        .collect()
}

#[derive(Debug)]
pub(crate) enum AvailableColumns {
    Pending(SelectedColumns),
    Loaded(Vec<ColumnInfo>),
}

impl AvailableColumns {
    pub(crate) fn as_loaded(&self) -> FastExcelResult<&[ColumnInfo]> {
        match self {
            AvailableColumns::Loaded(column_infos) => Ok(column_infos),
            AvailableColumns::Pending(selected_columns) => {
                Err(FastExcelErrorKind::Internal(format!(
                    "Expected available columns to be loaded, got {selected_columns:?}. \
                    This is a bug, please report it to the fastexcel repository"
                ))
                .into())
            }
        }
    }
}
