pub(crate) mod column_info;
#[cfg(feature = "polars")]
mod polars;
#[cfg(feature = "python")]
mod python;
pub(crate) mod table;

use std::{cmp, collections::HashSet, fmt::Debug, str::FromStr};

use calamine::{CellType, Range, Sheet as CalamineSheet, SheetVisible as CalamineSheetVisible};
use column_info::{AvailableColumns, ColumnInfoNoDtype};
#[cfg(feature = "polars")]
use polars_core::frame::DataFrame;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny, Python, pyclass};

use self::column_info::{ColumnInfo, build_available_columns_info, finalize_column_info};
use crate::{
    data::{ExcelSheetData, FastExcelColumn},
    error::{ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult},
    types::{dtype::DTypes, idx_or_name::IdxOrName},
};
use crate::{types::dtype::DTypeCoercion, utils::schema::get_schema_sample_rows};
#[cfg(feature = "python")]
pub(crate) use python::{CellError, CellErrors};

#[derive(Debug)]
pub(crate) enum Header {
    None,
    At(usize),
    With(Vec<String>),
}

impl Header {
    pub(crate) fn new(header_row: Option<usize>, column_names: Option<Vec<String>>) -> Self {
        match column_names {
            Some(headers) => Header::With(headers),
            None => match header_row {
                Some(row) => Header::At(row),
                None => Header::None,
            },
        }
    }

    pub(crate) fn offset(&self) -> usize {
        match self {
            Header::At(index) => index + 1,
            Header::None => 0,
            Header::With(_) => 0,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(not(feature = "python"), derive(Clone, PartialEq, Eq))]
pub(crate) struct Pagination {
    skip_rows: SkipRows,
    n_rows: Option<usize>,
}

/// How rows should be skipped.
#[derive(Debug, Default)]
#[cfg_attr(not(feature = "python"), derive(Clone, PartialEq, Eq))]
pub enum SkipRows {
    /// Skip a fixed number of rows.
    Simple(usize),
    /// Skip rows based on a list of row indices.
    List(HashSet<usize>),
    #[cfg(feature = "python")]
    Callable(Py<PyAny>),
    /// Skip empty rows at the beginning of the filer (default).
    #[default]
    SkipEmptyRowsAtBeginning,
}

impl SkipRows {
    pub(crate) fn simple_offset(&self) -> Option<usize> {
        match self {
            SkipRows::Simple(offset) => Some(*offset),
            SkipRows::SkipEmptyRowsAtBeginning => Some(0), // Let calamine's FirstNonEmptyRow handle it
            _ => None,
        }
    }
}

impl Pagination {
    pub(crate) fn try_new<CT: CellType>(
        skip_rows: SkipRows,
        n_rows: Option<usize>,
        range: &Range<CT>,
    ) -> FastExcelResult<Self> {
        let max_height = range.height();
        // Only validate for simple skip_rows case
        if let SkipRows::Simple(skip_count) = &skip_rows {
            if max_height < *skip_count {
                return Err(FastExcelErrorKind::InvalidParameters(format!(
                    "Too many rows skipped. Max height is {max_height}"
                ))
                .into());
            }
        }
        Ok(Self { skip_rows, n_rows })
    }

    pub(crate) fn offset(&self) -> usize {
        self.skip_rows.simple_offset().unwrap_or(0)
    }

    pub(crate) fn n_rows(&self) -> Option<usize> {
        self.n_rows
    }

    pub(crate) fn skip_rows(&self) -> &SkipRows {
        &self.skip_rows
    }
}

#[derive(Default)]
pub enum SelectedColumns {
    #[default]
    All,
    Selection(Vec<IdxOrName>),
    #[cfg(feature = "python")]
    DynamicSelection(Py<PyAny>),
    DeferredSelection(Vec<DeferredColumnSelection>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeferredColumnSelection {
    Fixed(IdxOrName),
    /// start column index, end is determined by sheet width
    OpenEndedRange(usize),
    /// end column index, start is 0
    FromBeginningRange(usize),
}

impl std::fmt::Debug for SelectedColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "All"),
            Self::Selection(selection) => write!(f, "Selection({selection:?})"),
            #[cfg(feature = "python")]
            Self::DynamicSelection(func) => {
                let addr = func as *const _ as usize;
                write!(f, "DynamicSelection({addr})")
            }
            Self::DeferredSelection(deferred) => write!(f, "DeferredSelection({deferred:?})"),
        }
    }
}

impl PartialEq for SelectedColumns {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::All, Self::All) => true,
            (Self::Selection(selection), Self::Selection(other_selection)) => {
                selection == other_selection
            }
            #[cfg(feature = "python")]
            (Self::DynamicSelection(f1), Self::DynamicSelection(f2)) => std::ptr::eq(f1, f2),
            (Self::DeferredSelection(deferred1), Self::DeferredSelection(deferred2)) => {
                deferred1 == deferred2
            }
            _ => false,
        }
    }
}

impl SelectedColumns {
    pub(super) fn select_columns(
        &self,
        available_columns: Vec<ColumnInfoNoDtype>,
    ) -> FastExcelResult<Vec<ColumnInfoNoDtype>> {
        match self {
            SelectedColumns::All => Ok(available_columns),
            SelectedColumns::Selection(selection) => {
                let selected_indices: Vec<usize> = selection
                    .iter()
                    .map(|selected_column| {
                        match selected_column {
                            IdxOrName::Idx(index) => available_columns
                                .iter()
                                .position(|col_info| &col_info.index() == index),
                            IdxOrName::Name(name) => available_columns
                                .iter()
                                .position(|col_info| col_info.name() == name.as_str()),
                        }
                        .ok_or_else(|| {
                            FastExcelErrorKind::ColumnNotFound(selected_column.clone()).into()
                        })
                        .with_context(|| format!("available columns are: {available_columns:?}"))
                    })
                    .collect::<FastExcelResult<_>>()?;

                // We need to sort `available_columns` based on the order of the provided selection.
                // First, we associated every element in the Vec with its position in the selection,
                // and we filter out unselected columns
                let mut cols: Vec<(usize, ColumnInfoNoDtype)> = available_columns
                    .into_iter()
                    .enumerate()
                    .filter_map(|(idx, elem)| {
                        selected_indices
                            .iter()
                            .position(|selected_idx| *selected_idx == idx)
                            .map(|position| (position, elem))
                    })
                    .collect();
                // Then, we sort the columns based on their position in the selection
                cols.sort_by_key(|(pos, _elem)| *pos);

                // And finally, we drop the positions
                Ok(cols.into_iter().map(|(_pos, elem)| elem).collect())
            }
            #[cfg(feature = "python")]
            SelectedColumns::DynamicSelection(use_col_func) => Python::attach(|py| {
                available_columns
                    .into_iter()
                    .filter_map(
                        |col_info| match use_col_func.call1(py, (col_info.clone(),)) {
                            Err(err) => Some(Err(FastExcelErrorKind::InvalidParameters(format!(
                                "`use_columns` callable could not be called ({err})"
                            ))
                            .into())),
                            Ok(should_use_col) => match should_use_col.extract::<bool>(py) {
                                Err(_) => Some(Err(FastExcelErrorKind::InvalidParameters(
                                    "`use_columns` callable should return a boolean".to_string(),
                                )
                                .into())),
                                Ok(true) => Some(Ok(col_info)),
                                Ok(false) => None,
                            },
                        },
                    )
                    .collect()
            }),
            SelectedColumns::DeferredSelection(deferred_selection) => {
                // First, resolve all deferred selections into concrete column indices
                let mut resolved_indices = Vec::new();
                let max_col_index = available_columns.len().saturating_sub(1);

                for deferred in deferred_selection {
                    match deferred {
                        DeferredColumnSelection::Fixed(idx_or_name) => {
                            resolved_indices.push(idx_or_name.clone());
                        }
                        DeferredColumnSelection::OpenEndedRange(start_idx) => {
                            // Add all columns from start_idx to the end
                            resolved_indices
                                .extend((*start_idx..=max_col_index).map(IdxOrName::Idx));
                        }
                        DeferredColumnSelection::FromBeginningRange(end_idx) => {
                            // Add all columns from 0 to end_idx (inclusive)
                            let actual_end = (*end_idx).min(max_col_index);
                            resolved_indices.extend((0..=actual_end).map(IdxOrName::Idx));
                        }
                    }
                }

                // Now use the same logic as Selection but with resolved indices
                let concrete_selection = SelectedColumns::Selection(resolved_indices);
                concrete_selection.select_columns(available_columns)
            }
        }
    }

    const ALPHABET: [char; 26] = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    fn col_idx_for_col_as_letter(col: &str) -> FastExcelResult<usize> {
        use FastExcelErrorKind::InvalidParameters;

        if col.is_empty() {
            return Err(InvalidParameters(
                "a column should have at least one character, got none".to_string(),
            )
            .into());
        }

        col.chars()
            //  iterating over all chars reversed, to have a power based on their rank
            .rev()
            .enumerate()
            //  Parses every char, checks its position and returns its numeric equivalent based on
            //  its rank. For example, AB becomes 27 (26 + 1)
            .map(|(idx, col_chr)| {
                let pos_in_alphabet = Self::ALPHABET
                    .iter()
                    .position(|chr| chr == &col_chr)
                    .ok_or_else(|| {
                        FastExcelError::from(InvalidParameters(format!(
                            "Char is not a valid column name: {col_chr}"
                        )))
                    })?;

                Ok(match idx {
                    // in case it's the last char, just return its position
                    0 => pos_in_alphabet,
                    // otherwise, 26^idx * (position + 1)
                    // For example, CBA is 2081:
                    // A -> 0
                    // B -> 26 (53^1 * (1 + 1))
                    // C -> 2028 (26^2 * (2 + 1))
                    _ => 26usize.pow(idx as u32) * (pos_in_alphabet + 1),
                })
            })
            // Sums all previously obtained ranks
            .try_fold(0usize, |acc, elem_result| {
                elem_result.map(|elem| acc + elem)
            })
    }

    fn col_indices_for_letter_range(col_range: &str) -> FastExcelResult<Vec<usize>> {
        use FastExcelErrorKind::InvalidParameters;

        let col_elements = col_range.split(':').collect::<Vec<_>>();
        if col_elements.len() == 2 {
            let start = Self::col_idx_for_col_as_letter(col_elements[0])
                .with_context(|| format!("invalid start element for range \"{col_range}\""))?;

            // Check if this is an open-ended range (empty end element)
            if col_elements[1].is_empty() {
                // For open-ended ranges, we can't return concrete indices yet
                // This will be handled differently in the parsing logic
                return Err(InvalidParameters(format!(
                    "open-ended range detected: \"{col_range}\". This should be handled by col_selection_for_letter_range"
                ))
                .into());
            }

            let end = Self::col_idx_for_col_as_letter(col_elements[1])
                .with_context(|| format!("invalid end element for range \"{col_range}\""))?;

            match start.cmp(&end) {
                cmp::Ordering::Less => Ok((start..=end).collect()),
                cmp::Ordering::Greater => Err(InvalidParameters(format!(
                    "end of range is before start: \"{col_range}\""
                ))
                .into()),
                cmp::Ordering::Equal => {
                    Err(InvalidParameters(format!("empty range: \"{col_range}\"")).into())
                }
            }
        } else {
            Err(InvalidParameters(format!(
                "expected range to contain exactly 2 elements, got {n_elements}: \"{col_range}\"",
                n_elements = col_elements.len()
            ))
            .into())
        }
    }

    fn col_selection_for_letter_range(
        col_range: &str,
    ) -> FastExcelResult<Vec<DeferredColumnSelection>> {
        use FastExcelErrorKind::InvalidParameters;

        let col_elements = col_range.split(':').collect::<Vec<_>>();
        if col_elements.len() == 2 {
            // Check if this is a from-beginning range (empty start element)
            if col_elements[0].is_empty() {
                if col_elements[1].is_empty() {
                    return Err(InvalidParameters(format!(
                        "cannot have both start and end empty in range: \"{col_range}\""
                    ))
                    .into());
                }
                let end = Self::col_idx_for_col_as_letter(col_elements[1])
                    .with_context(|| format!("invalid end element for range \"{col_range}\""))?;
                return Ok(vec![DeferredColumnSelection::FromBeginningRange(end)]);
            }

            let start = Self::col_idx_for_col_as_letter(col_elements[0])
                .with_context(|| format!("invalid start element for range \"{col_range}\""))?;

            // Check if this is an open-ended range (empty end element)
            if col_elements[1].is_empty() {
                return Ok(vec![DeferredColumnSelection::OpenEndedRange(start)]);
            }

            let end = Self::col_idx_for_col_as_letter(col_elements[1])
                .with_context(|| format!("invalid end element for range \"{col_range}\""))?;

            match start.cmp(&end) {
                cmp::Ordering::Less => Ok((start..=end)
                    .map(|idx| DeferredColumnSelection::Fixed(IdxOrName::Idx(idx)))
                    .collect()),
                cmp::Ordering::Greater => Err(InvalidParameters(format!(
                    "end of range is before start: \"{col_range}\""
                ))
                .into()),
                cmp::Ordering::Equal => {
                    Err(InvalidParameters(format!("empty range: \"{col_range}\"")).into())
                }
            }
        } else {
            Err(InvalidParameters(format!(
                "expected range to contain exactly 2 elements, got {n_elements}: \"{col_range}\"",
                n_elements = col_elements.len()
            ))
            .into())
        }
    }
}

impl FromStr for SelectedColumns {
    type Err = FastExcelError;

    fn from_str(s: &str) -> FastExcelResult<Self> {
        let uppercase_s = s.to_uppercase();
        let parts: Vec<&str> = uppercase_s.split(',').collect();
        let has_open_ended = parts
            .iter()
            .any(|p| p.contains(':') && (p.ends_with(':') || p.starts_with(':')));

        if has_open_ended {
            // Use deferred selection logic
            let deferred_selections = parts
                .iter()
                .map(|part| {
                    if part.contains(':') {
                        Self::col_selection_for_letter_range(part).map(|mut selections| {
                            std::mem::take(&mut selections)
                                .into_iter()
                                .collect::<Vec<_>>()
                        })
                    } else {
                        Self::col_idx_for_col_as_letter(part)
                            .map(|idx| vec![DeferredColumnSelection::Fixed(IdxOrName::Idx(idx))])
                    }
                })
                .collect::<Result<Vec<Vec<_>>, _>>()?
                .into_iter()
                .flatten()
                .collect();
            Ok(Self::DeferredSelection(deferred_selections))
        } else {
            // Use the original immediate resolution logic for backwards compatibility
            let unique_col_indices: HashSet<usize> = parts
                .iter()
                .map(|col_or_range| {
                    if col_or_range.contains(':') {
                        Self::col_indices_for_letter_range(col_or_range)
                    } else {
                        Self::col_idx_for_col_as_letter(col_or_range).map(|idx| vec![idx])
                    }
                })
                .collect::<FastExcelResult<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            let mut sorted_col_indices: Vec<usize> = unique_col_indices.into_iter().collect();
            sorted_col_indices.sort();
            Ok(Self::Selection(
                sorted_col_indices.into_iter().map(IdxOrName::Idx).collect(),
            ))
        }
    }
}

/// Visibility of a sheet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SheetVisible {
    Visible,
    Hidden,
    VeryHidden,
}

impl From<CalamineSheetVisible> for SheetVisible {
    fn from(value: CalamineSheetVisible) -> Self {
        match value {
            CalamineSheetVisible::Visible => SheetVisible::Visible,
            CalamineSheetVisible::Hidden => SheetVisible::Hidden,
            CalamineSheetVisible::VeryHidden => SheetVisible::VeryHidden,
        }
    }
}

/// A single sheet in an Excel file.
#[derive(Debug)]
#[cfg_attr(feature = "python", pyclass(name = "_ExcelSheet"))]
pub struct ExcelSheet {
    sheet_meta: CalamineSheet,
    header: Header,
    pagination: Pagination,
    data: ExcelSheetData<'static>,
    height: Option<usize>,
    total_height: Option<usize>,
    width: Option<usize>,
    schema_sample_rows: Option<usize>,
    dtype_coercion: DTypeCoercion,
    selected_columns: Vec<ColumnInfo>,
    available_columns: AvailableColumns,
    dtypes: Option<DTypes>,
}

impl ExcelSheet {
    pub(crate) fn data(&self) -> &ExcelSheetData<'_> {
        &self.data
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        sheet_meta: CalamineSheet,
        data: ExcelSheetData<'static>,
        header: Header,
        pagination: Pagination,
        schema_sample_rows: Option<usize>,
        dtype_coercion: DTypeCoercion,
        selected_columns: SelectedColumns,
        dtypes: Option<DTypes>,
    ) -> FastExcelResult<Self> {
        let available_columns_info =
            build_available_columns_info(&data, &selected_columns, &header)?;
        let selected_columns_info = selected_columns.select_columns(available_columns_info)?;

        let mut sheet = ExcelSheet {
            sheet_meta,
            header,
            pagination,
            data,
            schema_sample_rows,
            dtype_coercion,
            dtypes,
            height: None,
            total_height: None,
            width: None,
            available_columns: AvailableColumns::Pending(selected_columns),
            // Empty vec as It'll be replaced
            selected_columns: Vec::with_capacity(0),
        };

        // Finalizing column info (figure out dtypes for every column)
        let row_limit = sheet.schema_sample_rows();
        let selected_columns = finalize_column_info(
            selected_columns_info,
            &sheet.data,
            sheet.offset(),
            row_limit,
            sheet.dtypes.as_ref(),
            &sheet.dtype_coercion,
        )?;

        sheet.selected_columns = selected_columns;

        Ok(sheet)
    }

    fn ensure_available_columns_loaded(&mut self) -> FastExcelResult<()> {
        let available_columns = match &self.available_columns {
            AvailableColumns::Pending(selected_columns) => {
                let available_columns_info =
                    build_available_columns_info(&self.data, selected_columns, &self.header)?;
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

    pub(crate) fn limit(&self) -> usize {
        let upper_bound = self.data.height();
        if let Some(n_rows) = self.pagination.n_rows {
            let limit = self.offset() + n_rows;
            if limit < upper_bound {
                return limit;
            }
        }

        upper_bound
    }

    pub(crate) fn schema_sample_rows(&self) -> usize {
        get_schema_sample_rows(self.schema_sample_rows, self.offset(), self.limit())
    }

    pub fn width(&mut self) -> usize {
        self.width.unwrap_or_else(|| {
            let width = self.data.width();
            self.width = Some(width);
            width
        })
    }

    pub fn height(&mut self) -> usize {
        self.height.unwrap_or_else(|| {
            use crate::data::generate_row_selector;
            let height =
                generate_row_selector(self.pagination.skip_rows(), self.offset(), self.limit())
                    .map(|selector| selector.len())
                    .unwrap_or_else(|_| self.limit() - self.offset());
            self.height = Some(height);
            height
        })
    }

    pub fn total_height(&mut self) -> usize {
        self.total_height.unwrap_or_else(|| {
            let total_height = self.data.height() - self.header.offset();
            self.total_height = Some(total_height);
            total_height
        })
    }

    pub fn offset(&self) -> usize {
        self.header.offset() + self.pagination.offset()
    }

    pub fn selected_columns(&self) -> &Vec<ColumnInfo> {
        &self.selected_columns
    }

    pub fn available_columns(&mut self) -> FastExcelResult<Vec<ColumnInfo>> {
        self.load_available_columns().map(|cols| cols.to_vec())
    }

    pub fn specified_dtypes(&self) -> Option<&DTypes> {
        self.dtypes.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.sheet_meta.name
    }

    pub fn visible(&self) -> SheetVisible {
        self.sheet_meta.visible.into()
    }

    pub fn to_columns(&self) -> FastExcelResult<Vec<FastExcelColumn>> {
        self.selected_columns
            .iter()
            .map(|column_info| {
                let offset = self.offset();
                let limit = self.limit();

                match self.data() {
                    ExcelSheetData::Owned(range) => {
                        FastExcelColumn::try_from_column_info(column_info, range, offset, limit)
                    }
                    ExcelSheetData::Ref(range) => {
                        FastExcelColumn::try_from_column_info(column_info, range, offset, limit)
                    }
                }
            })
            .collect()
    }

    #[cfg(feature = "polars")]
    pub fn to_polars(&self) -> FastExcelResult<DataFrame> {
        let pl_columns = self.to_columns()?.into_iter().map(Into::into).collect();
        DataFrame::new(pl_columns).map_err(|err| {
            FastExcelErrorKind::Internal(format!("could not create DataFrame: {err:?}")).into()
        })
    }
}

#[cfg(feature = "__pyo3-tests")]
#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use pyo3::{
        prelude::PyListMethods,
        types::{PyList, PyString},
    };
    use rstest::rstest;

    #[test]
    fn selected_columns_from_none() {
        assert_eq!(
            TryInto::<SelectedColumns>::try_into(None).unwrap(),
            SelectedColumns::All
        )
    }

    #[test]
    fn selected_columns_from_list_of_valid_ints() {
        Python::attach(|py| {
            let py_list = PyList::new(py, vec![0, 1, 2]).expect("could not create PyList");
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap(),
                SelectedColumns::Selection([0, 1, 2].into_iter().map(IdxOrName::Idx).collect())
            )
        });
    }

    #[test]
    fn selected_columns_from_list_of_valid_strings() {
        Python::attach(|py| {
            let py_list = PyList::new(py, vec!["foo", "bar"]).expect("could not create PyList");
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap(),
                SelectedColumns::Selection(
                    ["foo", "bar"]
                        .iter()
                        .map(ToString::to_string)
                        .map(IdxOrName::Name)
                        .collect()
                )
            )
        });
    }

    #[test]
    fn selected_columns_from_list_of_valid_strings_and_ints() {
        Python::attach(|py| {
            let py_list = PyList::new(py, vec!["foo", "bar"]).expect("could not create PyList");
            py_list.append(42).unwrap();
            py_list.append(5).unwrap();
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap(),
                SelectedColumns::Selection(vec![
                    IdxOrName::Name("foo".to_string()),
                    IdxOrName::Name("bar".to_string()),
                    IdxOrName::Idx(42),
                    IdxOrName::Idx(5)
                ])
            )
        });
    }

    #[test]
    fn selected_columns_from_invalid_ints() {
        Python::attach(|py| {
            let py_list = PyList::new(py, vec![0, 2, -1]).expect("could not create PyList");
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_int_list() {
        Python::attach(|py| {
            let py_list = PyList::new(py, Vec::<usize>::new()).expect("could not create PyList");
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_string_list() {
        Python::attach(|py| {
            let py_list = PyList::new(py, Vec::<String>::new()).expect("could not create PyList");
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[rstest]
    // Standard unique columns
    #[case("A,B,D", vec![0, 1, 3])]
    // Standard unique columns + range
    #[case("A,B:E,Y", vec![0, 1, 2, 3, 4, 24])]
    // Standard unique column + ranges with mixed case
    #[case("A:c,b:E,w,Y:z", vec![0, 1, 2, 3, 4, 22, 24, 25])]
    // Ranges beyond Z
    #[case("A,y:AB", vec![0, 24, 25, 26, 27])]
    #[case("BB:BE,DDC:DDF", vec![53, 54, 55, 56, 2810, 2811, 2812, 2813])]
    fn selected_columns_from_valid_ranges(#[case] raw: &str, #[case] expected_indices: Vec<usize>) {
        Python::attach(|py| {
            let expected_range = SelectedColumns::Selection(
                expected_indices.into_iter().map(IdxOrName::Idx).collect(),
            );
            let input = PyString::new(py, raw);

            let range = TryInto::<SelectedColumns>::try_into(Some(input.as_ref()))
                .expect("expected a valid column selection");

            assert_eq!(range, expected_range)
        })
    }

    #[rstest]
    #[case("B:")]
    #[case("A,C:")]
    #[case("A:")]
    #[case(":E")]
    #[case(":C")]
    #[case(":A")]
    #[case(":C,E:")]
    fn selected_columns_from_valid_open_ended_ranges(#[case] raw: &str) {
        Python::attach(|py| {
            let input = PyString::new(py, raw);

            let range = TryInto::<SelectedColumns>::try_into(Some(input.as_ref()))
                .expect("expected a valid column selection");

            assert!(matches!(range, SelectedColumns::DeferredSelection(_)));
        })
    }

    #[rstest]
    // Standard unique columns
    #[case("", "at least one character")]
    // empty range
    #[case("a:a,b:d,e", "empty range")]
    // end before start
    #[case("b:a", "end of range is before start")]
    // both start and end empty
    #[case(":", "cannot have both start and end empty")]
    // too many elements
    #[case("a:b:e", "exactly 2 elements, got 3")]
    fn selected_columns_from_invalid_ranges(#[case] raw: &str, #[case] message: &str) {
        Python::attach(|py| {
            let input = PyString::new(py, raw);

            let err = TryInto::<SelectedColumns>::try_into(Some(input.as_ref()))
                .expect_err("expected an error");

            match err.kind {
                FastExcelErrorKind::InvalidParameters(detail) => {
                    if !detail.contains(message) {
                        panic!("expected \"{detail}\" to contain \"{message}\"")
                    }
                }
                _ => panic!("Expected error to be InvalidParameters, got {err:?}"),
            }
        })
    }
}
