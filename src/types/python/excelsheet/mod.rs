pub(crate) mod column_info;
pub(crate) mod table;

use calamine::{CellType, Range, Sheet as CalamineSheet, SheetVisible as CalamineSheetVisible};
use std::{cmp, collections::HashSet, fmt::Debug, str::FromStr};

use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};

use pyo3::{
    prelude::{pyclass, pymethods, PyAnyMethods, Python},
    types::PyList,
    Bound, PyAny, PyObject, PyResult, ToPyObject,
};

use crate::{
    data::{record_batch_from_data_and_columns, ExcelSheetData},
    error::{
        py_errors::IntoPyResult, ErrorContext, FastExcelError, FastExcelErrorKind, FastExcelResult,
    },
    types::{dtype::DTypes, idx_or_name::IdxOrName},
};
use crate::{types::dtype::DTypeCoercion, utils::schema::get_schema_sample_rows};

use self::column_info::{build_available_columns, build_available_columns_info, ColumnInfo};

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

pub(crate) struct Pagination {
    skip_rows: usize,
    n_rows: Option<usize>,
}

impl Pagination {
    pub(crate) fn new<CT: CellType>(
        skip_rows: usize,
        n_rows: Option<usize>,
        range: &Range<CT>,
    ) -> FastExcelResult<Self> {
        let max_height = range.height();
        if max_height < skip_rows {
            Err(FastExcelErrorKind::InvalidParameters(format!(
                "Too many rows skipped. Max height is {max_height}"
            ))
            .into())
        } else {
            Ok(Self { skip_rows, n_rows })
        }
    }

    pub(crate) fn offset(&self) -> usize {
        self.skip_rows
    }

    pub(crate) fn n_rows(&self) -> Option<usize> {
        self.n_rows
    }
}

impl TryFrom<&Bound<'_, PyList>> for SelectedColumns {
    type Error = FastExcelError;

    fn try_from(py_list: &Bound<'_, PyList>) -> FastExcelResult<Self> {
        use FastExcelErrorKind::InvalidParameters;

        if py_list.is_empty().map_err(|err| {
            FastExcelErrorKind::InvalidParameters(format!("invalid list object: {err}"))
        })? {
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

pub(crate) enum SelectedColumns {
    All,
    Selection(Vec<IdxOrName>),
    DynamicSelection(PyObject),
}

impl std::fmt::Debug for SelectedColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "All"),
            Self::Selection(selection) => write!(f, "Selection({selection:?})"),
            Self::DynamicSelection(func) => {
                let addr = func as *const _ as usize;
                write!(f, "DynamicSelection({addr})")
            }
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
            (Self::DynamicSelection(f1), Self::DynamicSelection(f2)) => std::ptr::eq(f1, f2),
            _ => false,
        }
    }
}

impl SelectedColumns {
    pub(super) fn select_columns(
        &self,
        available_columns: &[ColumnInfo],
    ) -> FastExcelResult<Vec<ColumnInfo>> {
        match self {
            SelectedColumns::All => Ok(available_columns.to_vec()),
            SelectedColumns::Selection(selection) => selection
                .iter()
                .map(|selected_column| {
                    match selected_column {
                        IdxOrName::Idx(index) => available_columns
                            .iter()
                            .find(|col_info| &col_info.index() == index),
                        IdxOrName::Name(name) => available_columns
                            .iter()
                            .find(|col_info| col_info.name() == name.as_str()),
                    }
                    .ok_or_else(|| {
                        FastExcelErrorKind::ColumnNotFound(selected_column.clone()).into()
                    })
                    .cloned()
                    .with_context(|| format!("available columns are: {available_columns:?}"))
                })
                .collect(),
            SelectedColumns::DynamicSelection(use_col_func) => Python::with_gil(|py| {
                Ok(available_columns
                    .iter()
                    .filter_map(
                        |col_info| match use_col_func.call1(py, (col_info.clone(),)) {
                            Err(err) => Some(Err(FastExcelErrorKind::InvalidParameters(format!(
                                "`use_columns` callable could not be called ({err})"
                            )))),
                            Ok(should_use_col) => match should_use_col.extract::<bool>(py) {
                                Err(_) => Some(Err(FastExcelErrorKind::InvalidParameters(
                                    "`use_columns` callable should return a boolean".to_string(),
                                ))),
                                Ok(true) => Some(Ok(col_info.clone())),
                                Ok(false) => None,
                            },
                        },
                    )
                    .collect::<Result<Vec<_>, _>>()?)
            }),
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
}

impl FromStr for SelectedColumns {
    type Err = FastExcelError;

    fn from_str(s: &str) -> FastExcelResult<Self> {
        let unique_col_indices: HashSet<usize> = s
            .to_uppercase()
            .split(',')
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
                } else if let Ok(py_function) = py_any.extract::<PyObject>() {
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

#[derive(Clone, Debug)]
struct SheetVisible(CalamineSheetVisible);

impl ToPyObject for &SheetVisible {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self.0 {
            CalamineSheetVisible::Visible => "visible".to_object(py),
            CalamineSheetVisible::Hidden => "hidden".to_object(py),
            CalamineSheetVisible::VeryHidden => "veryhidden".to_object(py),
        }
    }
}

impl From<CalamineSheetVisible> for SheetVisible {
    fn from(value: CalamineSheetVisible) -> Self {
        Self(value)
    }
}

#[pyclass(name = "_ExcelSheet")]
pub(crate) struct ExcelSheet {
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
    available_columns: Vec<ColumnInfo>,
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
            // Empty vecs as they'll be replaced
            available_columns: Vec::with_capacity(0),
            selected_columns: Vec::with_capacity(0),
        };

        let row_limit = sheet.schema_sample_rows();

        // Finalizing column info
        let available_columns = build_available_columns(
            available_columns_info,
            &sheet.data,
            sheet.offset(),
            row_limit,
            sheet.dtypes.as_ref(),
            &sheet.dtype_coercion,
        )?;

        // Figure out dtype for every column
        let selected_columns = selected_columns.select_columns(&available_columns)?;
        sheet.available_columns = available_columns;
        sheet.selected_columns = selected_columns;

        Ok(sheet)
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
}

impl TryFrom<&ExcelSheet> for RecordBatch {
    type Error = FastExcelError;

    fn try_from(sheet: &ExcelSheet) -> FastExcelResult<Self> {
        let offset = sheet.offset();
        let limit = sheet.limit();

        record_batch_from_data_and_columns(&sheet.selected_columns, sheet.data(), offset, limit)
            .with_context(|| format!("could not convert sheet {} to RecordBatch", sheet.name()))
    }
}

#[pymethods]
impl ExcelSheet {
    #[getter]
    pub fn width(&mut self) -> usize {
        self.width.unwrap_or_else(|| {
            let width = self.data.width();
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
            let total_height = self.data.height() - self.header.offset();
            self.total_height = Some(total_height);
            total_height
        })
    }

    #[getter]
    pub fn offset(&self) -> usize {
        self.header.offset() + self.pagination.offset()
    }

    #[getter]
    pub fn selected_columns<'p>(&'p self, _py: Python<'p>) -> Vec<ColumnInfo> {
        self.selected_columns.clone()
    }

    #[getter]
    pub fn available_columns<'p>(&'p self, _py: Python<'p>) -> Vec<ColumnInfo> {
        self.available_columns.clone()
    }

    #[getter]
    pub fn specified_dtypes<'p>(&'p self, py: Python<'p>) -> Option<PyObject> {
        self.dtypes.as_ref().map(|dtypes| dtypes.to_object(py))
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.sheet_meta.name
    }

    #[getter]
    pub fn visible<'p>(&'p self, py: Python<'p>) -> PyObject {
        let visible: SheetVisible = self.sheet_meta.visible.into();
        (&visible).to_object(py)
    }

    pub fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        RecordBatch::try_from(self)
            .with_context(|| {
                format!(
                    "could not create RecordBatch from sheet \"{}\"",
                    self.name()
                )
            })
            .and_then(|rb| {
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
    }

    pub fn __repr__(&self) -> String {
        format!("ExcelSheet<{}>", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use pyo3::{prelude::PyListMethods, types::PyString};
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
        Python::with_gil(|py| {
            let py_list = PyList::new_bound(py, vec![0, 1, 2]);
            assert_eq!(
                TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap(),
                SelectedColumns::Selection([0, 1, 2].into_iter().map(IdxOrName::Idx).collect())
            )
        });
    }

    #[test]
    fn selected_columns_from_list_of_valid_strings() {
        Python::with_gil(|py| {
            let py_list = PyList::new_bound(py, vec!["foo", "bar"]);
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
        Python::with_gil(|py| {
            let py_list = PyList::new_bound(py, vec!["foo", "bar"]);
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
        Python::with_gil(|py| {
            let py_list = PyList::new_bound(py, vec![0, 2, -1]);
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_int_list() {
        Python::with_gil(|py| {
            let py_list = PyList::new_bound(py, Vec::<usize>::new());
            let err = TryInto::<SelectedColumns>::try_into(Some(py_list.as_ref())).unwrap_err();

            assert!(matches!(err.kind, FastExcelErrorKind::InvalidParameters(_)));
        });
    }

    #[test]
    fn selected_columns_from_empty_string_list() {
        Python::with_gil(|py| {
            let py_list = PyList::new_bound(py, Vec::<String>::new());
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
        Python::with_gil(|py| {
            let expected_range = SelectedColumns::Selection(
                expected_indices.into_iter().map(IdxOrName::Idx).collect(),
            );
            let input = PyString::new_bound(py, raw);

            let range = TryInto::<SelectedColumns>::try_into(Some(input.as_ref()))
                .expect("expected a valid column selection");

            assert_eq!(range, expected_range)
        })
    }

    #[rstest]
    // Standard unique columns
    #[case("", "at least one character")]
    // empty range
    #[case("a:a,b:d,e", "empty range")]
    // end before start
    #[case("b:a", "end of range is before start")]
    // no start
    #[case(":a", "at least one character, got none")]
    // no end
    #[case("a:", "at least one character, got none")]
    // too many elements
    #[case("a:b:e", "exactly 2 elements, got 3")]
    fn selected_columns_from_invalid_ranges(#[case] raw: &str, #[case] message: &str) {
        Python::with_gil(|py| {
            let input = PyString::new_bound(py, raw);

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
