#[cfg(feature = "python")]
mod python;

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    str::FromStr,
    sync::OnceLock,
};

use calamine::{CellErrorType, CellType, DataType, Range};
use log::warn;
#[cfg(feature = "python")]
use pyo3::{IntoPyObject, IntoPyObjectRef};

use crate::error::{FastExcelError, FastExcelErrorKind, FastExcelResult};

use super::idx_or_name::IdxOrName;

/// A column or a cell's data type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum DType {
    Null,
    Int,
    Float,
    String,
    Bool,
    DateTime,
    Date,
    Duration,
}

impl FromStr for DType {
    type Err = FastExcelError;

    fn from_str(raw_dtype: &str) -> FastExcelResult<Self> {
        match raw_dtype {
            "null" => Ok(Self::Null),
            "int" => Ok(Self::Int),
            "float" => Ok(Self::Float),
            "string" => Ok(Self::String),
            "boolean" => Ok(Self::Bool),
            "datetime" => Ok(Self::DateTime),
            "date" => Ok(Self::Date),
            "duration" => Ok(Self::Duration),
            _ => Err(FastExcelErrorKind::InvalidParameters(format!(
                "unsupported dtype: \"{raw_dtype}\""
            ))
            .into()),
        }
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DType::Null => "null",
            DType::Int => "int",
            DType::Float => "float",
            DType::String => "string",
            DType::Bool => "boolean",
            DType::DateTime => "datetime",
            DType::Date => "date",
            DType::Duration => "duration",
        })
    }
}

pub type DTypeMap = HashMap<IdxOrName, DType>;

/// Provided data types.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", derive(IntoPyObject, IntoPyObjectRef))]
pub enum DTypes {
    /// Coerce all data types to the given type.
    All(DType),
    /// Coerce data types based on the provided map.
    Map(DTypeMap),
}

impl FromStr for DTypes {
    type Err = FastExcelError;

    fn from_str(dtypes: &str) -> FastExcelResult<Self> {
        Ok(DTypes::All(DType::from_str(dtypes)?))
    }
}

/// Whether data types should be coerced or not.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy, Default)]
pub enum DTypeCoercion {
    /// Coerce data types (default).
    #[default]
    Coerce,
    /// Strictly enforce data types.
    Strict,
}

impl FromStr for DTypeCoercion {
    type Err = FastExcelError;

    fn from_str(raw_dtype_coercion: &str) -> FastExcelResult<Self> {
        match raw_dtype_coercion {
            "coerce" => Ok(Self::Coerce),
            "strict" => Ok(Self::Strict),
            _ => Err(FastExcelErrorKind::InvalidParameters(format!(
                "unsupported dtype_coercion: \"{raw_dtype_coercion}\""
            ))
            .into()),
        }
    }
}

/// All the possible string values that should be considered as NULL
const NULL_STRING_VALUES: [&str; 19] = [
    "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN",
    "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null",
];

fn get_cell_dtype<DT: CellType + Debug + DataType>(
    data: &Range<DT>,
    row: usize,
    col: usize,
) -> FastExcelResult<DType> {
    let cell = data
        .get((row, col))
        .ok_or(FastExcelErrorKind::CannotRetrieveCellData(row, col))?;

    if cell.is_int() {
        Ok(DType::Int)
    } else if cell.is_float() {
        Ok(DType::Float)
    } else if cell.is_string() {
        if NULL_STRING_VALUES.contains(&cell.get_string().unwrap()) {
            Ok(DType::Null)
        } else {
            Ok(DType::String)
        }
    } else if cell.is_bool() {
        Ok(DType::Bool)
    } else if cell.is_datetime() {
        // Since calamine 0.24.0, a new ExcelDateTime exists for the Datetime type. It can either be
        // a duration or a datatime
        let excel_datetime = cell
            .get_datetime()
            .expect("calamine indicated that cell is a datetime but get_datetime returned None");
        Ok(if excel_datetime.is_datetime() {
            DType::DateTime
        } else {
            DType::Duration
        })
    }
    // These types contain an ISO8601 representation of a date/datetime or a durat
    else if cell.is_datetime_iso() {
        match cell.as_datetime() {
            // If we cannot convert the cell to a datetime, we're working on a date
            Some(_) => Ok(DType::DateTime),
            // NOTE: not using the Date64 type on purpose, as pyarrow converts it to a datetime
            // rather than a date
            None => Ok(DType::Date),
        }
    }
    // Simple durations
    else if cell.is_duration_iso() {
        Ok(DType::Duration)
    }
    // Empty cell
    else if cell.is_empty() {
        Ok(DType::Null)
    } else if cell.is_error() {
        match cell.get_error() {
            // considering cells with #N/A! or #REF! as null
            Some(
                CellErrorType::NA
                | CellErrorType::Value
                | CellErrorType::Null
                | CellErrorType::Ref
                | CellErrorType::Num
                | CellErrorType::Div0,
            ) => Ok(DType::Null),
            Some(err) => Err(FastExcelErrorKind::CalamineCellError(err.to_owned()).into()),
            None => Err(FastExcelErrorKind::Internal(format!(
                "cell is an error but get_error returned None: {cell:?}"
            ))
            .into()),
        }
    } else {
        Err(FastExcelErrorKind::Internal(format!("unsupported cell type: {cell:?}")).into())
    }
}

static FLOAT_TYPES_CELL: OnceLock<HashSet<DType>> = OnceLock::new();
static INT_TYPES_CELL: OnceLock<HashSet<DType>> = OnceLock::new();
static STRING_TYPES_CELL: OnceLock<HashSet<DType>> = OnceLock::new();

fn float_types() -> &'static HashSet<DType> {
    FLOAT_TYPES_CELL.get_or_init(|| HashSet::from([DType::Int, DType::Float, DType::Bool]))
}

fn int_types() -> &'static HashSet<DType> {
    INT_TYPES_CELL.get_or_init(|| HashSet::from([DType::Int, DType::Bool]))
}

fn string_types() -> &'static HashSet<DType> {
    STRING_TYPES_CELL.get_or_init(|| {
        HashSet::from([
            DType::Bool,
            DType::Int,
            DType::Float,
            DType::String,
            DType::DateTime,
            DType::Date,
        ])
    })
}

pub(crate) fn get_dtype_for_column<DT: CellType + Debug + DataType>(
    data: &Range<DT>,
    start_row: usize,
    end_row: usize,
    col: usize,
    dtype_coercion: &DTypeCoercion,
) -> FastExcelResult<DType> {
    let mut column_types = (start_row..end_row)
        .map(|row| get_cell_dtype(data, row, col))
        .collect::<FastExcelResult<HashSet<_>>>()?;

    // All columns are nullable anyway so we're not taking Null into account here
    column_types.remove(&DType::Null);

    if column_types.is_empty() {
        // If no type apart from NULL was found, fallback to string except if the column is empty
        if start_row == end_row {
            Ok(DType::Null)
        } else {
            warn!("Could not determine dtype for column {col}, falling back to string");
            Ok(DType::String)
        }
    } else if matches!(dtype_coercion, &DTypeCoercion::Strict) && column_types.len() != 1 {
        // If dtype coercion is strict and we do not have a single dtype, it's an error
        Err(
            FastExcelErrorKind::UnsupportedColumnTypeCombination(format!(
                "type coercion is strict and column contains {column_types:?}"
            ))
            .into(),
        )
    } else if column_types.len() == 1 {
        // If a single non-null type was found, return it
        Ok(column_types.into_iter().next().unwrap())
    } else if column_types.is_subset(int_types()) {
        // If every cell in the column can be converted to an int, return int64
        Ok(DType::Int)
    } else if column_types.is_subset(float_types()) {
        // If every cell in the column can be converted to a float, return Float64
        Ok(DType::Float)
    } else if column_types.is_subset(string_types()) {
        // If every cell in the column can be converted to a string, return Utf8
        Ok(DType::String)
    } else {
        // NOTE: Not being too smart about multi-types columns for now
        Err(
            FastExcelErrorKind::UnsupportedColumnTypeCombination(format!("{column_types:?}"))
                .into(),
        )
    }
}

/// Convert a float to a nice string to mimic Excel behaviour.
///
/// Excel can store a float like 29.02 set by the user as "29.020000000000003" in the XML.
/// But in fact, the user will see "29.02" in the cell.
/// Excel indeed displays decimal numbers with 8 digits in a standard cell width
/// and 10 digits in a wide cell. Like this:
///
/// Format = 0.000000000 |  Unformatted, wide cell  | Unformatted, standard width
/// ---------------------|--------------------------|----------------------------
///     1.123456789      |        1.123456789       |           1.123457
///    12.123456789      |        12.12345679       |           12.12346
///         ...          |            ...           |              ...
///   123456.123456789   |        123456.1235       |           123456.1
///
/// Excel also trims trailing zeros and the decimal point if there is no fractional part.
///
/// We do not distinguish between wide cells and standard cells here, so we retain at most
/// nine digits after the decimal point and trim any trailing zeros.
pub(crate) fn excel_float_to_string(x: f64) -> String {
    format!("{x:.9}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

#[cfg(feature = "__pyo3-tests")]
#[cfg(test)]
mod tests {
    use calamine::{Cell, Data as CalData};
    use rstest::{fixture, rstest};

    use super::*;

    #[fixture]
    fn range() -> Range<CalData> {
        Range::from_sparse(vec![
            // First column
            Cell::new((0, 0), CalData::Bool(true)),
            Cell::new((1, 0), CalData::Bool(false)),
            Cell::new((2, 0), CalData::String("NULL".to_string())),
            Cell::new((3, 0), CalData::Int(42)),
            Cell::new((4, 0), CalData::Float(13.37)),
            Cell::new((5, 0), CalData::String("hello".to_string())),
            Cell::new((6, 0), CalData::Empty),
            Cell::new((7, 0), CalData::String("#N/A".to_string())),
            Cell::new((8, 0), CalData::Int(12)),
            Cell::new((9, 0), CalData::Float(12.21)),
            Cell::new((10, 0), CalData::Bool(true)),
            Cell::new((11, 0), CalData::Int(1337)),
        ])
    }

    #[rstest]
    // pure bool
    #[case(0, 2, DType::Bool)]
    // pure int
    #[case(3, 4, DType::Int)]
    // pure float
    #[case(4, 5, DType::Float)]
    // pure string
    #[case(5, 6, DType::String)]
    // pure int + float
    #[case(3, 5, DType::Float)]
    // null + int + float
    #[case(2, 5, DType::Float)]
    // float + string
    #[case(4, 6, DType::String)]
    // int + float + string
    #[case(3, 6, DType::String)]
    // null + int + float + string + empty + null
    #[case(2, 8, DType::String)]
    // empty + null + int
    #[case(6, 9, DType::Int)]
    // int + float + null
    #[case(7, 10, DType::Float)]
    // int + float + bool + null
    #[case(7, 11, DType::Float)]
    // int + bool
    #[case(10, 12, DType::Int)]
    fn get_arrow_column_type_multi_dtype_ok_coerce(
        range: Range<CalData>,
        #[case] start_row: usize,
        #[case] end_row: usize,
        #[case] expected: DType,
    ) {
        assert_eq!(
            get_dtype_for_column(&range, start_row, end_row, 0, &DTypeCoercion::Coerce).unwrap(),
            expected
        );
    }

    #[rstest]
    // pure bool
    #[case(0, 2, DType::Bool)]
    // pure int
    #[case(3, 4, DType::Int)]
    // pure float
    #[case(4, 5, DType::Float)]
    // pure string
    #[case(5, 6, DType::String)]
    // empty + null + int
    #[case(6, 9, DType::Int)]
    fn get_arrow_column_type_multi_dtype_ok_strict(
        range: Range<CalData>,
        #[case] start_row: usize,
        #[case] end_row: usize,
        #[case] expected: DType,
    ) {
        assert_eq!(
            get_dtype_for_column(&range, start_row, end_row, 0, &DTypeCoercion::Strict).unwrap(),
            expected
        );
    }

    #[rstest]
    // pure int + float
    #[case(3, 5)]
    // float + string
    #[case(4, 6)]
    // int + float + string
    #[case(3, 6)]
    // null + int + float + string + empty + null
    #[case(2, 8)]
    // int + float + null
    #[case(7, 10)]
    // int + float + bool + null
    #[case(7, 11)]
    // int + bool
    #[case(10, 12)]
    fn get_arrow_column_type_multi_dtype_ko_strict(
        range: Range<CalData>,
        #[case] start_row: usize,
        #[case] end_row: usize,
    ) {
        let result = get_dtype_for_column(&range, start_row, end_row, 0, &DTypeCoercion::Strict);
        assert!(matches!(
            result.unwrap_err().kind,
            FastExcelErrorKind::UnsupportedColumnTypeCombination(_)
        ));
    }

    #[rstest]
    #[case(29.020000000000003, "29.02")]
    #[case(10000_f64, "10000")]
    #[case(23.0, "23")]
    fn test_excel_float_to_string(#[case] x: f64, #[case] expected: &str) {
        assert_eq!(excel_float_to_string(x), expected.to_string());
    }
}
