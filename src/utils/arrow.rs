use std::{collections::HashSet, sync::OnceLock};

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use calamine::{Data as CalData, DataType, Range};

fn get_cell_type(data: &Range<CalData>, row: usize, col: usize) -> Result<ArrowDataType> {
    let cell = data
        .get((row, col))
        .with_context(|| format!("Could not retrieve data at ({row},{col})"))?;
    match cell {
        CalData::Int(_) => Ok(ArrowDataType::Int64),
        CalData::Float(_) => Ok(ArrowDataType::Float64),
        CalData::String(_) => Ok(ArrowDataType::Utf8),
        CalData::Bool(_) => Ok(ArrowDataType::Boolean),
        // Since calamine 0.24.0, a new ExcelDateTime exists for the Datetime type. It can either be
        // a duration or a datatime
        CalData::DateTime(excel_datetime) => Ok(if excel_datetime.is_datetime() {
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        } else {
            ArrowDataType::Duration(TimeUnit::Millisecond)
        }),
        // These types contain an ISO8601 representation of a date/datetime or a duration
        CalData::DateTimeIso(_) => match cell.as_datetime() {
            // If we cannot convert the cell to a datetime, we're working on a date
            Some(_) => Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
            // NOTE: not using the Date64 type on purpose, as pyarrow converts it to a datetime
            // rather than a date
            None => Ok(ArrowDataType::Date32),
        },
        // A simple duration
        CalData::DurationIso(_) => Ok(ArrowDataType::Duration(TimeUnit::Millisecond)),
        // Errors and nulls
        CalData::Error(err) => Err(anyhow!("Error in calamine cell: {err:?}")),
        CalData::Empty => Ok(ArrowDataType::Null),
    }
}

static FLOAT_TYPES_CELL: OnceLock<HashSet<ArrowDataType>> = OnceLock::new();
static STRING_TYPES_CELL: OnceLock<HashSet<ArrowDataType>> = OnceLock::new();

// NOTE: Add Bools in here as well once https://github.com/tafia/calamine/pull/399 is merged
fn float_types() -> &'static HashSet<ArrowDataType> {
    FLOAT_TYPES_CELL.get_or_init(|| HashSet::from([ArrowDataType::Int64, ArrowDataType::Float64]))
}

fn string_types() -> &'static HashSet<ArrowDataType> {
    STRING_TYPES_CELL.get_or_init(|| {
        HashSet::from([
            ArrowDataType::Int64,
            ArrowDataType::Float64,
            ArrowDataType::Utf8,
        ])
    })
}

fn get_arrow_column_type(
    data: &Range<CalData>,
    start_row: usize,
    end_row: usize,
    col: usize,
) -> Result<ArrowDataType> {
    let mut column_types = (start_row..end_row)
        .map(|row| get_cell_type(data, row, col))
        .collect::<Result<HashSet<_>>>()?;

    // All columns are nullable anyway so we're not taking Null into account here
    column_types.remove(&ArrowDataType::Null);

    if column_types.is_empty() {
        // If no type apart from NULL was found, it's a NULL column
        Ok(ArrowDataType::Null)
    } else if column_types.len() == 1 {
        // If a single non-null type was found, return it
        Ok(column_types.into_iter().next().unwrap())
    } else if column_types.is_subset(float_types()) {
        // If every cell in the column can be converted to a float, return Float64
        Ok(ArrowDataType::Float64)
    } else if column_types.is_subset(string_types()) {
        // If every cell in the column can be converted to a string, return Utf8
        Ok(ArrowDataType::Utf8)
    } else {
        // NOTE: Not being too smart about multi-types columns for now
        Err(anyhow!(
            "could not figure out column type for following type combination: {column_types:?}"
        ))
    }
}

fn alias_for_name(name: &str, fields: &[Field]) -> String {
    fn rec(name: &str, fields: &[Field], depth: usize) -> String {
        let alias = if depth == 0 {
            name.to_owned()
        } else {
            format!("{name}_{depth}")
        };
        match fields.iter().any(|f| f.name() == &alias) {
            true => rec(name, fields, depth + 1),
            false => alias,
        }
    }

    rec(name, fields, 0)
}

pub(crate) fn arrow_schema_from_column_names_and_range(
    range: &Range<CalData>,
    column_names: &[String],
    row_idx: usize,
    row_limit: usize,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(column_names.len());

    for (col_idx, name) in column_names.iter().enumerate() {
        let col_type = get_arrow_column_type(range, row_idx, row_limit, col_idx)?;
        fields.push(Field::new(&alias_for_name(name, &fields), col_type, true));
    }

    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use calamine::Cell;
    use rstest::{fixture, rstest};

    use super::*;

    #[fixture]
    fn range() -> Range<CalData> {
        Range::from_sparse(vec![
            // First column
            Cell::new((0, 0), CalData::Bool(true)),
            Cell::new((1, 0), CalData::Bool(false)),
            Cell::new((2, 0), CalData::Int(42)),
            Cell::new((3, 0), CalData::Float(13.37)),
            Cell::new((4, 0), CalData::String("hello".to_string())),
            Cell::new((5, 0), CalData::Empty),
            Cell::new((6, 0), CalData::Int(12)),
            Cell::new((7, 0), CalData::Float(12.21)),
        ])
    }

    #[rstest]
    // pure bool
    #[case(0, 2, ArrowDataType::Boolean)]
    // pure int
    #[case(2, 3, ArrowDataType::Int64)]
    // pure float
    #[case(3, 4, ArrowDataType::Float64)]
    // pure string
    #[case(4, 5, ArrowDataType::Utf8)]
    // pure int + float
    #[case(2, 4, ArrowDataType::Float64)]
    // float + string
    #[case(3, 5, ArrowDataType::Utf8)]
    // int + float + string
    #[case(2, 5, ArrowDataType::Utf8)]
    // int + float + string + empty
    #[case(2, 6, ArrowDataType::Utf8)]
    // int + null
    #[case(5, 7, ArrowDataType::Int64)]
    // int + float + null
    #[case(5, 8, ArrowDataType::Float64)]
    fn get_arrow_column_type_multi_dtype_ok(
        range: Range<CalData>,
        #[case] start_row: usize,
        #[case] end_row: usize,
        #[case] expected: ArrowDataType,
    ) {
        assert_eq!(
            get_arrow_column_type(&range, start_row, end_row, 0).unwrap(),
            expected
        );
    }
}
