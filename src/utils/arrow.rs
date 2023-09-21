use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use calamine::{DataType as CalDataType, Range};

fn get_arrow_column_type(
    data: &Range<CalDataType>,
    row: usize,
    col: usize,
) -> Result<ArrowDataType> {
    let cell = data
        .get((row, col))
        .with_context(|| format!("Could not retrieve data at ({row},{col})"))?;
    match cell {
        CalDataType::Int(_) => Ok(ArrowDataType::Int64),
        CalDataType::Float(_) => Ok(ArrowDataType::Float64),
        CalDataType::String(_) => Ok(ArrowDataType::Utf8),
        CalDataType::Bool(_) => Ok(ArrowDataType::Boolean),
        CalDataType::DateTime(_) => Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
        // These types contain an ISO8601 representation of a date/datetime or a duration
        CalDataType::DateTimeIso(_) => match cell.as_datetime() {
            // If we cannot convert the cell to a datetime, we're working on a date
            Some(_) => Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
            // NOTE: not using the Date64 type on purpose, as pyarrow converts it to a datetime
            // rather than a date
            None => Ok(ArrowDataType::Date32),
        },
        CalDataType::DurationIso(_) => Ok(ArrowDataType::Duration(TimeUnit::Millisecond)),
        // A simple duration
        CalDataType::Duration(_) => Ok(ArrowDataType::Duration(TimeUnit::Millisecond)),
        // Errors and nulls
        CalDataType::Error(err) => Err(anyhow!("Error in calamine cell: {err:?}")),
        CalDataType::Empty => Ok(ArrowDataType::Null),
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
    range: &Range<CalDataType>,
    column_names: &[String],
    row_idx: usize,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(column_names.len());

    for (col_idx, name) in column_names.iter().enumerate() {
        let col_type = get_arrow_column_type(range, row_idx, col_idx)?;
        fields.push(Field::new(&alias_for_name(name, &fields), col_type, true));
    }

    Ok(Schema::new(fields))
}
