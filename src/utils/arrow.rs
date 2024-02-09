use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use calamine::{Data as CalData, DataType, Range};

fn get_arrow_column_type(data: &Range<CalData>, row: usize, col: usize) -> Result<ArrowDataType> {
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
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(column_names.len());

    for (col_idx, name) in column_names.iter().enumerate() {
        let col_type = get_arrow_column_type(range, row_idx, col_idx)?;
        fields.push(Field::new(&alias_for_name(name, &fields), col_type, true));
    }

    Ok(Schema::new(fields))
}
