use std::fmt::Debug;

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use calamine::{CellType, DataTypeTrait, Range};

fn get_arrow_column_type<DT: CellType + DataTypeTrait + Debug>(
    data: &Range<DT>,
    row: usize,
    col: usize,
) -> Result<ArrowDataType> {
    let cell = data
        .get((row, col))
        .with_context(|| format!("Could not retrieve data at ({row},{col})"))?;
    if cell.is_int() {
        Ok(ArrowDataType::Int64)
    } else if cell.is_float() {
        Ok(ArrowDataType::Float64)
    } else if cell.is_string() {
        Ok(ArrowDataType::Utf8)
    } else if cell.is_bool() {
        Ok(ArrowDataType::Boolean)
    } else if cell.is_datetime() {
        Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None))
    }
    // These types contain an ISO8601 representation of a date/datetime or a durat
    else if cell.is_datetime_iso() {
        match cell.as_datetime() {
            // If we cannot convert the cell to a datetime, we're working on a date
            Some(_) => Ok(ArrowDataType::Timestamp(TimeUnit::Millisecond, None)),
            // NOTE: not using the Date64 type on purpose, as pyarrow converts it to a datetime
            // rather than a date
            None => Ok(ArrowDataType::Date32),
        }
    }
    // Simple durations
    else if cell.is_duration() || cell.is_duration_iso() {
        Ok(ArrowDataType::Duration(TimeUnit::Millisecond))
    } else if cell.is_empty() {
        Ok(ArrowDataType::Null)
    }
    // Error datatype
    else {
        Err(anyhow!("Unexpected cell type: {cell:?}"))
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

pub(crate) fn arrow_schema_from_column_names_and_range<DT: CellType + DataTypeTrait + Debug>(
    range: &Range<DT>,
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
