use crate::{FastExcelColumn, FastExcelSeries};
use polars_core::{
    frame::column::{Column as PolarsColumn, ScalarColumn},
    prelude::DataType,
    scalar::Scalar,
};

impl From<FastExcelColumn> for PolarsColumn {
    fn from(column: FastExcelColumn) -> Self {
        let name = column.name().into();
        match column.data {
            FastExcelSeries::Null => PolarsColumn::Scalar(ScalarColumn::new(
                name,
                Scalar::null(DataType::Null),
                column.len(),
            )),
            FastExcelSeries::Bool(values) => PolarsColumn::new(name, values),
            FastExcelSeries::String(values) => PolarsColumn::new(name, values),
            FastExcelSeries::Int(values) => PolarsColumn::new(name, values),
            FastExcelSeries::Float(values) => PolarsColumn::new(name, values),
            FastExcelSeries::Datetime(values) => PolarsColumn::new(name, values),
            FastExcelSeries::Date(values) => PolarsColumn::new(name, values),
            FastExcelSeries::Duration(values) => PolarsColumn::new(name, values),
        }
    }
}
