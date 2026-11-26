#[allow(unused_macros)]
mod utils;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate};
use fastexcel::PropertyDate;
use pretty_assertions::assert_eq;

use crate::utils::path_for_fixture;

#[test]
fn workbook_properties_xlsx() -> Result<()> {
    let mut reader = fastexcel::read_excel(path_for_fixture("infer-dtypes-fallback.xlsx"))
        .context("could not read excel file")?;
    let properties = reader.workbook_properties()?;

    assert_eq!(properties.creator.as_deref(), Some("Montes, Adrian"));
    assert_eq!(properties.company.as_deref(), Some("Hudson Advisors"));
    let created = DateTime::parse_from_rfc3339("2024-10-11T12:55:22Z")
        .ok()
        .context("invalid date")?;
    assert_eq!(properties.created, Some(PropertyDate::Aware(created)));
    assert!(properties.custom_properties.is_empty());
    Ok(())
}

#[test]
fn workbook_properties_ods() -> Result<()> {
    let mut reader = fastexcel::read_excel(path_for_fixture("dates.ods"))
        .context("could not read excel file")?;
    let properties = reader.workbook_properties()?;

    let created = NaiveDate::from_ymd_opt(2023, 6, 1)
        .and_then(|date| date.and_hms_nano_opt(17, 15, 3, 994_817_446))
        .context("invalid date")?;
    assert_eq!(properties.created, Some(PropertyDate::Naive(created)));
    assert_eq!(properties.creator, None);
    Ok(())
}
