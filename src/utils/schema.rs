use std::cmp::min;

/// Determines how many rows should be used for schema sampling, based on the provided parameter,
/// and the sheet's offset and limit.
///
/// Note that here, the limit should be retrieved from the sheet's `limit()` method, and must not
/// be out of the sheet's bounds
pub(crate) fn get_schema_sample_rows(
    sample_rows: Option<usize>,
    offset: usize,
    limit: usize,
) -> usize {
    // Checking how many rows we want to use to determine the dtype for a column. If sample_rows is
    // not provided, we sample limit rows, i.e on the entire column
    let sample_rows = offset + sample_rows.unwrap_or(limit);
    // If sample_rows is higher than the sheet's limit, use the limit instead
    min(sample_rows, limit)
}

#[cfg(feature = "__pyo3-tests")]
#[cfg(test)]
mod tests {
    use super::get_schema_sample_rows;
    use rstest::rstest;

    #[rstest]
    // default value, 50 rows sheet, row limit should be 50
    #[case(Some(1000), 0, 50, 50)]
    // default value, 5000 rows sheet, row limit should be 1000
    #[case(Some(1000), 0, 5000, 1000)]
    // default value, 1500 rows sheet, offset of 1000, row limit should be 1500
    #[case(Some(1000), 1000, 1500, 1500)]
    // 100 sampling size, 1500 rows sheet, offset of 1000, row limit should be 1100
    #[case(Some(100), 1000, 1500, 1100)]
    // No value, 50 rows sheet, row limit should be 50
    #[case(None, 0, 50, 50)]
    // No value, 5000 rows sheet, row limit should be 5000
    #[case(None, 0, 5000, 5000)]
    // no value, 1500 rows sheet, offset of 1000, row limit should be 1500
    #[case(None, 1000, 1500, 1500)]
    fn test_get_schema_sample_rows_return_values(
        #[case] sample_rows: Option<usize>,
        #[case] offset: usize,
        #[case] limit: usize,
        #[case] expected: usize,
    ) {
        assert_eq!(get_schema_sample_rows(sample_rows, offset, limit), expected);
    }
}
