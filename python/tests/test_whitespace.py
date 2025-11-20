import datetime

import fastexcel
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from .utils import path_for_fixture


def test_skip_tail_whitespace_rows() -> None:
    """Test that skip_whitespace_tail_rows option works correctly."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-and-table-with-whitespace.xlsx"))

    # Expected data when NOT skipping whitespace tail rows
    expected_with_whitespace = pl.DataFrame(
        {
            "Column One": ["1", "2", "3", None, "5", None, None, None, None, " "],
            "Column Two": ["one", "two", None, "four", "five", None, None, "", None, None],
            "Column Three": [
                datetime.datetime(2025, 11, 19, 14, 34, 2),
                datetime.datetime(2025, 11, 20, 14, 56, 34),
                datetime.datetime(2025, 11, 21, 15, 19, 6),
                None,
                datetime.datetime(2025, 11, 22, 15, 41, 38),
                datetime.datetime(2025, 11, 23, 16, 4, 10),
                None,
                None,
                None,
                None,
            ],
        }
    ).with_columns(pl.col("Column Three").dt.cast_time_unit("ms"))

    # Expected data when skipping whitespace tail rows
    expected_without_whitespace = pl.DataFrame(
        {
            "Column One": [1.0, 2.0, 3.0, None, 5.0, None],
            "Column Two": ["one", "two", None, "four", "five", None],
            "Column Three": [
                datetime.datetime(2025, 11, 19, 14, 34, 2),
                datetime.datetime(2025, 11, 20, 14, 56, 34),
                datetime.datetime(2025, 11, 21, 15, 19, 6),
                None,
                datetime.datetime(2025, 11, 22, 15, 41, 38),
                datetime.datetime(2025, 11, 23, 16, 4, 10),
            ],
        }
    ).with_columns(pl.col("Column Three").dt.cast_time_unit("ms"))

    # Test sheet without skipping whitespace tail rows
    sheet_with_whitespace = excel_reader.load_sheet("Without Table")
    pl_assert_frame_equal(sheet_with_whitespace.to_polars(), expected_with_whitespace)

    # Test table without skipping whitespace tail rows
    table_with_whitespace = excel_reader.load_table("Table_with_whitespace")
    pl_assert_frame_equal(table_with_whitespace.to_polars(), expected_with_whitespace)

    # Test sheet with skipping whitespace tail rows
    sheet_without_whitespace = excel_reader.load_sheet(
        "Without Table", skip_whitespace_tail_rows=True
    )
    pl_assert_frame_equal(sheet_without_whitespace.to_polars(), expected_without_whitespace)

    # Test table with skipping whitespace tail rows
    table_without_whitespace = excel_reader.load_table(
        "Table_with_whitespace", skip_whitespace_tail_rows=True
    )
    pl_assert_frame_equal(table_without_whitespace.to_polars(), expected_without_whitespace)

    # Also verify pandas compatibility
    pd_assert_frame_equal(
        sheet_without_whitespace.to_pandas(), expected_without_whitespace.to_pandas()
    )
    pd_assert_frame_equal(
        table_without_whitespace.to_pandas(), expected_without_whitespace.to_pandas()
    )


def test_skip_tail_rows_and_whitespace_as_null_behavior() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-and-table-with-whitespace.xlsx"))

    # Expected data when converting whitespace to null but not skipping tail rows
    expected_with_whitespace_as_null = pl.DataFrame(
        {
            # All rows should be taken into account but the space in the last row should be
            # considered null
            "Column One": [1.0, 2.0, 3.0, None, 5.0, None, None, None, None, None],
            # All rows should be taken into account but the empty string in 8th row should be
            # considered null
            "Column Two": ["one", "two", None, "four", "five", None, None, None, None, None],
            "Column Three": [
                datetime.datetime(2025, 11, 19, 14, 34, 2),
                datetime.datetime(2025, 11, 20, 14, 56, 34),
                datetime.datetime(2025, 11, 21, 15, 19, 6),
                None,
                datetime.datetime(2025, 11, 22, 15, 41, 38),
                datetime.datetime(2025, 11, 23, 16, 4, 10),
                None,
                None,
                None,
                None,
            ],
        }
    ).with_columns(pl.col("Column Three").dt.cast_time_unit("ms"))

    # Expected data when converting whitespace to null and skipping tail rows
    expected_without_whitespace = pl.DataFrame(
        {
            "Column One": [1.0, 2.0, 3.0, None, 5.0, None],
            "Column Two": ["one", "two", None, "four", "five", None],
            "Column Three": [
                datetime.datetime(2025, 11, 19, 14, 34, 2),
                datetime.datetime(2025, 11, 20, 14, 56, 34),
                datetime.datetime(2025, 11, 21, 15, 19, 6),
                None,
                datetime.datetime(2025, 11, 22, 15, 41, 38),
                datetime.datetime(2025, 11, 23, 16, 4, 10),
            ],
        }
    ).with_columns(pl.col("Column Three").dt.cast_time_unit("ms"))

    # Test sheet with whitespace_as_null but not skipping tail rows
    sheet_with_whitespace_as_null = excel_reader.load_sheet(
        "Without Table", whitespace_as_null=True
    )
    pl_assert_frame_equal(
        sheet_with_whitespace_as_null.to_polars(), expected_with_whitespace_as_null
    )

    # Test table with whitespace_as_null but not skipping tail rows
    table_with_whitespace_as_null = excel_reader.load_table(
        "Table_with_whitespace", whitespace_as_null=True
    )
    pl_assert_frame_equal(
        table_with_whitespace_as_null.to_polars(), expected_with_whitespace_as_null
    )

    # Test sheet with both whitespace_as_null and skip_whitespace_tail_rows
    sheet_without_whitespace = excel_reader.load_sheet(
        "Without Table", whitespace_as_null=True, skip_whitespace_tail_rows=True
    )
    pl_assert_frame_equal(sheet_without_whitespace.to_polars(), expected_without_whitespace)

    # Test table with both whitespace_as_null and skip_whitespace_tail_rows
    table_without_whitespace = excel_reader.load_table(
        "Table_with_whitespace", whitespace_as_null=True, skip_whitespace_tail_rows=True
    )
    pl_assert_frame_equal(table_without_whitespace.to_polars(), expected_without_whitespace)

    # Also verify pandas compatibility
    pd_assert_frame_equal(
        sheet_without_whitespace.to_pandas(), expected_without_whitespace.to_pandas()
    )
    pd_assert_frame_equal(
        sheet_with_whitespace_as_null.to_pandas(), expected_with_whitespace_as_null.to_pandas()
    )
    pd_assert_frame_equal(
        table_without_whitespace.to_pandas(), expected_without_whitespace.to_pandas()
    )
    pd_assert_frame_equal(
        table_with_whitespace_as_null.to_pandas(), expected_with_whitespace_as_null.to_pandas()
    )
