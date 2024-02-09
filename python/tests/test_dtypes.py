from __future__ import annotations

from datetime import datetime
from typing import Any

import fastexcel
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from utils import path_for_fixture


@pytest.fixture
def expected_data() -> dict[str, list[Any]]:
    return {
        "Employee ID": [
            "123456",
            "44333",
            "44333",
            "87878",
            "87878",
            "US00011",
            "135967",
            "IN86868",
            "IN86868",
        ],
        "Employee Name": [
            "Test1",
            "Test2",
            "Test2",
            "Test3",
            "Test3",
            "Test4",
            "Test5",
            "Test6",
            "Test6",
        ],
        "Date": [datetime(2023, 7, 21)] * 9,
        "Details": ["Healthcare"] * 7 + ["Something"] * 2,
        "Asset ID": ["84444"] * 7 + ["ABC123"] * 2,
    }


def test_sheet_with_mixed_dtypes(expected_data: dict[str, list[Any]]) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))
    sheet = excel_reader.load_sheet(0)

    pd_df = sheet.to_pandas()
    pd_assert_frame_equal(pd_df, pd.DataFrame(expected_data).astype({"Date": "datetime64[ms]"}))

    pl_df = sheet.to_polars()
    pl_assert_frame_equal(
        pl_df, pl.DataFrame(expected_data, schema_overrides={"Date": pl.Datetime(time_unit="ms")})
    )


def test_sheet_with_mixed_dtypes_and_sample_rows(expected_data: dict[str, list[Any]]) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))

    # Since we skip rows here, the dtypes should be correctly guessed, even if we only check 5 rows
    sheet = excel_reader.load_sheet(0, schema_sample_rows=5, skip_rows=5)

    expected_data_subset = {col_name: values[5:] for col_name, values in expected_data.items()}
    pd_df = sheet.to_pandas()
    pd_assert_frame_equal(
        pd_df, pd.DataFrame(expected_data_subset).astype({"Date": "datetime64[ms]"})
    )

    pl_df = sheet.to_polars()
    pl_assert_frame_equal(
        pl_df,
        pl.DataFrame(expected_data_subset, schema_overrides={"Date": pl.Datetime(time_unit="ms")}),
    )

    # Guess the sheet's dtypes on 5 rows only
    sheet = excel_reader.load_sheet(0, schema_sample_rows=5)
    # String fields should not have been loaded
    expected_data["Employee ID"] = [
        123456.0,
        44333.0,
        44333.0,
        87878.0,
        87878.0,
        None,
        135967.0,
        None,
        None,
    ]
    expected_data["Asset ID"] = [84444.0] * 7 + [None] * 2

    pd_df = sheet.to_pandas()
    pd_assert_frame_equal(pd_df, pd.DataFrame(expected_data).astype({"Date": "datetime64[ms]"}))

    pl_df = sheet.to_polars()
    pl_assert_frame_equal(
        pl_df, pl.DataFrame(expected_data, schema_overrides={"Date": pl.Datetime(time_unit="ms")})
    )
