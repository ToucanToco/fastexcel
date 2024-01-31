from datetime import datetime

import fastexcel
import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from utils import path_for_fixture


def test_sheet_with_mixed_dtypes() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))
    sheet = excel_reader.load_sheet(0)

    expected_data = {
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

    pd_df = sheet.to_pandas()
    pd_assert_frame_equal(pd_df, pd.DataFrame(expected_data).astype({"Date": "datetime64[ms]"}))

    pl_df = sheet.to_polars()
    pl_assert_frame_equal(
        pl_df, pl.DataFrame(expected_data, schema_overrides={"Date": pl.Datetime(time_unit="ms")})
    )
