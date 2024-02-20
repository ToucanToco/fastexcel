from __future__ import annotations

from datetime import date, datetime, timedelta

import fastexcel
import numpy as np
import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.datatypes import Date as PlDate
from polars.datatypes import Datetime as PlDateTime
from polars.datatypes import Duration as PlDuration
from polars.datatypes import PolarsDataType
from polars.datatypes import Utf8 as PlUtf8
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from utils import path_for_fixture


def test_sheet_with_different_time_types() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("dates.ods"))
    sheet = excel_reader.load_sheet_by_idx(0)

    pd_df = sheet.to_pandas()
    pl_df = sheet.to_polars()

    ## dtypes
    assert pd_df.dtypes.to_dict() == {
        # the dtype for a date is object
        "date": np.dtype("object"),
        "datestr": np.dtype("object"),
        "time": np.dtype("timedelta64[ms]"),
        "datetime": np.dtype("datetime64[ms]"),
    }
    expected_pl_dtypes: dict[str, PolarsDataType] = {
        "date": PlDate,
        "datestr": PlUtf8,
        "time": PlDuration(time_unit="ms"),
        "datetime": PlDateTime(time_unit="ms", time_zone=None),
    }
    assert dict(zip(pl_df.columns, pl_df.dtypes)) == expected_pl_dtypes

    ## Contents

    expected_pd = pd.DataFrame(
        {
            "date": [date(2023, 6, 1)],
            "datestr": ["2023-06-01T02:03:04+02:00"],
            "time": pd.Series([pd.to_timedelta("01:02:03")]).astype("timedelta64[ms]"),
            "datetime": pd.Series([pd.to_datetime("2023-06-01 02:03:04")]).astype("datetime64[ms]"),
        }
    )
    expected_pl = pl.DataFrame(
        {
            "date": [date(2023, 6, 1)],
            "datestr": ["2023-06-01T02:03:04+02:00"],
            "time": [timedelta(hours=1, minutes=2, seconds=3)],
            "datetime": [datetime(2023, 6, 1, 2, 3, 4)],
        },
        schema=expected_pl_dtypes,
    )
    pd_assert_frame_equal(pd_df, expected_pd)
    pl_assert_frame_equal(pl_df, expected_pl)


def test_sheet_with_offset_header_row_and_durations() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("single-sheet-skip-rows-durations.xlsx"))
    sheet = excel_reader.load_sheet(0, header_row=9)

    pd_df = sheet.to_pandas()
    pl_df = sheet.to_polars()

    assert pd_df["Tot. Time Away From System"].dtype == np.dtype("timedelta64[ms]")
    assert pd_df["Tot. Time Away From System"].tolist() == [
        pd.Timedelta("01:18:43"),
        pd.Timedelta("07:16:51"),
    ]

    assert pl_df["Tot. Time Away From System"].dtype == pl.Duration(time_unit="ms")
    assert pl_df["Tot. Time Away From System"].to_list() == [
        timedelta(hours=1, minutes=18, seconds=43),
        timedelta(hours=7, minutes=16, seconds=51),
    ]
