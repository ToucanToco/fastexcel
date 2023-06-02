from datetime import date, datetime, timedelta

import fastexcel
import numpy as np
import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.datatypes import Date as PlDate
from polars.datatypes import Datetime as PlDateTime
from polars.datatypes import Duration as PlDuration
from polars.datatypes import Utf8 as PlUtf8
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from tests.utils import path_for_fixture


def test_sheet_with_different_time_types() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("dates.ods"))
    sheet = excel_reader.load_sheet_by_idx(0)

    pd_df = sheet.to_pandas()
    pl_df = sheet.to_polars()

    ## dtypes
    # PyArrow always converts to ns precision, even though we're in ms ¯\_(ツ)_/¯
    assert pd_df.dtypes.to_dict() == {
        # the dtype for a date is object
        "date": np.dtype("object"),
        "datestr": np.dtype("object"),
        "time": np.dtype("timedelta64[ns]"),
        "datetime": np.dtype("datetime64[ns]"),
    }
    expected_pl_dtypes = {
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
            "time": [pd.to_timedelta("01:02:03")],
            "datetime": [pd.to_datetime("2023-06-01 02:03:04")],
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
