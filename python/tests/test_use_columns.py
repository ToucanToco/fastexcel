from __future__ import annotations

import fastexcel
import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from utils import path_for_fixture


def test_use_columns_with_use_columns() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet-with-types.xlsx"))

    sheet = excel_reader.load_sheet(
        0,
        use_columns=[1, 2],
        header_row=None,
        skip_rows=1,
        column_names=["bools_renamed", "dates_renamed"],
    )

    assert sheet.available_columns == [
        fastexcel.ColumnInfo(
            name="__UNNAMED__0",
            column_name_from="generated",
            index=0,
            dtype="float",
            dtype_from="guessed",
        ),
        fastexcel.ColumnInfo(
            name="bools_renamed",
            index=1,
            dtype="boolean",
            dtype_from="guessed",
            column_name_from="provided",
        ),
        fastexcel.ColumnInfo(
            name="dates_renamed",
            index=2,
            dtype="datetime",
            dtype_from="guessed",
            column_name_from="provided",
        ),
        fastexcel.ColumnInfo(
            name="__UNNAMED__3",
            index=3,
            dtype="float",
            dtype_from="guessed",
            column_name_from="generated",
        ),
    ]

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "bools_renamed": [True, False, True],
                "dates_renamed": pd.Series([pd.Timestamp("2022-03-02 05:43:04")] * 3).astype(
                    "datetime64[ms]"
                ),
            }
        ),
    )
    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "bools_renamed": [True, False, True],
                "dates_renamed": ["2022-03-02 05:43:04"] * 3,
            }
        ).with_columns(
            pl.col("dates_renamed").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )
