from __future__ import annotations

import fastexcel
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from utils import path_for_fixture


@pytest.mark.parametrize(
    "use_columns", [None, [0, 1, 2], ["col", "col_1", "col_2"], [0, "col_1", 2]]
)
def test_alias_generation_with_use_columns(use_columns: list[str] | list[int] | None) -> None:
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-duplicated-columns.xlsx")
    )

    sheet = excel_reader.load_sheet(0, use_columns=use_columns)
    assert [col.name for col in sheet.available_columns] == ["col", "col_1", "col_2"]

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "col": [1.0, 2.0],
                "col_1": [2019.0, 2020.0],
                "col_2": pd.Series(
                    [pd.Timestamp("2019-02-01 00:01:02"), pd.Timestamp("2014-01-02 06:01:02")]
                ).astype("datetime64[ms]"),
            }
        ),
    )
    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "col": [1.0, 2.0],
                "col_1": [2019.0, 2020.0],
                "col_2": ["2019-02-01 00:01:02", "2014-01-02 06:01:02"],
            }
        ).with_columns(pl.col("col_2").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")),
    )
