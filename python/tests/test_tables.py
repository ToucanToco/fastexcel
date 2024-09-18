from datetime import datetime

import fastexcel
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from utils import path_for_fixture


@pytest.mark.parametrize("path", ("sheet-with-tables.xlsx",))
def test_table_names(path: str) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture(path))
    table_names = excel_reader.table_names()

    assert table_names == ["users"]


@pytest.mark.parametrize("path", ("sheet-with-tables.xlsx",))
def test_table_names_with_sheet_name(path: str) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture(path))
    table_names = excel_reader.table_names("sheet1")

    assert table_names == ["users"]

    table_names = excel_reader.table_names("sheet2")

    assert table_names == []


@pytest.mark.parametrize("path", ("sheet-with-tables.xlsx",))
def test_load_table(path: str) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture(path))
    users_tbl = excel_reader.load_table("users")

    assert users_tbl.name == "users"
    assert users_tbl.sheet_name == "sheet1"
    assert users_tbl.specified_dtypes is None
    assert users_tbl.available_columns == [
        fastexcel.ColumnInfo(
            name="User Id",
            index=0,
            dtype="float",
            dtype_from="guessed",
            column_name_from="provided",
        ),
        fastexcel.ColumnInfo(
            name="FirstName",
            index=1,
            dtype="string",
            dtype_from="guessed",
            column_name_from="provided",
        ),
        fastexcel.ColumnInfo(
            name="LastName",
            index=2,
            dtype="string",
            dtype_from="guessed",
            column_name_from="provided",
        ),
        fastexcel.ColumnInfo(
            name="Date",
            index=3,
            dtype="datetime",
            dtype_from="guessed",
            column_name_from="provided",
        ),
    ]
    assert users_tbl.total_height == 3
    assert users_tbl.offset == 0
    assert users_tbl.height == 3
    assert users_tbl.width == 4

    expected_pl = pl.DataFrame(
        {
            "User Id": [1.0, 2.0, 5.0],
            "FirstName": ["Peter", "John", "Hans"],
            "LastName": ["Müller", "Meier", "Fricker"],
            "Date": [datetime(2020, 1, 1), datetime(2024, 5, 4), datetime(2025, 2, 1)],
        }
    ).with_columns(pl.col("Date").dt.cast_time_unit("ms"))
    pl_assert_frame_equal(users_tbl.to_polars(), expected_pl)

    expected_pd = pd.DataFrame(
        {
            "User Id": [1.0, 2.0, 5.0],
            "FirstName": ["Peter", "John", "Hans"],
            "LastName": ["Müller", "Meier", "Fricker"],
            "Date": pd.Series(
                [datetime(2020, 1, 1), datetime(2024, 5, 4), datetime(2025, 2, 1)]
            ).astype("datetime64[ms]"),
        }
    )

    pd_assert_frame_equal(users_tbl.to_pandas(), expected_pd)

    table_eager = excel_reader.load_table("users", eager=True)
    pl_assert_frame_equal(pl.from_arrow(table_eager), expected_pl)  # type:ignore[arg-type]
    pd_assert_frame_equal(table_eager.to_pandas(), expected_pd)
