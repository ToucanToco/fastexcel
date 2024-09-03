from datetime import datetime

import fastexcel
import polars as pl
import pytest
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
    users_tbl = excel_reader.load_table("users").to_polars()

    target_tbl = pl.DataFrame(
        {
            "User Id": [1.0, 2.0, 5.0],
            "FirstName": ["Peter", "John", "Hans"],
            "LastName": ["Müller", "Meier", "Fricker"],
            "Date": [datetime(2020, 1, 1), datetime(2024, 5, 4), datetime(2025, 2, 1)],
        }
    ).with_columns(pl.col("Date").dt.cast_time_unit("ms"))

    pl_assert_frame_equal(users_tbl, target_tbl)
