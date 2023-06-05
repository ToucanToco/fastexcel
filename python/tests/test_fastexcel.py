import fastexcel
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from utils import path_for_fixture


def test_single_sheet_to_pandas():
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    assert excel_reader.sheet_names == ["January"]
    sheet_by_name = excel_reader.load_sheet("January")
    sheet_by_idx = excel_reader.load_sheet(0)

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "January"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 2

    expected = {"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}

    pd_expected = pd.DataFrame(expected)
    pd_assert_frame_equal(sheet_by_name.to_pandas(), pd_expected)
    pd_assert_frame_equal(sheet_by_idx.to_pandas(), pd_expected)

    pl_expected = pl.DataFrame(expected)
    pl_assert_frame_equal(sheet_by_name.to_polars(), pl_expected)
    pl_assert_frame_equal(sheet_by_idx.to_polars(), pl_expected)


def test_single_sheet_with_types_to_pandas():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    sheet = excel_reader.load_sheet(0)
    assert sheet.name == "Sheet1"
    assert sheet.height == sheet.total_height == 3
    assert sheet.width == 4

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "__UNNAMED__0": [0.0, 1.0, 2.0],
                "bools": [True, False, True],
                "dates": [pd.Timestamp("2022-03-02 05:43:04")] * 3,
                "floats": [12.35, 42.69, 1234567],
            }
        ),
    )

    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "__UNNAMED__0": [0.0, 1.0, 2.0],
                "bools": [True, False, True],
                "dates": ["2022-03-02 05:43:04"] * 3,
                "floats": [12.35, 42.69, 1234567],
            }
        ).with_columns(
            pl.col("dates").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )


def test_multiple_sheets_to_pandas():
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))
    assert excel_reader.sheet_names == ["January", "February", "With unnamed columns"]

    pd_assert_frame_equal(
        excel_reader.load_sheet_by_idx(0).to_pandas(),
        pd.DataFrame({"Month": [1.0], "Year": [2019.0]}),
    )
    pd_assert_frame_equal(
        excel_reader.load_sheet_by_idx(1).to_pandas(),
        pd.DataFrame({"Month": [2.0, 3.0, 4.0], "Year": [2019.0, 2021.0, 2022.0]}),
    )
    pd_assert_frame_equal(
        excel_reader.load_sheet_by_name("With unnamed columns").to_pandas(),
        pd.DataFrame(
            {
                "col1": [2.0, 3.0],
                "__UNNAMED__1": [1.5, 2.5],
                "col3": ["hello", "world"],
                "__UNNAMED__3": [-5.0, -6.0],
                "col5": ["a", "b"],
            }
        ),
    )

    pl_assert_frame_equal(
        excel_reader.load_sheet_by_idx(0).to_polars(),
        pl.DataFrame({"Month": [1.0], "Year": [2019.0]}),
    )
    pl_assert_frame_equal(
        excel_reader.load_sheet_by_idx(1).to_polars(),
        pl.DataFrame({"Month": [2.0, 3.0, 4.0], "Year": [2019.0, 2021.0, 2022.0]}),
    )
    pl_assert_frame_equal(
        excel_reader.load_sheet_by_name("With unnamed columns").to_polars(),
        pl.DataFrame(
            {
                "col1": [2.0, 3.0],
                "__UNNAMED__1": [1.5, 2.5],
                "col3": ["hello", "world"],
                "__UNNAMED__3": [-5.0, -6.0],
                "col5": ["a", "b"],
            }
        ),
    )


def test_sheets_with_header_line_diff_from_zero():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-changing-header-location.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1", "Sheet2", "Sheet3"]
    sheet_by_name = excel_reader.load_sheet("Sheet1", header_row=1)
    sheet_by_idx = excel_reader.load_sheet(0, header_row=1)

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "Sheet1"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 2

    expected = {"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}

    pd_expected = pd.DataFrame(expected)
    pd_assert_frame_equal(sheet_by_name.to_pandas(), pd_expected)
    pd_assert_frame_equal(sheet_by_idx.to_pandas(), pd_expected)

    pl_expected = pl.DataFrame(expected)
    pl_assert_frame_equal(sheet_by_name.to_polars(), pl_expected)
    pl_assert_frame_equal(sheet_by_idx.to_polars(), pl_expected)


def test_sheets_with_no_header():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-changing-header-location.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1", "Sheet2", "Sheet3"]
    sheet_by_name = excel_reader.load_sheet("Sheet2", header_row=None)
    sheet_by_idx = excel_reader.load_sheet(1, header_row=None)

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "Sheet2"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 3

    expected = {
        "__UNNAMED__0": [1.0, 2.0],
        "__UNNAMED__1": [3.0, 4.0],
        "__UNNAMED__2": [5.0, 6.0],
    }

    pd_expected = pd.DataFrame(expected)
    pd_assert_frame_equal(sheet_by_name.to_pandas(), pd_expected)
    pd_assert_frame_equal(sheet_by_idx.to_pandas(), pd_expected)

    pl_expected = pl.DataFrame(expected)
    pl_assert_frame_equal(sheet_by_name.to_polars(), pl_expected)
    pl_assert_frame_equal(sheet_by_idx.to_polars(), pl_expected)


def test_sheets_with_empty_rows_before_header():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-changing-header-location.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1", "Sheet2", "Sheet3"]
    sheet_by_name = excel_reader.load_sheet("Sheet3")
    sheet_by_idx = excel_reader.load_sheet(2)

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "Sheet3"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 2

    expected = {"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}

    pd_expected = pd.DataFrame(expected)
    pd_assert_frame_equal(sheet_by_name.to_pandas(), pd_expected)
    pd_assert_frame_equal(sheet_by_idx.to_pandas(), pd_expected)

    pl_expected = pl.DataFrame(expected)
    pl_assert_frame_equal(sheet_by_name.to_polars(), pl_expected)
    pl_assert_frame_equal(sheet_by_idx.to_polars(), pl_expected)


def test_sheets_with_custom_headers():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-changing-header-location.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1", "Sheet2", "Sheet3"]
    sheet_by_name = excel_reader.load_sheet(
        "Sheet2", header_row=None, column_names=["foo", "bar", "baz"]
    )
    sheet_by_idx = excel_reader.load_sheet(
        1, header_row=None, column_names=["foo", "bar", "baz"]
    )

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "Sheet2"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 3

    expected = {"foo": [1.0, 2.0], "bar": [3.0, 4.0], "baz": [5.0, 6.0]}

    pd_expected = pd.DataFrame(expected)
    pd_assert_frame_equal(sheet_by_name.to_pandas(), pd_expected)
    pd_assert_frame_equal(sheet_by_idx.to_pandas(), pd_expected)

    pl_expected = pl.DataFrame(expected)
    pl_assert_frame_equal(sheet_by_name.to_polars(), pl_expected)
    pl_assert_frame_equal(sheet_by_idx.to_polars(), pl_expected)


def test_sheets_with_skipping_headers():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-changing-header-location.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1", "Sheet2", "Sheet3"]
    sheet_by_name = excel_reader.load_sheet(
        "Sheet2", header_row=1, column_names=["Bugs"]
    )
    sheet_by_idx = excel_reader.load_sheet(1, header_row=1, column_names=["Bugs"])

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "Sheet2"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 3

    expected = {
        "Bugs": [1.0, 2.0],
        "__UNNAMED__1": [3.0, 4.0],
        "__UNNAMED__2": [5.0, 6.0],
    }

    pd_expected = pd.DataFrame(expected)
    pd_assert_frame_equal(sheet_by_name.to_pandas(), pd_expected)
    pd_assert_frame_equal(sheet_by_idx.to_pandas(), pd_expected)

    pl_expected = pl.DataFrame(expected)
    pl_assert_frame_equal(sheet_by_name.to_polars(), pl_expected)
    pl_assert_frame_equal(sheet_by_idx.to_polars(), pl_expected)


def test_sheet_with_pagination():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    sheet = excel_reader.load_sheet(0, skip_rows=1, n_rows=1)
    assert sheet.name == "Sheet1"
    assert sheet.height == 1
    assert sheet.total_height == 3
    assert sheet.width == 4

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "__UNNAMED__0": [1.0],
                "bools": [False],
                "dates": [pd.Timestamp("2022-03-02 05:43:04")],
                "floats": [42.69],
            }
        ),
    )

    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "__UNNAMED__0": [1.0],
                "bools": [False],
                "dates": ["2022-03-02 05:43:04"],
                "floats": [42.69],
            }
        ).with_columns(
            pl.col("dates").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )


def test_sheet_with_skip_rows():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    sheet = excel_reader.load_sheet(0, skip_rows=1)
    assert sheet.name == "Sheet1"
    assert sheet.height == 2
    assert sheet.width == 4

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "__UNNAMED__0": [1.0, 2.0],
                "bools": [False, True],
                "dates": [pd.Timestamp("2022-03-02 05:43:04")] * 2,
                "floats": [42.69, 1234567],
            }
        ),
    )

    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "__UNNAMED__0": [1.0, 2.0],
                "bools": [False, True],
                "dates": ["2022-03-02 05:43:04"] * 2,
                "floats": [42.69, 1234567],
            }
        ).with_columns(
            pl.col("dates").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )


def test_sheet_with_n_rows():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    sheet = excel_reader.load_sheet(0, n_rows=1)
    assert sheet.name == "Sheet1"
    assert sheet.height == 1
    assert sheet.width == 4

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "__UNNAMED__0": [0.0],
                "bools": [True],
                "dates": [pd.Timestamp("2022-03-02 05:43:04")],
                "floats": [12.35],
            }
        ),
    )

    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "__UNNAMED__0": [0.0],
                "bools": [True],
                "dates": ["2022-03-02 05:43:04"],
                "floats": [12.35],
            }
        ).with_columns(
            pl.col("dates").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )


def test_sheet_with_pagination_and_without_headers():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    sheet = excel_reader.load_sheet(
        0,
        n_rows=1,
        skip_rows=1,
        header_row=None,
        column_names=["This", "Is", "Amazing", "Stuff"],
    )
    assert sheet.name == "Sheet1"
    assert sheet.height == 1
    assert sheet.width == 4

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "This": [0.0],
                "Is": [True],
                "Amazing": [pd.Timestamp("2022-03-02 05:43:04")],
                "Stuff": [12.35],
            }
        ),
    )

    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "This": [0.0],
                "Is": [True],
                "Amazing": ["2022-03-02 05:43:04"],
                "Stuff": [12.35],
            }
        ).with_columns(
            pl.col("Amazing").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )


def test_sheet_with_pagination_out_of_bound():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    with pytest.raises(RuntimeError, match="To many rows skipped. Max height is 4"):
        excel_reader.load_sheet(
            0,
            skip_rows=1000000,
            header_row=None,
            column_names=["This", "Is", "Amazing", "Stuff"],
        )

    sheet = excel_reader.load_sheet(
        0,
        n_rows=1000000,
        skip_rows=1,
        header_row=None,
        column_names=["This", "Is", "Amazing", "Stuff"],
    )
    assert sheet.name == "Sheet1"
    assert sheet.height == 3
    assert sheet.width == 4

    pd_assert_frame_equal(
        sheet.to_pandas(),
        pd.DataFrame(
            {
                "This": [0.0, 1.0, 2.0],
                "Is": [True, False, True],
                "Amazing": [pd.Timestamp("2022-03-02 05:43:04")] * 3,
                "Stuff": [12.35, 42.69, 1234567],
            }
        ),
    )

    pl_assert_frame_equal(
        sheet.to_polars(),
        pl.DataFrame(
            {
                "This": [0.0, 1.0, 2.0],
                "Is": [True, False, True],
                "Amazing": ["2022-03-02 05:43:04"] * 3,
                "Stuff": [12.35, 42.69, 1234567],
            }
        ).with_columns(
            pl.col("Amazing").str.strptime(pl.Datetime, "%F %T").dt.cast_time_unit("ms")
        ),
    )
