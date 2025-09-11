# ruff: noqa: E501
from __future__ import annotations

import re
from typing import Any

import fastexcel
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from .utils import path_for_fixture


@pytest.fixture
def excel_reader_single_sheet() -> fastexcel.ExcelReader:
    return fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))


@pytest.fixture
def expected_column_info() -> list[fastexcel.ColumnInfo]:
    return [
        fastexcel.ColumnInfo(
            name="Month", index=0, column_name_from="looked_up", dtype="float", dtype_from="guessed"
        ),
        fastexcel.ColumnInfo(
            name="Year", index=1, column_name_from="looked_up", dtype="float", dtype_from="guessed"
        ),
    ]


def test_single_sheet_all_columns(
    excel_reader_single_sheet: fastexcel.ExcelReader,
    expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    sheet = excel_reader_single_sheet.load_sheet(0)

    sheet_explicit_arg = excel_reader_single_sheet.load_sheet(0, use_columns=None)
    assert sheet.selected_columns == expected_column_info
    assert sheet.available_columns() == expected_column_info

    expected = {"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}
    expected_pd_df = pd.DataFrame(expected)
    expected_pl_df = pl.DataFrame(expected)

    pd_df = sheet.to_pandas()
    pd_assert_frame_equal(pd_df, expected_pd_df)
    pd_df_explicit_arg = sheet_explicit_arg.to_pandas()
    pd_assert_frame_equal(pd_df_explicit_arg, expected_pd_df)

    pl_df = sheet.to_polars()
    pl_assert_frame_equal(pl_df, expected_pl_df)
    pl_df_explicit_arg = sheet_explicit_arg.to_polars()
    pl_assert_frame_equal(pl_df_explicit_arg, expected_pl_df)


def test_single_sheet_subset_by_str(
    excel_reader_single_sheet: fastexcel.ExcelReader,
    expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    expected = {"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}

    # looks like mypy 1.8 became more stupid
    sheets: list[str | int] = [0, "January"]
    for sheet_name_or_idx in sheets:
        for idx, col in enumerate(["Month", "Year"]):
            sheet = excel_reader_single_sheet.load_sheet(sheet_name_or_idx, use_columns=[col])
            assert sheet.selected_columns == [expected_column_info[idx]]
            assert sheet.available_columns() == expected_column_info

            pd_df = sheet.to_pandas()
            pd_assert_frame_equal(pd_df, pd.DataFrame({col: expected[col]}))

            pl_df = sheet.to_polars()
            pl_assert_frame_equal(pl_df, pl.DataFrame({col: expected[col]}))


def test_single_sheet_subset_by_index(
    excel_reader_single_sheet: fastexcel.ExcelReader,
    expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    expected = {"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}

    sheets: list[str | int] = [0, "January"]
    for sheet_name_or_idx in sheets:
        for idx, col_name in enumerate(["Month", "Year"]):
            sheet = excel_reader_single_sheet.load_sheet(sheet_name_or_idx, use_columns=[idx])
            assert sheet.selected_columns == [expected_column_info[idx]]
            assert sheet.available_columns() == expected_column_info

            pd_df = sheet.to_pandas()
            pd_assert_frame_equal(pd_df, pd.DataFrame({col_name: expected[col_name]}))

            pl_df = sheet.to_polars()
            pl_assert_frame_equal(pl_df, pl.DataFrame({col_name: expected[col_name]}))


@pytest.fixture
def excel_reader_single_sheet_with_unnamed_columns() -> fastexcel.ExcelReader:
    return fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))


@pytest.fixture
def single_sheet_with_unnamed_columns_expected() -> dict[str, list[Any]]:
    return {
        "col1": [2.0, 3.0],
        "__UNNAMED__1": [1.5, 2.5],
        "col3": ["hello", "world"],
        "__UNNAMED__3": [-5.0, -6.0],
        "col5": ["a", "b"],
    }


@pytest.fixture
def sheet_with_unnamed_columns_expected_column_info() -> list[fastexcel.ColumnInfo]:
    return [
        fastexcel.ColumnInfo(
            name="col1", index=0, column_name_from="looked_up", dtype="float", dtype_from="guessed"
        ),
        fastexcel.ColumnInfo(
            name="__UNNAMED__1",
            index=1,
            column_name_from="generated",
            dtype="float",
            dtype_from="guessed",
        ),
        fastexcel.ColumnInfo(
            name="col3", index=2, column_name_from="looked_up", dtype="string", dtype_from="guessed"
        ),
        fastexcel.ColumnInfo(
            name="__UNNAMED__3",
            index=3,
            column_name_from="generated",
            dtype="float",
            dtype_from="guessed",
        ),
        fastexcel.ColumnInfo(
            name="col5", index=4, column_name_from="looked_up", dtype="string", dtype_from="guessed"
        ),
    ]


def test_single_sheet_with_unnamed_columns(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    use_columns_str = ["col1", "col3", "__UNNAMED__3"]
    use_columns_idx = [0, 2, 3]
    expected = {
        k: v for k, v in single_sheet_with_unnamed_columns_expected.items() if k in use_columns_str
    }

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    assert sheet.selected_columns == [
        sheet_with_unnamed_columns_expected_column_info[0],
        sheet_with_unnamed_columns_expected_column_info[2],
        sheet_with_unnamed_columns_expected_column_info[3],
    ]
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_idx
    )
    assert sheet.selected_columns == [
        sheet_with_unnamed_columns_expected_column_info[0],
        sheet_with_unnamed_columns_expected_column_info[2],
        sheet_with_unnamed_columns_expected_column_info[3],
    ]
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_pagination(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    use_columns_str = ["col1", "col3", "__UNNAMED__3"]
    use_columns_idx = [0, 2, 3]

    # first row only
    expected = {
        k: v[:1]
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in use_columns_str
    }

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str, n_rows=1
    )
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_idx, n_rows=1
    )
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))

    # second row
    expected = {
        k: v[1:]
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in use_columns_str
    }

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str, skip_rows=1
    )
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_idx, skip_rows=1
    )
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_pagination_and_column_names(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
) -> None:
    use_columns_str = ["col0", "col2", "col3"]
    use_columns_idx = [0, 2, 3]
    expected: dict[str, list[Any]] = {
        "col0": [2.0, 3.0],
        "col1": ["hello", "world"],
        "col2": [-5.0, -6.0],
    }
    column_names = [f"col{i}" for i in range(3)]
    expected_columns_names = ["col0", "__UNNAMED__1", "col1", "col2", "__UNNAMED__4"]

    # skipping the header row only
    with pytest.raises(
        fastexcel.InvalidParametersError,
        match='use_columns can only contain integers when used with columns_names, got "col0"',
    ):
        excel_reader_single_sheet_with_unnamed_columns.load_sheet(
            "With unnamed columns",
            use_columns=use_columns_str,
            skip_rows=1,
            column_names=column_names,
        )

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_idx, skip_rows=1, column_names=column_names
    )
    assert [col.name for col in sheet.available_columns()] == expected_columns_names

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))

    # skipping the header row + first data row
    expected_first_row_skipped = {k: v[1:] for k, v in expected.items()}

    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_idx, skip_rows=2, column_names=column_names
    )
    assert [col.name for col in sheet.available_columns()] == expected_columns_names

    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected_first_row_skipped))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected_first_row_skipped))


def test_single_sheet_with_unnamed_columns_and_str_range(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    use_columns_str = "A,C:E"
    expected = {
        k: v
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in ["col1", "col3", "__UNNAMED__3", "col5"]
    }
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    assert sheet.selected_columns == (
        sheet_with_unnamed_columns_expected_column_info[:1]
        + sheet_with_unnamed_columns_expected_column_info[2:]
    )
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_open_ended_range(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    # Test B: (should get columns B, C, D, E - indices 1, 2, 3, 4)
    use_columns_str = "B:"
    expected = {
        k: v
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in ["__UNNAMED__1", "col3", "__UNNAMED__3", "col5"]
    }
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    assert sheet.selected_columns == sheet_with_unnamed_columns_expected_column_info[1:]
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_open_ended_range_from_start(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    # Test A: (should get all columns)
    use_columns_str = "A:"
    expected = single_sheet_with_unnamed_columns_expected
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    assert sheet.selected_columns == sheet_with_unnamed_columns_expected_column_info
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_mixed_open_ended_range(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    # Test A,C: (should get column A and columns from C onwards - indices 0, 2, 3, 4)
    use_columns_str = "A,C:"
    expected = {
        k: v
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in ["col1", "col3", "__UNNAMED__3", "col5"]
    }
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    expected_selected_cols = [
        sheet_with_unnamed_columns_expected_column_info[0]
    ] + sheet_with_unnamed_columns_expected_column_info[2:]
    assert sheet.selected_columns == expected_selected_cols
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_from_beginning_range(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    # Test :C (should get columns A, B, C - indices 0, 1, 2)
    use_columns_str = ":C"
    expected = {
        k: v
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in ["col1", "__UNNAMED__1", "col3"]
    }
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    assert sheet.selected_columns == sheet_with_unnamed_columns_expected_column_info[:3]
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_from_beginning_range_single_column(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    # Test :A (should get only column A - index 0)
    use_columns_str = ":A"
    expected = {
        k: v for k, v in single_sheet_with_unnamed_columns_expected.items() if k in ["col1"]
    }
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    assert sheet.selected_columns == [sheet_with_unnamed_columns_expected_column_info[0]]
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_with_unnamed_columns_and_complex_mixed_pattern(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
    single_sheet_with_unnamed_columns_expected: dict[str, list[Any]],
    sheet_with_unnamed_columns_expected_column_info: list[fastexcel.ColumnInfo],
) -> None:
    # Test A,:B,D,E: (should get A, A,B again (deduplicated), D, and E)
    # This effectively becomes A,B,D,E (columns 0,1,3,4)
    use_columns_str = "A,:B,D,E:"
    expected = {
        k: v
        for k, v in single_sheet_with_unnamed_columns_expected.items()
        if k in ["col1", "__UNNAMED__1", "__UNNAMED__3", "col5"]
    }
    sheet = excel_reader_single_sheet_with_unnamed_columns.load_sheet(
        "With unnamed columns", use_columns=use_columns_str
    )
    # Expected: columns A, A,B (from :B), D, E (from E:)
    # After deduplication: 0,1,3,4
    expected_selected_cols = [
        sheet_with_unnamed_columns_expected_column_info[0],  # A
        sheet_with_unnamed_columns_expected_column_info[1],  # B
        sheet_with_unnamed_columns_expected_column_info[3],  # D
        sheet_with_unnamed_columns_expected_column_info[4],  # E
    ]
    assert sheet.selected_columns == expected_selected_cols
    assert sheet.available_columns() == sheet_with_unnamed_columns_expected_column_info
    pd_assert_frame_equal(sheet.to_pandas(), pd.DataFrame(expected))
    pl_assert_frame_equal(sheet.to_polars(), pl.DataFrame(expected))


def test_single_sheet_invalid_column_indices_negative_integer(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
) -> None:
    expected_message = """invalid parameters: expected list[int] | list[str], got [-2]
Context:
    0: could not determine selected columns from provided object: [-2]
    1: expected selected columns to be list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None, got Some([-2])
"""
    with pytest.raises(fastexcel.InvalidParametersError, match=re.escape(expected_message)):
        excel_reader_single_sheet_with_unnamed_columns.load_sheet(0, use_columns=[-2])


def test_single_sheet_invalid_column_indices_empty_list(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
) -> None:
    expected_message = """invalid parameters: list of selected columns is empty
Context:
    0: could not determine selected columns from provided object: []
    1: expected selected columns to be list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None, got Some([])
"""
    with pytest.raises(fastexcel.InvalidParametersError, match=re.escape(expected_message)):
        excel_reader_single_sheet_with_unnamed_columns.load_sheet(0, use_columns=[])


def test_single_sheet_invalid_column_indices_column_does_not_exist_str(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
) -> None:
    expected_message = """column with name \"nope\" not found
Context:
    0: available columns are: .*
"""
    with pytest.raises(fastexcel.ColumnNotFoundError, match=expected_message):
        excel_reader_single_sheet_with_unnamed_columns.load_sheet(0, use_columns=["nope"])


def test_single_sheet_invalid_column_indices_column_does_not_exist_int(
    excel_reader_single_sheet_with_unnamed_columns: fastexcel.ExcelReader,
) -> None:
    expected_message = """column at index 42 not found
Context:
    0: available columns are: .*
"""
    with pytest.raises(fastexcel.ColumnNotFoundError, match=expected_message):
        excel_reader_single_sheet_with_unnamed_columns.load_sheet(0, use_columns=[42])


def test_use_columns_with_column_names() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet-with-types.xlsx"))

    sheet = excel_reader.load_sheet(
        0,
        use_columns=[1, 2],
        header_row=None,
        skip_rows=1,
        column_names=["bools_renamed", "dates_renamed"],
    )

    assert sheet.available_columns() == [
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


def test_use_columns_with_callable() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))

    sheet = excel_reader.load_sheet(2)
    assert (
        [(c.name, c.dtype) for c in sheet.available_columns()]
        == [(c.name, c.dtype) for c in sheet.selected_columns]
        == [
            ("col1", "float"),
            ("__UNNAMED__1", "float"),
            ("col3", "string"),
            ("__UNNAMED__3", "float"),
            ("col5", "string"),
        ]
    )

    sheet = excel_reader.load_sheet(
        2,
        use_columns=lambda col: col.name.startswith("col"),
    )
    assert [(c.name, c.dtype) for c in sheet.selected_columns] == [
        ("col1", "float"),
        ("col3", "string"),
        ("col5", "string"),
    ]

    sheet = excel_reader.load_sheet(
        2,
        use_columns=lambda col: col.index % 2 == 1,
    )
    assert [(c.name, c.dtype) for c in sheet.selected_columns] == [
        ("__UNNAMED__1", "float"),
        ("__UNNAMED__3", "float"),
    ]


def test_use_columns_with_bad_callable() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))
    with pytest.raises(
        fastexcel.InvalidParametersError,
        match=re.escape("`use_columns` callable could not be called (TypeError: "),
    ):
        excel_reader.load_sheet(
            2,
            use_columns=lambda: True,  # type: ignore
        )

    with pytest.raises(
        fastexcel.InvalidParametersError, match="`use_columns` callable should return a boolean"
    ):
        excel_reader.load_sheet(
            2,
            use_columns=lambda _: 42,  # type: ignore
        )


def test_use_columns_with_eager_loading() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    expected_months = [1.0, 2.0]
    expected_years = [2019.0, 2020.0]

    # default
    rb = excel_reader.load_sheet_eager(0)
    assert rb.schema.names == ["Month", "Year"]
    assert rb["Year"].tolist() == expected_years
    assert rb["Month"].tolist() == expected_months

    # changing order
    rb = excel_reader.load_sheet_eager(0, use_columns=["Year", "Month"])
    assert rb.schema.names == ["Year", "Month"]
    assert rb["Year"].tolist() == expected_years
    assert rb["Month"].tolist() == expected_months

    # subset
    rb = excel_reader.load_sheet_eager(0, use_columns=["Year"])
    assert rb.schema.names == ["Year"]
    assert rb["Year"].tolist() == expected_years
    assert "Month" not in (field.name for field in rb.schema)


@pytest.mark.parametrize("excel_file", ["sheet-null-strings.xlsx", "sheet-null-strings-empty.xlsx"])
def test_use_columns_dtypes_eager_loading(
    excel_file: str, expected_data_sheet_null_strings: dict[str, list[Any]]
) -> None:
    expected_pl_df = pl.DataFrame(expected_data_sheet_null_strings).with_columns(
        pl.col("DATES_AND_NULLS").dt.cast_time_unit("ms"),
        pl.col("TIMESTAMPS_AND_NULLS").dt.cast_time_unit("ms"),
    )
    expected_pd_df = pd.DataFrame(expected_data_sheet_null_strings)
    expected_pd_df["DATES_AND_NULLS"] = expected_pd_df["DATES_AND_NULLS"].dt.as_unit("ms")
    expected_pd_df["TIMESTAMPS_AND_NULLS"] = expected_pd_df["TIMESTAMPS_AND_NULLS"].dt.as_unit("ms")

    for use_columns in (
        list(expected_data_sheet_null_strings.keys()),
        [key for idx, key in enumerate(expected_data_sheet_null_strings.keys()) if idx % 2],
        [key for idx, key in enumerate(expected_data_sheet_null_strings.keys()) if idx % 2 == 0],
        list(reversed(expected_data_sheet_null_strings.keys())),
        [
            key
            for idx, key in enumerate(reversed(expected_data_sheet_null_strings.keys()))
            if idx % 2
        ],
        [
            key
            for idx, key in enumerate(reversed(expected_data_sheet_null_strings.keys()))
            if idx % 2 == 0
        ],
    ):
        excel_reader = fastexcel.read_excel(path_for_fixture(excel_file))
        sheet = excel_reader.load_sheet_eager(0, use_columns=use_columns)
        pd_df = sheet.to_pandas()
        pl_df = pl.from_arrow(data=sheet)
        assert isinstance(pl_df, pl.DataFrame)
        sheet_lazy = excel_reader.load_sheet(0, use_columns=use_columns)
        pl_df_lazy = sheet_lazy.to_polars()
        pd_df_lazy = sheet_lazy.to_pandas()

        pl_assert_frame_equal(pl_df_lazy, pl_df)
        pd_assert_frame_equal(pd_df_lazy, pd_df)

        pl_assert_frame_equal(expected_pl_df.select(use_columns), pl_df)
        pd_assert_frame_equal(expected_pd_df[use_columns], pd_df)

        assert pd_df.columns.to_list() == use_columns
        assert pl_df.columns == use_columns
