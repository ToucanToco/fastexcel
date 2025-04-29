from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

import fastexcel
import numpy as np
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from pytest_mock import MockerFixture

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
        "Mixed dates": ["2023-07-21 00:00:00"] * 6 + ["July 23rd"] * 3,
        "Mixed bools": ["true"] * 5 + ["false"] * 3 + ["other"],
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
    expected_data["Mixed dates"] = [datetime(2023, 7, 21)] * 6 + [None] * 3
    expected_data["Mixed bools"] = [True] * 5 + [False] * 3 + [None]

    pd_df = sheet.to_pandas()
    pd_assert_frame_equal(
        pd_df,
        pd.DataFrame(expected_data).astype(
            {
                "Date": "datetime64[ms]",
                "Mixed dates": "datetime64[ms]",
            }
        ),
    )

    pl_df = sheet.to_polars()
    pl_assert_frame_equal(
        pl_df,
        pl.DataFrame(
            expected_data,
            schema_overrides={
                "Date": pl.Datetime(time_unit="ms"),
                "Mixed dates": pl.Datetime(time_unit="ms"),
            },
        ),
    )


@pytest.mark.parametrize("dtype_by_index", (True, False))
@pytest.mark.parametrize(
    "dtype,expected_data,expected_pd_dtype,expected_pl_dtype",
    [
        ("int", [123456, 44333, 44333, 87878, 87878], "int64", pl.Int64),
        ("float", [123456.0, 44333.0, 44333.0, 87878.0, 87878.0], "float64", pl.Float64),
        ("string", ["123456", "44333", "44333", "87878", "87878"], "object", pl.Utf8),
        ("boolean", [True] * 5, "bool", pl.Boolean),
        (
            "datetime",
            [datetime(2238, 1, 3)] + [datetime(2021, 5, 17)] * 2 + [datetime(2140, 8, 6)] * 2,
            "datetime64[ms]",
            pl.Datetime,
        ),
        (
            "date",
            [date(2238, 1, 3)] + [date(2021, 5, 17)] * 2 + [date(2140, 8, 6)] * 2,
            "object",
            pl.Date,
        ),
        #  conversion to duration not supported yet
        ("duration", [pd.NaT] * 5, "timedelta64[ms]", pl.Duration),
    ],
)
def test_sheet_with_mixed_dtypes_specify_dtypes(
    dtype_by_index: bool,
    dtype: fastexcel.DType,
    expected_data: list[Any],
    expected_pd_dtype: str,
    expected_pl_dtype: pl.DataType,
) -> None:
    dtypes: fastexcel.DTypeMap = {0: dtype} if dtype_by_index else {"Employee ID": dtype}
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))
    sheet = excel_reader.load_sheet(0, dtypes=dtypes, n_rows=5)
    assert sheet.specified_dtypes == dtypes

    pd_df = sheet.to_pandas()
    assert pd_df["Employee ID"].dtype == expected_pd_dtype
    assert pd_df["Employee ID"].to_list() == expected_data

    pl_df = sheet.to_polars()
    assert pl_df["Employee ID"].dtype == expected_pl_dtype
    assert pl_df["Employee ID"].to_list() == (expected_data if dtype != "duration" else [None] * 5)


@pytest.mark.parametrize(
    "dtypes,expected,expected_pd_dtype,expected_pl_dtype",
    [
        (None, datetime(2023, 7, 21), "datetime64[ms]", pl.Datetime),
        ({"Date": "datetime"}, datetime(2023, 7, 21), "datetime64[ms]", pl.Datetime),
        ({"Date": "date"}, date(2023, 7, 21), "object", pl.Date),
        ({"Date": "string"}, "2023-07-21 00:00:00", "object", pl.Utf8),
        ({2: "datetime"}, datetime(2023, 7, 21), "datetime64[ms]", pl.Datetime),
        ({2: "date"}, date(2023, 7, 21), "object", pl.Date),
        ({2: "string"}, "2023-07-21 00:00:00", "object", pl.Utf8),
    ],
)
def test_sheet_datetime_conversion(
    dtypes: fastexcel.DTypeMap | None,
    expected: Any,
    expected_pd_dtype: str,
    expected_pl_dtype: pl.DataType,
) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))

    sheet = excel_reader.load_sheet(0, dtypes=dtypes)
    assert sheet.specified_dtypes == dtypes
    pd_df = sheet.to_pandas()
    assert pd_df["Date"].dtype == expected_pd_dtype
    assert pd_df["Date"].to_list() == [expected] * 9

    pl_df = sheet.to_polars()
    assert pl_df["Date"].dtype == expected_pl_dtype
    assert pl_df["Date"].to_list() == [expected] * 9


@pytest.mark.parametrize("eager", [True, False])
@pytest.mark.parametrize("dtype_coercion", ["coerce", None])
def test_dtype_coercion_behavior__coerce(
    dtype_coercion: Literal["coerce"] | None, eager: bool
) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))

    kwargs = {"dtype_coercion": dtype_coercion} if dtype_coercion else {}
    sheet = (
        excel_reader.load_sheet_eager(0, **kwargs)  # type:ignore[arg-type]
        if eager
        else excel_reader.load_sheet(0, **kwargs).to_arrow()  # type:ignore[arg-type]
    )

    pd_df = sheet.to_pandas()
    assert pd_df["Mixed dates"].dtype == "object"
    assert pd_df["Mixed dates"].to_list() == ["2023-07-21 00:00:00"] * 6 + ["July 23rd"] * 3

    pl_df = pl.from_arrow(data=sheet)
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df["Mixed dates"].dtype == pl.Utf8
    assert pl_df["Mixed dates"].to_list() == ["2023-07-21 00:00:00"] * 6 + ["July 23rd"] * 3


@pytest.mark.parametrize("eager", [True, False])
def test_dtype_coercion_behavior__strict_sampling_eveything(eager: bool) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))

    with pytest.raises(
        fastexcel.UnsupportedColumnTypeCombinationError, match="type coercion is strict"
    ):
        if eager:
            excel_reader.load_sheet_eager(0, dtype_coercion="strict")
        else:
            excel_reader.load_sheet(0, dtype_coercion="strict").to_arrow()


@pytest.mark.parametrize("eager", [True, False])
def test_dtype_coercion_behavior__strict_sampling_limit(eager: bool) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))

    sheet = (
        excel_reader.load_sheet_eager(0, dtype_coercion="strict", schema_sample_rows=5)
        if eager
        else excel_reader.load_sheet(0, dtype_coercion="strict", schema_sample_rows=5).to_arrow()
    )

    pd_df = sheet.to_pandas()
    assert pd_df["Mixed dates"].dtype == "datetime64[ms]"
    assert (
        pd_df["Mixed dates"].to_list() == [pd.Timestamp("2023-07-21 00:00:00")] * 6 + [pd.NaT] * 3
    )
    assert pd_df["Asset ID"].dtype == "float64"
    assert pd_df["Asset ID"].replace(np.nan, None).to_list() == [84444.0] * 7 + [None] * 2

    pl_df = pl.from_arrow(data=sheet)
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df["Mixed dates"].dtype == pl.Datetime
    assert pl_df["Mixed dates"].to_list() == [datetime(2023, 7, 21)] * 6 + [None] * 3
    assert pl_df["Asset ID"].dtype == pl.Float64
    assert pl_df["Asset ID"].to_list() == [84444.0] * 7 + [None] * 2


def test_one_dtype_for_all() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-dtypes-columns.xlsx"))
    sheet = excel_reader.load_sheet(0, dtypes="string")
    assert sheet.available_columns() == [
        fastexcel.ColumnInfo(
            name="Employee ID",
            index=0,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Employee Name",
            index=1,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Date",
            index=2,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Details",
            index=3,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Asset ID",
            index=4,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Mixed dates",
            index=5,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Mixed bools",
            index=6,
            dtype="string",
            dtype_from="provided_for_all",
            column_name_from="looked_up",
        ),
    ]
    assert sheet.to_polars().dtypes == [pl.String] * 7


def test_fallback_infer_dtypes(mocker: MockerFixture) -> None:
    """it should fallback to string if it can't infer the dtype"""
    import logging

    logger_instance_mock = mocker.patch("logging.getLogger", autospec=True).return_value

    excel_reader = fastexcel.read_excel(path_for_fixture("infer-dtypes-fallback.xlsx"))
    sheet = excel_reader.load_sheet(0)

    # Ensure a warning message was logged to explain the fallback to string
    logger_instance_mock.makeRecord.assert_called_once_with(
        "fastexcel.types.dtype",
        logging.WARNING,
        mocker.ANY,
        mocker.ANY,
        "Could not determine dtype for column 1, falling back to string",
        mocker.ANY,
        mocker.ANY,
    )

    assert sheet.available_columns() == [
        fastexcel.ColumnInfo(
            name="id",
            index=0,
            dtype="float",
            dtype_from="guessed",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="label",
            index=1,
            dtype="string",
            dtype_from="guessed",
            column_name_from="looked_up",
        ),
    ]
    assert sheet.to_polars().dtypes == [pl.Float64, pl.String]


@pytest.mark.parametrize(
    ("dtype", "expected_data"),
    [
        (
            "int",
            [None] * 2
            + [-1.0, 0.0, 1.0, 0.0, 1.0, 1.0, -1.0, 0.0, 1.0, None, 1.0, 0.0]
            + [None] * 7
            + [0.0],
        ),
        (
            "float",
            [None] * 2
            + [-1.0, 0.0, 1.0, 0.0, 1.0, 1.1, -1.0, 0.0, 1.0, 1.1, 1.0, 0.0]
            + [None] * 7
            + [0.1],
        ),
        (
            "string",
            [
                None,
                "foo",
                "-1",
                "0",
                "1",
                "0",
                "1",
                "1.1",
                "-1",
                "0",
                "1",
                "1.1",
                "true",
                "false",
                "2023-07-21 00:00:00",
                "2023-07-21 12:20:00",
                # calamine reads a time as datetimes here, which seems wrong
                "1899-12-31 12:20:00",
                "07/21/2023",
                "7/21/2023  12:20:00 PM",
                "July 23rd",
                "12:20:00",
                "0.1",
            ],
        ),
        (
            "boolean",
            [None] * 2
            + [True, False, True, False, True, True]
            + [None] * 4
            + [True, False]
            + [None] * 7
            + [True],
        ),
        (
            "datetime",
            [pd.NaT] * 2
            + [
                pd.Timestamp("1899-12-30 00:00:00"),
                pd.Timestamp("1899-12-31 00:00:00"),
                pd.Timestamp("1900-01-01 00:00:00"),
                pd.Timestamp("1899-12-31 00:00:00"),
                pd.Timestamp("1900-01-01 00:00:00"),
                pd.Timestamp("1900-01-01 02:24:00"),
            ]
            + [pd.NaT] * 6
            + [
                pd.Timestamp("2023-7-21 00:00:00"),
                pd.Timestamp("2023-7-21 12:20:00"),
                # calamine currently adds a date to a time, which is
                # questionable
                pd.Timestamp("1899-12-31 12:20:00"),
            ]
            + [pd.NaT] * 4
            + [
                # calamine converts percentages to datetimes (since it does not
                # distinguish from floats), which seems questionable
                pd.Timestamp("1899-12-31 02:24:00")
            ],
        ),
        (
            "date",
            [None] * 2
            + [
                pd.Timestamp("1899-12-30").date(),
                pd.Timestamp("1899-12-31").date(),
                pd.Timestamp("1900-01-01").date(),
                pd.Timestamp("1899-12-31").date(),
                pd.Timestamp("1900-01-01").date(),
                pd.Timestamp("1900-01-01").date(),
            ]
            + [None] * 6
            + [
                pd.Timestamp("2023-7-21").date(),
                pd.Timestamp("2023-7-21").date(),
                # calamine converts any time to 1899-12-31, which is
                # questionable
                pd.Timestamp("1899-12-31").date(),
            ]
            + [None] * 4
            + [
                # calamine converts percentages to dates (since it does not
                # distinguish from floats), which seems questionable
                pd.Timestamp("1899-12-31").date()
            ],
        ),
        (
            "duration",
            [pd.NaT] * 14
            + [
                # dates/datetimes are converted to durations, which seems
                # questionable
                pd.Timedelta(datetime(2023, 7, 21 + 1) - datetime(1899, 12, 31)),
                pd.Timedelta(datetime(2023, 7, 21 + 1, 12, 20, 0) - datetime(1899, 12, 31)),
                pd.Timedelta(hours=12, minutes=20),
            ]
            + [pd.NaT] * 5,
        ),
    ],
)
def test_to_arrow_with_errors(
    dtype: fastexcel.DType,
    expected_data: list[Any],
):
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-type-errors.xlsx"))
    rb, cell_errors = excel_reader.load_sheet(0, dtypes={"Column": dtype}).to_arrow_with_errors()

    pd_df = rb.to_pandas()
    assert pd_df["Column"].replace(np.nan, None).to_list() == expected_data

    def item_to_polars(item: Any):
        if isinstance(item, pd.Timestamp):
            return item.to_pydatetime()
        if pd.isna(item):
            return None
        return item

    pl_df = pl.from_arrow(rb)
    assert isinstance(pl_df, pl.DataFrame)
    pl_expected_data = list(map(item_to_polars, expected_data))
    assert pl_df["Column"].to_list() == pl_expected_data

    # the only empty cell is (0, 0), so all other cells that were read as None
    # should be errors
    expected_error_positions = [
        (i, 0) for i in range(1, len(expected_data)) if expected_data[i] in {None, pd.NaT}
    ]
    if expected_error_positions:
        assert cell_errors is not None
        error_positions = [err.offset_position for err in cell_errors.errors]
        assert error_positions == expected_error_positions
