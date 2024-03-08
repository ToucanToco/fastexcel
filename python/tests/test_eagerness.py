from datetime import date, datetime, timedelta

import fastexcel
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
from pyarrow import RecordBatch
from utils import path_for_fixture


def test_load_sheet_eager_single_sheet() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))

    eager_pandas = excel_reader.load_sheet_eager(0).to_pandas()
    lazy_pandas = excel_reader.load_sheet(0).to_pandas()
    pd_assert_frame_equal(eager_pandas, lazy_pandas)

    eager_polars = pl.from_arrow(data=excel_reader.load_sheet_eager(0))
    assert isinstance(eager_polars, pl.DataFrame)
    lazy_polars = excel_reader.load_sheet(0).to_polars()
    pl_assert_frame_equal(eager_polars, lazy_polars)


def test_multiple_sheets_with_unnamed_columns():
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))

    eager_pandas = excel_reader.load_sheet_eager("With unnamed columns").to_pandas()
    lazy_pandas = excel_reader.load_sheet("With unnamed columns").to_pandas()
    pd_assert_frame_equal(eager_pandas, lazy_pandas)

    eager_polars = pl.from_arrow(data=excel_reader.load_sheet_eager("With unnamed columns"))
    assert isinstance(eager_polars, pl.DataFrame)
    lazy_polars = excel_reader.load_sheet("With unnamed columns").to_polars()
    pl_assert_frame_equal(eager_polars, lazy_polars)


def test_eager_with_an_ods_file_should_return_a_recordbatch() -> None:
    ods_reader = fastexcel.read_excel(path_for_fixture("dates.ods"))

    record_batch = ods_reader.load_sheet_eager(0)
    assert isinstance(record_batch, RecordBatch)
    pl_df = pl.from_arrow(record_batch)
    assert isinstance(pl_df, pl.DataFrame)
    pl_assert_frame_equal(
        pl_df,
        pl.DataFrame(
            {
                "date": [date(2023, 6, 1)],
                "datestr": ["2023-06-01T02:03:04+02:00"],
                "time": [timedelta(hours=1, minutes=2, seconds=3)],
                "datetime": [datetime(2023, 6, 1, 2, 3, 4)],
            }
        ).with_columns(*(pl.col(col).dt.cast_time_unit("ms") for col in ("datetime", "time"))),
    )
