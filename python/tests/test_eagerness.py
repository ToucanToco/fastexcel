import fastexcel
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal
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
