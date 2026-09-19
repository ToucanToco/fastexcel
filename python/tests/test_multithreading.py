"""Tests for multithreading behavior with free-threaded Python builds.

These tests verify that multiple threads can safely read different sheets and tables
from the same ExcelReader instance concurrently, and that conversions can be called
concurrently on loaded sheets/tables.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import fastexcel
import pandas as pd
import polars as pl
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from .utils import path_for_fixture

# Fixtures for expected data


@pytest.fixture
def expected_multi_sheet_pl() -> dict[str, pl.DataFrame]:
    """Expected polars DataFrames for multi-sheet fixture."""
    return {
        "January": pl.DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}),
        "February": pl.DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}),
        "With unnamed columns": pl.DataFrame(
            {
                "foo": [1.0, 4.0],
                "__UNNAMED__1": [2.0, 5.0],
                "bar": [3.0, 6.0],
            }
        ),
    }


@pytest.fixture
def expected_multi_sheet_pd() -> dict[str, pd.DataFrame]:
    """Expected pandas DataFrames for multi-sheet fixture."""
    return {
        "January": pd.DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}),
        "February": pd.DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]}),
        "With unnamed columns": pd.DataFrame(
            {
                "foo": [1.0, 4.0],
                "__UNNAMED__1": [2.0, 5.0],
                "bar": [3.0, 6.0],
            }
        ),
    }


@pytest.fixture
def expected_single_sheet_pl() -> pl.DataFrame:
    """Expected polars DataFrame for single-sheet fixture."""
    return pl.DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]})


@pytest.fixture
def expected_single_sheet_pd() -> pd.DataFrame:
    """Expected pandas DataFrame for single-sheet fixture."""
    return pd.DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]})


@pytest.fixture
def expected_table_pl() -> pl.DataFrame:
    """Expected polars DataFrame for table fixture."""
    return pl.DataFrame(
        {
            "User Id": [1.0, 2.0, 5.0],
            "FirstName": ["Peter", "John", "Hans"],
            "LastName": ["Müller", "Meier", "Fricker"],
            "Date": [datetime(2020, 1, 1), datetime(2024, 5, 4), datetime(2025, 2, 1)],
        }
    ).with_columns(pl.col("Date").dt.cast_time_unit("ms"))


@pytest.fixture
def expected_table_pd() -> pd.DataFrame:
    """Expected pandas DataFrame for table fixture."""
    return pd.DataFrame(
        {
            "User Id": [1.0, 2.0, 5.0],
            "FirstName": ["Peter", "John", "Hans"],
            "LastName": ["Müller", "Meier", "Fricker"],
            "Date": pd.Series(
                [datetime(2020, 1, 1), datetime(2024, 5, 4), datetime(2025, 2, 1)]
            ).astype("datetime64[ms]"),
        }
    )


# Tests for concurrent loading of different sheets


def test_concurrent_load_different_sheets_lazy_to_arrow(
    expected_multi_sheet_pl: dict[str, pl.DataFrame],
) -> None:
    """Load different sheets concurrently (lazy) and convert to arrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))

    def load_sheet_to_arrow(sheet_name: str) -> tuple[str, pl.DataFrame]:
        sheet = excel_reader.load_sheet(sheet_name)
        arrow_data = sheet.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return sheet_name, df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(load_sheet_to_arrow, sheet_name)
            for sheet_name in excel_reader.sheet_names
            for _ in range(10)  # Load each sheet 10 times concurrently
        ]

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for sheet_name, df in results:
        pl_assert_frame_equal(df, expected_multi_sheet_pl[sheet_name])


def test_concurrent_load_different_sheets_lazy_to_pandas(
    expected_multi_sheet_pd: dict[str, pd.DataFrame],
) -> None:
    """Load different sheets concurrently (lazy) and convert to pandas."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))

    def load_sheet_to_pandas(sheet_name: str) -> tuple[str, pd.DataFrame]:
        sheet = excel_reader.load_sheet(sheet_name)
        return sheet_name, sheet.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(load_sheet_to_pandas, sheet_name)
            for sheet_name in excel_reader.sheet_names
            for _ in range(10)  # Load each sheet 10 times concurrently
        ]

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for sheet_name, df in results:
        pd_assert_frame_equal(df, expected_multi_sheet_pd[sheet_name])


def test_concurrent_load_different_sheets_eager_to_arrow(
    expected_multi_sheet_pl: dict[str, pl.DataFrame],
) -> None:
    """Load different sheets concurrently (eager) and convert to arrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))

    def load_sheet_eager_to_arrow(sheet_name: str) -> tuple[str, pl.DataFrame]:
        record_batch = excel_reader.load_sheet_eager(sheet_name)
        df = pl.from_arrow(record_batch)
        assert isinstance(df, pl.DataFrame)
        return sheet_name, df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(load_sheet_eager_to_arrow, sheet_name)
            for sheet_name in excel_reader.sheet_names
            for _ in range(10)  # Load each sheet 10 times concurrently
        ]

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for sheet_name, df in results:
        pl_assert_frame_equal(df, expected_multi_sheet_pl[sheet_name])


def test_concurrent_load_different_sheets_eager_to_pandas(
    expected_multi_sheet_pd: dict[str, pd.DataFrame],
) -> None:
    """Load different sheets concurrently (eager) and convert to pandas."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))

    def load_sheet_eager_to_pandas(sheet_name: str) -> tuple[str, pd.DataFrame]:
        record_batch = excel_reader.load_sheet_eager(sheet_name)
        return sheet_name, record_batch.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(load_sheet_eager_to_pandas, sheet_name)
            for sheet_name in excel_reader.sheet_names
            for _ in range(10)  # Load each sheet 10 times concurrently
        ]

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for sheet_name, df in results:
        pd_assert_frame_equal(df, expected_multi_sheet_pd[sheet_name])


# Tests for concurrent loading of same sheet


def test_concurrent_load_same_sheet_lazy_to_arrow(
    expected_single_sheet_pl: pl.DataFrame,
) -> None:
    """Load the same sheet concurrently (lazy) and convert to arrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))

    def load_sheet_to_arrow(thread_id: int) -> pl.DataFrame:
        sheet = excel_reader.load_sheet("January")
        arrow_data = sheet.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_sheet_to_arrow, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_single_sheet_pl)


def test_concurrent_load_same_sheet_lazy_to_pandas(
    expected_single_sheet_pd: pd.DataFrame,
) -> None:
    """Load the same sheet concurrently (lazy) and convert to pandas."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))

    def load_sheet_to_pandas(thread_id: int) -> pd.DataFrame:
        sheet = excel_reader.load_sheet("January")
        return sheet.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_sheet_to_pandas, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pd_assert_frame_equal(df, expected_single_sheet_pd)


def test_concurrent_load_same_sheet_eager_to_arrow(
    expected_single_sheet_pl: pl.DataFrame,
) -> None:
    """Load the same sheet concurrently (eager) and convert to arrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))

    def load_sheet_eager_to_arrow(thread_id: int) -> pl.DataFrame:
        record_batch = excel_reader.load_sheet_eager("January")
        df = pl.from_arrow(record_batch)
        assert isinstance(df, pl.DataFrame)
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_sheet_eager_to_arrow, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_single_sheet_pl)


def test_concurrent_load_same_sheet_eager_to_pandas(
    expected_single_sheet_pd: pd.DataFrame,
) -> None:
    """Load the same sheet concurrently (eager) and convert to pandas."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))

    def load_sheet_eager_to_pandas(thread_id: int) -> pd.DataFrame:
        record_batch = excel_reader.load_sheet_eager("January")
        return record_batch.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_sheet_eager_to_pandas, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pd_assert_frame_equal(df, expected_single_sheet_pd)


# Tests for concurrent conversion of already loaded sheet


def test_concurrent_conversions_on_same_loaded_sheet_to_arrow(
    expected_single_sheet_pl: pl.DataFrame,
) -> None:
    """Load a sheet once, then convert to arrow from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    def convert_to_arrow(thread_id: int) -> pl.DataFrame:
        arrow_data = sheet.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(convert_to_arrow, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_single_sheet_pl)


def test_concurrent_conversions_on_same_loaded_sheet_to_pandas(
    expected_single_sheet_pd: pd.DataFrame,
) -> None:
    """Load a sheet once, then convert to pandas from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    def convert_to_pandas(thread_id: int) -> pd.DataFrame:
        return sheet.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(convert_to_pandas, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pd_assert_frame_equal(df, expected_single_sheet_pd)


def test_concurrent_conversions_on_same_loaded_sheet_to_polars(
    expected_single_sheet_pl: pl.DataFrame,
) -> None:
    """Load a sheet once, then convert to polars from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    def convert_to_polars(thread_id: int) -> pl.DataFrame:
        return sheet.to_polars()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(convert_to_polars, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_single_sheet_pl)


def test_concurrent_mixed_conversions_on_same_loaded_sheet(
    expected_single_sheet_pl: pl.DataFrame,
    expected_single_sheet_pd: pd.DataFrame,
) -> None:
    """Load a sheet once, then call different conversion methods from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    def convert_to_arrow(thread_id: int) -> tuple[str, pl.DataFrame]:
        arrow_data = sheet.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return ("arrow", df)

    def convert_to_pandas(thread_id: int) -> tuple[str, pd.DataFrame]:
        return ("pandas", sheet.to_pandas())

    def convert_to_polars(thread_id: int) -> tuple[str, pl.DataFrame]:
        return ("polars", sheet.to_polars())

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(10):
            futures.append(executor.submit(convert_to_arrow, i))
            futures.append(executor.submit(convert_to_pandas, i))
            futures.append(executor.submit(convert_to_polars, i))

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for conversion_type, df in results:
        if conversion_type == "pandas":
            pd_assert_frame_equal(df, expected_single_sheet_pd)
        else:
            pl_assert_frame_equal(df, expected_single_sheet_pl)


# Tests for concurrent loading of tables


def test_concurrent_load_table_lazy_to_arrow(expected_table_pl: pl.DataFrame) -> None:
    """Load the same table concurrently (lazy) and convert to arrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))

    def load_table_to_arrow(thread_id: int) -> pl.DataFrame:
        table = excel_reader.load_table("users")
        arrow_data = table.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_table_to_arrow, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_table_pl)


def test_concurrent_load_table_lazy_to_pandas(expected_table_pd: pd.DataFrame) -> None:
    """Load the same table concurrently (lazy) and convert to pandas."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))

    def load_table_to_pandas(thread_id: int) -> pd.DataFrame:
        table = excel_reader.load_table("users")
        return table.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_table_to_pandas, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pd_assert_frame_equal(df, expected_table_pd)


def test_concurrent_load_table_eager_to_arrow(expected_table_pl: pl.DataFrame) -> None:
    """Load the same table concurrently (eager) and convert to arrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))

    def load_table_eager_to_arrow(thread_id: int) -> pl.DataFrame:
        record_batch = excel_reader.load_table("users", eager=True)
        df = pl.from_arrow(record_batch)
        assert isinstance(df, pl.DataFrame)
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_table_eager_to_arrow, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_table_pl)


def test_concurrent_load_table_eager_to_pandas(expected_table_pd: pd.DataFrame) -> None:
    """Load the same table concurrently (eager) and convert to pandas."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))

    def load_table_eager_to_pandas(thread_id: int) -> pd.DataFrame:
        record_batch = excel_reader.load_table("users", eager=True)
        return record_batch.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_table_eager_to_pandas, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pd_assert_frame_equal(df, expected_table_pd)


# Tests for concurrent conversion of already loaded table


def test_concurrent_conversions_on_same_loaded_table_to_arrow(
    expected_table_pl: pl.DataFrame,
) -> None:
    """Load a table once, then convert to arrow from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table = excel_reader.load_table("users")

    def convert_to_arrow(thread_id: int) -> pl.DataFrame:
        arrow_data = table.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(convert_to_arrow, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_table_pl)


def test_concurrent_conversions_on_same_loaded_table_to_pandas(
    expected_table_pd: pd.DataFrame,
) -> None:
    """Load a table once, then convert to pandas from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table = excel_reader.load_table("users")

    def convert_to_pandas(thread_id: int) -> pd.DataFrame:
        return table.to_pandas()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(convert_to_pandas, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pd_assert_frame_equal(df, expected_table_pd)


def test_concurrent_conversions_on_same_loaded_table_to_polars(
    expected_table_pl: pl.DataFrame,
) -> None:
    """Load a table once, then convert to polars from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table = excel_reader.load_table("users")

    def convert_to_polars(thread_id: int) -> pl.DataFrame:
        return table.to_polars()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(convert_to_polars, i) for i in range(20)]
        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for df in results:
        pl_assert_frame_equal(df, expected_table_pl)


def test_concurrent_mixed_conversions_on_same_loaded_table(
    expected_table_pl: pl.DataFrame,
    expected_table_pd: pd.DataFrame,
) -> None:
    """Load a table once, then call different conversion methods from multiple threads."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table = excel_reader.load_table("users")

    def convert_to_arrow(thread_id: int) -> tuple[str, pl.DataFrame]:
        arrow_data = table.to_arrow()
        df = pl.from_arrow(arrow_data)
        assert isinstance(df, pl.DataFrame)
        return ("arrow", df)

    def convert_to_pandas(thread_id: int) -> tuple[str, pd.DataFrame]:
        return ("pandas", table.to_pandas())

    def convert_to_polars(thread_id: int) -> tuple[str, pl.DataFrame]:
        return ("polars", table.to_polars())

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(10):
            futures.append(executor.submit(convert_to_arrow, i))
            futures.append(executor.submit(convert_to_pandas, i))
            futures.append(executor.submit(convert_to_polars, i))

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for conversion_type, df in results:
        if conversion_type == "pandas":
            pd_assert_frame_equal(df, expected_table_pd)
        else:
            pl_assert_frame_equal(df, expected_table_pl)


# Tests for mixed operations


def test_concurrent_mixed_operations(expected_table_pl: pl.DataFrame) -> None:
    """Mix different operations (sheets and tables, lazy and eager) concurrently."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))

    expected_sheet = expected_table_pl  # Same data in this fixture

    def load_sheet_lazy(thread_id: int) -> tuple[str, pl.DataFrame]:
        sheet = excel_reader.load_sheet("sheet1")
        df = pl.from_arrow(sheet.to_arrow())
        assert isinstance(df, pl.DataFrame)
        return ("sheet_lazy", df)

    def load_sheet_eager(thread_id: int) -> tuple[str, pl.DataFrame]:
        record_batch = excel_reader.load_sheet_eager("sheet1")
        df = pl.from_arrow(record_batch)
        assert isinstance(df, pl.DataFrame)
        return ("sheet_eager", df)

    def load_table_lazy(thread_id: int) -> tuple[str, pl.DataFrame]:
        table = excel_reader.load_table("users")
        df = pl.from_arrow(table.to_arrow())
        assert isinstance(df, pl.DataFrame)
        return ("table_lazy", df)

    def load_table_eager(thread_id: int) -> tuple[str, pl.DataFrame]:
        record_batch = excel_reader.load_table("users", eager=True)
        df = pl.from_arrow(record_batch)
        assert isinstance(df, pl.DataFrame)
        return ("table_eager", df)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(5):
            futures.append(executor.submit(load_sheet_lazy, i))
            futures.append(executor.submit(load_sheet_eager, i))
            futures.append(executor.submit(load_table_lazy, i))
            futures.append(executor.submit(load_table_eager, i))

        results = [future.result() for future in as_completed(futures)]

    # Verify all results match expected
    for operation_type, df in results:
        if "sheet" in operation_type:
            pl_assert_frame_equal(df, expected_sheet)
        else:
            pl_assert_frame_equal(df, expected_table_pl)
