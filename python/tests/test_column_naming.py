from __future__ import annotations

from typing import Any

import fastexcel
import pytest

from .utils import path_for_fixture


@pytest.fixture
def duplicated_columns_reader() -> fastexcel.ExcelReader:
    return fastexcel.read_excel(path_for_fixture("fixture-single-sheet-duplicated-columns.xlsx"))


@pytest.fixture
def tables_reader() -> fastexcel.ExcelReader:
    return fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))


SHEET_CASES = [
    pytest.param({}, ["col", "col_1", "col_2"], id="defaults"),
    pytest.param(
        {"duplicate_column_separator": "."}, ["col", "col.1", "col.2"], id="custom_separator"
    ),
    pytest.param({"duplicate_column_separator": ""}, ["col", "col1", "col2"], id="empty_separator"),
    pytest.param(
        {"header_row": None},
        ["__UNNAMED__0", "__UNNAMED__1", "__UNNAMED__2"],
        id="no_header_default_prefix",
    ),
    pytest.param(
        {"header_row": None, "unnamed_column_prefix": "column_"},
        ["column_0", "column_1", "column_2"],
        id="custom_prefix",
    ),
    pytest.param(
        {"header_row": None, "unnamed_column_prefix": ""}, ["0", "1", "2"], id="empty_prefix"
    ),
    pytest.param(
        {"header_row": None, "column_names": ["1"], "unnamed_column_prefix": ""},
        ["1", "1_1", "2"],
        id="generated_names_are_deduplicated",
    ),
    pytest.param({"column_names": ["a", "a", "a_1"]}, ["a", "a_1", "a_1_1"], id="collision_chain"),
]


@pytest.mark.parametrize("kwargs, expected", SHEET_CASES)
def test_sheet_column_naming(
    duplicated_columns_reader: fastexcel.ExcelReader,
    kwargs: dict[str, Any],
    expected: list[str],
) -> None:
    sheet = duplicated_columns_reader.load_sheet(0, **kwargs)
    assert [col.name for col in sheet.available_columns()] == expected
    assert sheet.to_polars().columns == expected


@pytest.mark.parametrize("kwargs, expected", SHEET_CASES)
def test_sheet_column_naming_by_name_and_idx(
    duplicated_columns_reader: fastexcel.ExcelReader,
    kwargs: dict[str, Any],
    expected: list[str],
) -> None:
    by_idx = duplicated_columns_reader.load_sheet_by_idx(0, **kwargs)
    assert [col.name for col in by_idx.available_columns()] == expected
    by_name = duplicated_columns_reader.load_sheet_by_name(by_idx.name, **kwargs)
    assert [col.name for col in by_name.available_columns()] == expected


@pytest.mark.parametrize("kwargs, expected", SHEET_CASES)
def test_sheet_column_naming_eager(
    duplicated_columns_reader: fastexcel.ExcelReader,
    kwargs: dict[str, Any],
    expected: list[str],
) -> None:
    assert duplicated_columns_reader.load_sheet(0, eager=True, **kwargs).schema.names == expected
    assert duplicated_columns_reader.load_sheet_eager(0, **kwargs).schema.names == expected


TABLE_CASES = [
    pytest.param(
        {"column_names": ["a", "a"]},
        ["a", "a_1", "__UNNAMED__2", "__UNNAMED__3"],
        id="defaults",
    ),
    pytest.param(
        {
            "column_names": ["a", "a"],
            "duplicate_column_separator": "-",
            "unnamed_column_prefix": "col",
        },
        ["a", "a-1", "col2", "col3"],
        id="custom_options",
    ),
]


@pytest.mark.parametrize("kwargs, expected", TABLE_CASES)
def test_table_column_naming(
    tables_reader: fastexcel.ExcelReader, kwargs: dict[str, Any], expected: list[str]
) -> None:
    table = tables_reader.load_table("users", **kwargs)
    assert [col.name for col in table.available_columns()] == expected
    assert table.to_polars().columns == expected
    assert tables_reader.load_table("users", eager=True, **kwargs).schema.names == expected


def test_use_columns_refers_to_deduplicated_names(
    duplicated_columns_reader: fastexcel.ExcelReader,
) -> None:
    sheet = duplicated_columns_reader.load_sheet(
        0, duplicate_column_separator=".", use_columns=["col.2", "col"]
    )
    assert [col.name for col in sheet.selected_columns] == ["col.2", "col"]
    assert sheet.to_polars().columns == ["col.2", "col"]


def test_table_use_columns_refers_to_table_header_names() -> None:
    # The generated name of unselected column 0 ("Column0") clashes with the table header of
    # column 1, which gets renamed. use_columns still selects by table header name.
    reader = fastexcel.read_excel(path_for_fixture("table-with-numbered-headers.xlsx"))
    table = reader.load_table("numbered", use_columns=["Column0"], unnamed_column_prefix="Column")
    assert [col.name for col in table.available_columns()] == ["Column0", "Column0_1", "Column2"]
    assert [(col.name, col.index) for col in table.selected_columns] == [("Column0_1", 1)]
    assert table.to_polars().to_dict(as_series=False) == {"Column0_1": [1.0, 2.0]}
