from __future__ import annotations

import fastexcel
import pytest

from utils import path_for_fixture


def test_read_excel_bad_type() -> None:
    expected_message = "source must be a string or bytes"
    with pytest.raises(fastexcel.InvalidParametersError, match=expected_message):
        fastexcel.read_excel(42)  # type: ignore[arg-type]


def test_does_not_exist() -> None:
    expected_message = """calamine error: Cannot detect file format
Context:
    0: Could not open workbook at path_does_not_exist.nope
    1: could not load excel file at path_does_not_exist.nope"""

    with pytest.raises(fastexcel.CalamineError, match=expected_message) as exc_info:
        fastexcel.read_excel("path_does_not_exist.nope")

    assert exc_info.value.__doc__ == "Generic calamine error"

    # Should also work with the base error type
    with pytest.raises(fastexcel.FastExcelError, match=expected_message):
        fastexcel.read_excel("path_does_not_exist.nope")


def test_sheet_idx_not_found_error() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    expected_message = """sheet at index 42 not found
Context:
    0: Sheet index 42 is out of range. File has 1 sheets."""

    with pytest.raises(fastexcel.SheetNotFoundError, match=expected_message) as exc_info:
        excel_reader.load_sheet(42)

    assert exc_info.value.__doc__ == "Sheet was not found"

    # Should also work with the base error type
    with pytest.raises(fastexcel.FastExcelError, match=expected_message):
        excel_reader.load_sheet(42)


def test_sheet_name_not_found_error() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    expected_message = """sheet with name "idontexist" not found
Context:
    0: Sheet "idontexist" not found in file. Available sheets: "January"."""

    with pytest.raises(fastexcel.SheetNotFoundError, match=expected_message) as exc_info:
        excel_reader.load_sheet("idontexist")

    assert exc_info.value.__doc__ == "Sheet was not found"


@pytest.mark.parametrize(
    "exc_class, expected_docstring",
    [
        (fastexcel.FastExcelError, "The base class for all fastexcel errors"),
        (
            fastexcel.UnsupportedColumnTypeCombinationError,
            "Column contains an unsupported type combination",
        ),
        (fastexcel.CannotRetrieveCellDataError, "Data for a given cell cannot be retrieved"),
        (
            fastexcel.CalamineCellError,
            "calamine returned an error regarding the content of the cell",
        ),
        (fastexcel.CalamineError, "Generic calamine error"),
        (fastexcel.ColumnNotFoundError, "Column was not found"),
        (fastexcel.SheetNotFoundError, "Sheet was not found"),
        (fastexcel.ArrowError, "Generic arrow error"),
        (fastexcel.InvalidParametersError, "Provided parameters are invalid"),
    ],
)
def test_docstrings(exc_class: type[Exception], expected_docstring: str) -> None:
    assert exc_class.__doc__ == expected_docstring
