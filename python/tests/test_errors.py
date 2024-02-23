import fastexcel
import pytest
from utils import path_for_fixture


def test_does_not_exist() -> None:
    expected_message = """calamine error: Cannot detect file format
Context:
    0: Could not open workbook at path_does_not_exist.nope
    1: could not load excel file at path_does_not_exist.nope"""

    with pytest.raises(fastexcel.CalamineError, match=expected_message):
        fastexcel.read_excel("path_does_not_exist.nope")

    # Should also work with the base error type
    with pytest.raises(fastexcel.FastExcelError, match=expected_message):
        fastexcel.read_excel("path_does_not_exist.nope")


def test_sheet_not_found_error() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    expected_message = """sheet at index 42 not found
Context:
    0: Sheet index 42 is out of range. File has 1 sheets"""

    with pytest.raises(fastexcel.SheetNotFoundError, match=expected_message):
        excel_reader.load_sheet(42)

    # Should also work with the base error type
    with pytest.raises(fastexcel.FastExcelError, match=expected_message):
        excel_reader.load_sheet(42)
