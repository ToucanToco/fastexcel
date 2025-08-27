"""Tests for the Arrow PyCapsule Interface implementation."""

import fastexcel
import pandas as pd
import polars as pl

from .utils import path_for_fixture


def test_sheet_arrow_c_schema():
    """Test that __arrow_c_schema__ returns a valid PyCapsule."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    schema_capsule = sheet.__arrow_c_schema__()

    # Check it's a PyCapsule with the correct name
    assert hasattr(schema_capsule, "__class__")
    assert "PyCapsule" in str(type(schema_capsule))


def test_sheet_arrow_c_array():
    """Test that __arrow_c_array__ returns a tuple of PyCapsules."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    schema_capsule, array_capsule = sheet.__arrow_c_array__()

    # Check both are PyCapsules
    assert "PyCapsule" in str(type(schema_capsule))
    assert "PyCapsule" in str(type(array_capsule))


def test_table_arrow_c_schema():
    """Test that table __arrow_c_schema__ returns a valid PyCapsule."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table_names = excel_reader.table_names()

    table = excel_reader.load_table(table_names[0])  # Should be 'users'
    schema_capsule = table.__arrow_c_schema__()

    # Check it's a PyCapsule
    assert "PyCapsule" in str(type(schema_capsule))


def test_table_arrow_c_array():
    """Test that table __arrow_c_array__ returns a tuple of PyCapsules."""
    excel_reader = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table_names = excel_reader.table_names()

    table = excel_reader.load_table(table_names[0])  # Should be 'users'
    schema_capsule, array_capsule = table.__arrow_c_array__()

    # Check both are PyCapsules
    assert "PyCapsule" in str(type(schema_capsule))
    assert "PyCapsule" in str(type(array_capsule))


def test_pycapsule_interface_with_requested_schema():
    """Test PyCapsule interface methods with requested_schema parameter."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    # Test with None (current implementation ignores this)
    schema_capsule, array_capsule = sheet.__arrow_c_array__(None)

    assert "PyCapsule" in str(type(schema_capsule))
    assert "PyCapsule" in str(type(array_capsule))


def test_integration_with_polars():
    """Test that polars can consume our PyCapsule interface."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    # Polars should be able to create a DataFrame from our PyCapsule interface
    # This tests the actual interoperability
    df = pl.DataFrame(sheet)

    assert len(df) == 2
    assert df.columns == ["Month", "Year"]


def test_to_polars_without_pyarrow():
    """Test that to_polars() works via PyCapsule interface without pyarrow."""
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    # This should work via PyCapsule interface, not requiring pyarrow
    df = sheet.to_polars()

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    assert df.columns == ["Month", "Year"]

    # Test with table as well
    excel_reader_table = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table_names = excel_reader_table.table_names()
    table = excel_reader_table.load_table(table_names[0])
    df_table = table.to_polars()
    assert isinstance(df_table, pl.DataFrame)


def test_to_pandas_still_requires_pyarrow():
    """Test that to_pandas() currently still requires pyarrow.

    Note: pandas PyCapsule interface would require implementing __dataframe__
    or __arrow_c_stream__, which we don't currently do.
    """
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    sheet = excel_reader.load_sheet("January")

    # This still requires pyarrow for now
    df = sheet.to_pandas()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["Month", "Year"]

    # Test with table as well
    excel_reader_table = fastexcel.read_excel(path_for_fixture("sheet-with-tables.xlsx"))
    table_names = excel_reader_table.table_names()
    table = excel_reader_table.load_table(table_names[0])
    df_table = table.to_pandas()
    assert isinstance(df_table, pd.DataFrame)
