from os.path import dirname
from os.path import join as path_join

from pandas import DataFrame, Timestamp
from pandas.testing import assert_frame_equal

import fastexcel


def path_for_fixture(fixture_file: str) -> str:
    return path_join(dirname(__file__), "fixtures", fixture_file)


def test_single_sheet_to_pandas():
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-single-sheet.xlsx"))
    assert excel_reader.sheet_names == ["January"]
    sheet_by_name = excel_reader.load_sheet("January")
    sheet_by_idx = excel_reader.load_sheet(0)

    # Metadata
    assert sheet_by_name.name == sheet_by_idx.name == "January"
    assert sheet_by_name.height == sheet_by_idx.height == 2
    assert sheet_by_name.width == sheet_by_idx.width == 2

    expected = DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]})

    assert_frame_equal(sheet_by_name.to_pandas(), expected)
    assert_frame_equal(sheet_by_idx.to_pandas(), expected)


def test_single_sheet_with_types_to_pandas():
    excel_reader = fastexcel.read_excel(
        path_for_fixture("fixture-single-sheet-with-types.xlsx")
    )
    assert excel_reader.sheet_names == ["Sheet1"]

    sheet = excel_reader.load_sheet(0)
    assert sheet.name == "Sheet1"
    assert sheet.height == 3
    assert sheet.width == 4

    assert_frame_equal(
        sheet.to_pandas(),
        DataFrame(
            {
                "__NAMELESS__": [0.0, 1.0, 2.0],
                "bools": [True, False, True],
                "dates": [Timestamp("2022-03-02 05:43:04")] * 3,
                "floats": [12.35, 42.69, 1234567],
            }
        ),
    )


def test_multiple_sheets_to_pandas():
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-multi-sheet.xlsx"))
    assert excel_reader.sheet_names == ["January", "February", "With unnamed columns"]

    assert_frame_equal(
        excel_reader.load_sheet_by_idx(0).to_pandas(),
        DataFrame({"Month": [1.0], "Year": [2019.0]}),
    )

    assert_frame_equal(
        excel_reader.load_sheet_by_idx(1).to_pandas(),
        DataFrame({"Month": [2.0, 3.0, 4.0], "Year": [2019.0, 2021.0, 2022.0]}),
    )

    assert_frame_equal(
        excel_reader.load_sheet_by_name("With unnamed columns").to_pandas(),
        DataFrame(
            {
                "col1": [2.0, 3.0],
                "__NAMELESS__": [1.5, 2.5],
                "col3": ["hello", "world"],
                "__NAMELESS___1": [-5.0, -6.0],
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

    expected = DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]})

    assert_frame_equal(sheet_by_name.to_pandas(), expected)
    assert_frame_equal(sheet_by_idx.to_pandas(), expected)


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

    expected = DataFrame(
        {"column_0": [1.0, 2.0], "column_1": [3.0, 4.0], "column_2": [5.0, 6.0]}
    )

    assert_frame_equal(sheet_by_name.to_pandas(), expected)
    assert_frame_equal(sheet_by_idx.to_pandas(), expected)


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

    expected = DataFrame({"Month": [1.0, 2.0], "Year": [2019.0, 2020.0]})

    assert_frame_equal(sheet_by_name.to_pandas(), expected)
    assert_frame_equal(sheet_by_idx.to_pandas(), expected)


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

    expected = DataFrame({"foo": [1.0, 2.0], "bar": [3.0, 4.0], "baz": [5.0, 6.0]})

    assert_frame_equal(sheet_by_name.to_pandas(), expected)
    assert_frame_equal(sheet_by_idx.to_pandas(), expected)


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

    expected = DataFrame(
        {"Bugs": [1.0, 2.0], "column_1": [3.0, 4.0], "column_2": [5.0, 6.0]}
    )

    assert_frame_equal(sheet_by_name.to_pandas(), expected)
    assert_frame_equal(sheet_by_idx.to_pandas(), expected)