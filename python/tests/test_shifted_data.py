import fastexcel

from .utils import path_for_fixture


def test_sheet_with_offset():
    reader = fastexcel.read_excel(path_for_fixture("sheet-and-table-with-offset.xlsx"))
    sheet = reader.load_sheet("without-table")

    assert sheet.available_columns() == [
        fastexcel.ColumnInfo(
            name="Column at H10",
            index=0,
            absolute_index=7,
            dtype="float",
            dtype_from="guessed",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="Column at I10",
            index=1,
            absolute_index=8,
            dtype="float",
            dtype_from="guessed",
            column_name_from="looked_up",
        ),
        fastexcel.ColumnInfo(
            name="__UNNAMED__2",
            index=2,
            absolute_index=9,
            dtype="string",
            dtype_from="guessed",
            column_name_from="generated",
        ),
        fastexcel.ColumnInfo(
            name="Column at K10",
            index=3,
            absolute_index=10,
            dtype="float",
            dtype_from="guessed",
            column_name_from="looked_up",
        ),
    ]


def test_table_with_offset():
    reader = fastexcel.read_excel(path_for_fixture("sheet-and-table-with-offset.xlsx"))
    table = reader.load_table("TableAtD5")

    assert table.available_columns() == [
        fastexcel.ColumnInfo(
            name="Column at D5",
            index=0,
            absolute_index=3,
            dtype="float",
            dtype_from="guessed",
            column_name_from="provided",
        ),
        fastexcel.ColumnInfo(
            name="Column at E5",
            index=1,
            absolute_index=4,
            dtype="float",
            dtype_from="guessed",
            column_name_from="provided",
        ),
    ]
