from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

import pyarrow as pa

from ._fastexcel import __version__, _ExcelReader, _ExcelSheet
from ._fastexcel import read_excel as _read_excel


class ExcelSheet:
    """A class representing a single sheet in an Excel File"""

    def __init__(self, sheet: _ExcelSheet) -> None:
        self._sheet = sheet

    @property
    def name(self) -> str:
        """The name of the sheet"""
        return self._sheet.name

    @property
    def width(self) -> int:
        """The sheet's width"""
        return self._sheet.width

    @property
    def height(self) -> int:
        """The sheet's height, with skip_rows and nrows applied"""
        return self._sheet.height

    @property
    def total_height(self) -> int:
        """The sheet's total height"""
        return self._sheet.total_height

    def to_arrow(self) -> bytes:
        """Converts the sheet to an Arrow RecordBatch.

        The RecordBatch is serialized to the IPC format. It can be read with
        `pyarrow.ipc.open_stream`.
        """
        return self._sheet.to_arrow()

    def to_pandas(self) -> "pd.DataFrame":
        """Converts the sheet to a pandas DataFrame.

        Requires the "pandas" extra to be installed.
        """
        # We know for sure that the sheet will yield exactly one RecordBatch
        return list(pa.ipc.open_stream(self.to_arrow()))[0].to_pandas()

    def __repr__(self) -> str:
        return self._sheet.__repr__()


class ExcelReader:
    """A class representing an open Excel file and allowing to read its sheets"""

    def __init__(self, reader: _ExcelReader) -> None:
        self._reader = reader

    @property
    def sheet_names(self) -> list[str]:
        """The list of sheet names"""
        return self._reader.sheet_names

    def load_sheet_by_name(
        self,
        name: str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
    ) -> ExcelSheet:
        """Loads a sheet by name.

        `header_row` is the index of the row containing the column labels, default index is 0.
        If `None`, the sheet does not have any column labels.
        `column_names` overrides headers found in the document.
        If `column_names` is used `header_row` will be ignored.
        `n_rows` is the number of rows to parse.
        `skip_rows` is the line numbers to skip after the headers.
        If `header_row` is `None` then it skips the number of lines from 0.
        """
        return ExcelSheet(
            self._reader.load_sheet_by_name(
                name,
                header_row=header_row,
                column_names=column_names,
                skip_rows=skip_rows,
                n_rows=n_rows,
            )
        )

    def load_sheet_by_idx(
        self,
        idx: int,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
    ) -> ExcelSheet:
        """Loads a sheet by his index.

        `header_row` is the index of the row containing the column labels, default index is 0.
        If `None`, the sheet does not have any column labels.
        `column_names` overrides headers found in the document.
        If `column_names` is used `header_row` will be ignored.
        `n_rows` is the number of rows to parse.
        `skip_rows` is the line numbers to skip after the headers.
        If `header_row` is `None` then it skips the number of lines from 0.
        """
        if idx < 0:
            raise ValueError(f"Expected idx to be > 0, got {idx}")
        return ExcelSheet(
            self._reader.load_sheet_by_idx(
                idx,
                header_row=header_row,
                column_names=column_names,
                skip_rows=skip_rows,
                n_rows=n_rows,
            )
        )

    def load_sheet(
        self,
        idx_or_name: int | str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
    ) -> ExcelSheet:
        """Loads a sheet by name if a string is passed or by index if an integer is passed.

        `header_row` is the index of the row containing the column labels, default index is 0.
        If `None`, the sheet does not have any column labels.
        `column_names` overrides headers found in the document.
        If `column_names` is used `header_row` will be ignored.
        `n_rows` is the number of rows to parse.
        `skip_rows` is the line numbers to skip after the headers.
        If `header_row` is `None` then it skips the number of lines from 0.
        """
        return (
            self.load_sheet_by_idx(
                idx_or_name,
                header_row=header_row,
                column_names=column_names,
                skip_rows=skip_rows,
                n_rows=n_rows,
            )
            if isinstance(idx_or_name, int)
            else self.load_sheet_by_name(
                idx_or_name,
                header_row=header_row,
                column_names=column_names,
                skip_rows=skip_rows,
                n_rows=n_rows,
            )
        )

    def __repr__(self) -> str:
        return self._reader.__repr__()


def read_excel(path: str) -> ExcelReader:
    return ExcelReader(_read_excel(path))


__all__ = ("ExcelReader", "ExcelSheet", "read_excel", "__version__")
