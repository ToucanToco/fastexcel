from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Callable, Literal

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

from os.path import expanduser
from pathlib import Path

import pyarrow as pa

from ._fastexcel import (
    ArrowError,
    CalamineCellError,
    CalamineError,
    CannotRetrieveCellDataError,
    ColumnInfo,
    ColumnNotFoundError,
    FastExcelError,
    InvalidParametersError,
    SheetNotFoundError,
    UnsupportedColumnTypeCombinationError,
    __version__,
    _ExcelReader,
    _ExcelSheet,
)
from ._fastexcel import read_excel as _read_excel

DType = Literal["null", "int", "float", "string", "boolean", "datetime", "date", "duration"]
DTypeMap: TypeAlias = "dict[str | int, DType]"
ColumnNameFrom: TypeAlias = Literal["provided", "looked_up", "generated"]
DTypeFrom: TypeAlias = Literal["provided_by_index", "provided_by_name", "guessed"]


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
        """The sheet's height, with `skip_rows` and `nrows` applied"""
        return self._sheet.height

    @property
    def total_height(self) -> int:
        """The sheet's total height"""
        return self._sheet.total_height

    @property
    def selected_columns(self) -> list[ColumnInfo]:
        """The sheet's selected columns"""
        return self._sheet.selected_columns

    @property
    def available_columns(self) -> list[ColumnInfo]:
        """The columns available for the given sheet"""
        return self._sheet.available_columns

    @property
    def specified_dtypes(self) -> DTypeMap | None:
        """The dtypes specified for the sheet"""
        return self._sheet.specified_dtypes

    def to_arrow(self) -> pa.RecordBatch:
        """Converts the sheet to a pyarrow `RecordBatch`"""
        return self._sheet.to_arrow()

    def to_pandas(self) -> "pd.DataFrame":
        """Converts the sheet to a Pandas `DataFrame`.

        Requires the `pandas` extra to be installed.
        """
        # We know for sure that the sheet will yield exactly one RecordBatch
        return self.to_arrow().to_pandas()

    def to_polars(self) -> "pl.DataFrame":
        """Converts the sheet to a Polars `DataFrame`.

        Requires the `polars` extra to be installed.
        """
        import polars as pl

        df = pl.from_arrow(data=self.to_arrow())
        assert isinstance(df, pl.DataFrame)
        return df

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

    def load_sheet(
        self,
        idx_or_name: int | str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None = None,
        dtypes: DTypeMap | None = None,
    ) -> ExcelSheet:
        """Loads a sheet lazily by index or name.

        :param idx_or_name: The index (starting at 0) or the name of the sheet to load.
        :param header_row: The index of the row containing the column labels, default index is 0.
                           If `None`, the sheet does not have any column labels.
        :param column_names: Overrides headers found in the document.
                             If `column_names` is used, `header_row` will be ignored.
        :param n_rows: Specifies how many rows should be loaded.
                       If `None`, all rows are loaded
        :param skip_rows: Specifies how many rows should be skipped after the header.
                          If `header_row` is `None`, it skips the number of rows from the
                          start of the sheet.
        :param schema_sample_rows: Specifies how many rows should be used to determine
                                   the dtype of a column.
                                   If `None`, all rows will be used.
        :param dtype_coercion: Specifies how type coercion should behave. `coerce` (the default)
                               will try to coerce different dtypes in a column to the same one,
                               whereas `strict` will raise an error in case a column contains
                               several dtypes. Note that this only applies to columns whose dtype
                               is guessed, i.e. not specified via `dtypes`.
        :param use_columns: Specifies the columns to use. Can either be:
                            - `None` to select all columns
                            - A list of strings and ints, the column names and/or indices
                              (starting at 0)
                            - A string, a comma separated list of Excel column letters and column
                              ranges (e.g. `“A:E”` or `“A,C,E:F”`, which would result in
                              `A,B,C,D,E` and `A,C,E,F`)
                            - A callable, a function that takes a column and returns a boolean
                              indicating whether the column should be used
        :param dtypes: An optional dict of dtypes. Keys can be column indices or names
        """
        return ExcelSheet(
            self._reader.load_sheet(
                idx_or_name=idx_or_name,
                header_row=header_row,
                column_names=column_names,
                skip_rows=skip_rows,
                n_rows=n_rows,
                schema_sample_rows=schema_sample_rows,
                dtype_coercion=dtype_coercion,
                use_columns=use_columns,
                dtypes=dtypes,
                eager=False,
            )
        )

    def load_sheet_eager(
        self,
        idx_or_name: int | str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | None = None,
        dtypes: DTypeMap | None = None,
    ) -> pa.RecordBatch:
        """Loads a sheet eagerly by index or name.

        For xlsx files, this will be faster and more memory-efficient, as it will use
        `worksheet_range_ref` under the hood, which returns borrowed types.

        Refer to `load_sheet` for parameter documentation
        """
        return self._reader.load_sheet(
            idx_or_name=idx_or_name,
            header_row=header_row,
            column_names=column_names,
            skip_rows=skip_rows,
            n_rows=n_rows,
            schema_sample_rows=schema_sample_rows,
            dtype_coercion=dtype_coercion,
            use_columns=use_columns,
            dtypes=dtypes,
            eager=True,
        )

    def load_sheet_by_name(
        self,
        name: str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None = None,
        dtypes: DTypeMap | None = None,
    ) -> ExcelSheet:
        """Loads a sheet by name.

        Refer to `load_sheet` for parameter documentation
        """
        return self.load_sheet(
            name,
            header_row=header_row,
            column_names=column_names,
            skip_rows=skip_rows,
            n_rows=n_rows,
            schema_sample_rows=schema_sample_rows,
            dtype_coercion=dtype_coercion,
            use_columns=use_columns,
            dtypes=dtypes,
        )

    def load_sheet_by_idx(
        self,
        idx: int,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None = None,
        dtypes: DTypeMap | None = None,
    ) -> ExcelSheet:
        """Loads a sheet by index.

        Refer to `load_sheet` for parameter documentation
        """
        return self.load_sheet(
            idx,
            header_row=header_row,
            column_names=column_names,
            skip_rows=skip_rows,
            n_rows=n_rows,
            schema_sample_rows=schema_sample_rows,
            dtype_coercion=dtype_coercion,
            use_columns=use_columns,
            dtypes=dtypes,
        )

    def __repr__(self) -> str:
        return self._reader.__repr__()


def read_excel(source: Path | str | bytes) -> ExcelReader:
    """Opens and loads an excel file.

    :param source: The path to a file or its content as bytes
    """
    if isinstance(source, (str, Path)):
        source = expanduser(source)
    return ExcelReader(_read_excel(source))


__all__ = (
    ## version
    "__version__",
    ## main entrypoint
    "read_excel",
    ## Python types
    "DType",
    "DTypeMap",
    # Excel reader
    "ExcelReader",
    # Excel sheet
    "ExcelSheet",
    # Column metadata
    "DTypeFrom",
    "ColumnNameFrom",
    "ColumnInfo",
    # Exceptions
    "FastExcelError",
    "CannotRetrieveCellDataError",
    "CalamineCellError",
    "CalamineError",
    "SheetNotFoundError",
    "ColumnNotFoundError",
    "ArrowError",
    "InvalidParametersError",
    "UnsupportedColumnTypeCombinationError",
)
