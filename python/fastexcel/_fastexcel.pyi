from __future__ import annotations

from typing import Literal

import pyarrow as pa

_DType = Literal["null", "int", "float", "string", "boolean", "datetime", "date", "duration"]

_DTypeMap = dict[str, _DType] | dict[int, _DType]

class _ExcelSheet:
    @property
    def name(self) -> str:
        """The name of the sheet"""
    @property
    def width(self) -> int:
        """The sheet's width"""
    @property
    def height(self) -> int:
        """The sheet's height"""
    @property
    def total_height(self) -> int:
        """The sheet's total height"""
    @property
    def offset(self) -> int:
        """The sheet's offset before data starts"""
    @property
    def selected_columns(self) -> list[str] | list[int] | None:
        """The sheet's selected columns"""
    @property
    def available_columns(self) -> list[str]:
        """The columns available for the given sheet"""
    @property
    def specified_dtypes(self) -> _DTypeMap | None:
        """The dtypes specified for the sheet"""
    def to_arrow(self) -> pa.RecordBatch:
        """Converts the sheet to a pyarrow `RecordBatch`"""

class _ExcelReader:
    """A class representing an open Excel file and allowing to read its sheets"""

    def load_sheet_by_name(
        self,
        name: str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        use_columns: list[str] | list[int] | str | None = None,
        dtypes: _DTypeMap | None = None,
    ) -> _ExcelSheet: ...
    def load_sheet_by_idx(
        self,
        idx: int,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        use_columns: list[str] | list[int] | str | None = None,
        dtypes: _DTypeMap | None = None,
    ) -> _ExcelSheet: ...
    @property
    def sheet_names(self) -> list[str]: ...

def read_excel(source: str | bytes) -> _ExcelReader:
    """Reads an excel file and returns an ExcelReader"""

__version__: str

# Exceptions
class FastExcelError(Exception): ...
class UnsupportedColumnTypeCombinationError(FastExcelError): ...
class CannotRetrieveCellDataError(FastExcelError): ...
class CalamineCellError(FastExcelError): ...
class CalamineError(FastExcelError): ...
class SheetNotFoundError(FastExcelError): ...
class ColumnNotFoundError(FastExcelError): ...
class ArrowError(FastExcelError): ...
class InvalidParametersError(FastExcelError): ...
