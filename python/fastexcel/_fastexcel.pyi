from __future__ import annotations

import typing
from typing import Callable, Literal

import pyarrow as pa

DType = Literal["null", "int", "float", "string", "boolean", "datetime", "date", "duration"]
DTypeMap = dict[str | int, DType]
ColumnNameFrom = Literal["provided", "looked_up", "generated"]
DTypeFrom = Literal["provided_for_all", "provided_by_index", "provided_by_name", "guessed"]
SheetVisible = Literal["visible", "hidden", "veryhidden"]

class ColumnInfo:
    def __init__(
        self,
        *,
        name: str,
        index: int,
        column_name_from: ColumnNameFrom,
        dtype: DType,
        dtype_from: DTypeFrom,
    ) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def index(self) -> int: ...
    @property
    def dtype(self) -> DType: ...
    @property
    def column_name_from(self) -> ColumnNameFrom: ...
    @property
    def dtype_from(self) -> DTypeFrom: ...

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
    def selected_columns(self) -> list[ColumnInfo]:
        """The sheet's selected columns"""
    @property
    def available_columns(self) -> list[ColumnInfo]:
        """The columns available for the given sheet"""
    @property
    def specified_dtypes(self) -> DTypeMap | None:
        """The dtypes specified for the sheet"""
    @property
    def visible(self) -> SheetVisible:
        """The visibility of the sheet"""
    def to_arrow(self) -> pa.RecordBatch:
        """Converts the sheet to a pyarrow `RecordBatch`"""

class _ExcelTable:
    @property
    def name(self) -> str:
        """The name of the table"""
    @property
    def sheet_name(self) -> str:
        """The name of the sheet this table belongs to"""
    @property
    def width(self) -> int:
        """The table's width"""
    @property
    def height(self) -> int:
        """The table's height"""
    @property
    def total_height(self) -> int:
        """The table's total height"""
    @property
    def offset(self) -> int:
        """The table's offset before data starts"""
    @property
    def selected_columns(self) -> list[ColumnInfo]:
        """The table's selected columns"""
    @property
    def available_columns(self) -> list[ColumnInfo]:
        """The columns available for the given table"""
    @property
    def specified_dtypes(self) -> DTypeMap | None:
        """The dtypes specified for the table"""
    def to_arrow(self) -> pa.RecordBatch:
        """Converts the table to a pyarrow `RecordBatch`"""

class _ExcelReader:
    """A class representing an open Excel file and allowing to read its sheets"""

    @typing.overload
    def load_sheet(
        self,
        idx_or_name: str | int,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int | None = None,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None = None,
        dtypes: DType | DTypeMap | None = None,
        eager: Literal[False] = ...,
    ) -> _ExcelSheet: ...
    @typing.overload
    def load_sheet(
        self,
        idx_or_name: str | int,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int | None = None,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | None = None,
        dtypes: DType | DTypeMap | None = None,
        eager: Literal[True] = ...,
    ) -> pa.RecordBatch: ...
    @typing.overload
    def load_table(
        self,
        name: str,
        *,
        header_row: int | None = None,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None = None,
        dtypes: DType | DTypeMap | None = None,
        eager: Literal[False] = ...,
    ) -> _ExcelTable: ...
    @typing.overload
    def load_table(
        self,
        name: str,
        *,
        header_row: int | None = None,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
        schema_sample_rows: int | None = 1_000,
        dtype_coercion: Literal["coerce", "strict"] = "coerce",
        use_columns: list[str] | list[int] | str | Callable[[ColumnInfo], bool] | None = None,
        dtypes: DType | DTypeMap | None = None,
        eager: Literal[True] = ...,
    ) -> pa.RecordBatch: ...
    @property
    def sheet_names(self) -> list[str]: ...
    def table_names(self, sheet_name: str | None = None) -> list[str]: ...

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
