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
    def offset(self) -> int:
        """The sheet's offset before data starts"""
    def to_arrow(self) -> bytes:
        """Converts the sheet to an Arrow RecordBatch.

        The RecordBatch is serialized to the IPC format. It can be read with
        `pyarrow.ipc.open_stream`.
        """

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
    ) -> _ExcelSheet: ...
    def load_sheet_by_idx(
        self,
        idx: int,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
    ) -> _ExcelSheet: ...
    def load_sheet(
        self,
        idx_or_name: int | str,
        *,
        header_row: int | None = 0,
        column_names: list[str] | None = None,
        skip_rows: int = 0,
        n_rows: int | None = None,
    ) -> _ExcelSheet: ...
    @property
    def sheet_names(self) -> list[str]: ...

def read_excel(path: str) -> _ExcelReader:
    """Reads an excel file and returns an ExcelReader"""

__version__: str
