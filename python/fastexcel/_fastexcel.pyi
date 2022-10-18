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

    def load_sheet_by_name(self, name: str, header_line: None | int = 0) -> _ExcelSheet:
        """Loads a sheet by name.
        Optionaly a header_line can be specified.
        If None is passed it means that the sheet dont have any header.
        Otherwise it tries to use the first line as headers name."""
    def load_sheet_by_idx(self, idx: int, header_line: None | int = 0) -> _ExcelSheet:
        """Loads a sheet by index.
        Optionaly a header_line can be specified.
        If None is passed it means that the sheet dont have any header.
        Otherwise it tries to use the first line as headers name."""
    def load_sheet(
        self, idx_or_name: int | str, header_line: None | int = 0
    ) -> _ExcelSheet:
        """Try's to load a sheet name if a string is passed or a sheet index if a integer is passed.
        Optionaly a header_line can be specified.
        If None is passed it means that the sheet dont have any header.
        Otherwise it tries to use the first line as headers name."""
    @property
    def sheet_names(self) -> list[str]:
        """The list of sheet names"""

def read_excel(path: str) -> _ExcelReader:
    """Reads an excel file and returns an ExcelReader"""

__version__: str
