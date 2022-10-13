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
    def to_arrow(self) -> bytes:
        """Converts the sheet to an Arrow RecordBatch.

        The RecordBatch is serialized to the IPC format. It can be read with
        `pyarrow.ipc.open_stream`.
        """

class _ExcelReader:
    """A class representing an open Excel file and allowing to read its sheets"""

    def load_sheet_by_name(self, name: str) -> _ExcelSheet:
        """Loads a sheet by name"""
    def load_sheet_by_idx(self, idx: int) -> _ExcelSheet:
        """Loads a sheet by index"""
    @property
    def sheet_names(self) -> list[str]:
        """The list of sheet names"""

def read_excel(path: str) -> _ExcelReader:
    """Reads an excel file and returns an ExcelReader"""
