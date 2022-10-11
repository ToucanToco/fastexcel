from typing import Generator

class ExcelSheet:
    """A class representing a single sheet in an Excel File"""

    def name(self) -> str:
        """The name of the sheet"""
    def width(self) -> int:
        """The sheet's width"""
    def height(self) -> int:
        """The sheet's height"""
    def to_arrow(self) -> bytes:
        """Converts the sheet to an Arrow RecordBatch.

        The RecordBatch is serialized to the IPC format. It can be read with
        `pyarrow.ipc.open_stream`.
        """

def read_excel(path: str) -> tuple[list[str], Generator[ExcelSheet, None, None]]:
    """Reads an excel file and returns a generator of bytes objects.

    Each bytes object represents a sheet of the file as an Arrow RecordBatch,
    serialized in Arrow's IPC format.
    """
