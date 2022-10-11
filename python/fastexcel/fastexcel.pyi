from typing import Generator

def read_excel(path: str) -> Generator[bytes, None, None]:
    """Reads an excel file and returns a generator of bytes objects.

    Each bytes object represents a sheet of the file as an Arrow RecordBatch,
    serialized in Arrow's IPC format.
    """
