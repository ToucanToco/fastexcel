def read_excel(path: str) -> list[bytes]:
    """Reads an excel file and returns a list of bytes representing.

    Each bytes objects represents a sheet of the file as an Arrow RecordBatch,
    serialized in Arrow's IPC format.
    """
