from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from typing import Generator

    import pandas as pd

    from .fastexcel import ExcelSheet

import pyarrow as pa

from .fastexcel import read_excel  # noqa


class ExcelFile(NamedTuple):
    sheet_names: list[str]
    sheets: "Generator[ExcelSheet, None, None]"


def load_excel(path: str) -> ExcelFile:
    sheet_names, sheets = read_excel(path)
    return ExcelFile(sheet_names, sheets)


def sheet_to_dataframe(s: "ExcelSheet") -> "pd.DataFrame":
    """Converts an ExcelSheet to a pandas DataFrame"""
    # We know for sure that the sheet will yield exactly one RecordBatch
    return list(pa.ipc.open_stream(s.to_arrow()))[0].to_pandas()
