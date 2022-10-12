from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

    from .fastexcel import ExcelSheet

import pyarrow as pa

from .fastexcel import ExcelReader  # noqa
from .fastexcel import read_excel  # noqa


def sheet_to_dataframe(s: "ExcelSheet") -> "pd.DataFrame":
    """Converts an ExcelSheet to a pandas DataFrame"""
    # We know for sure that the sheet will yield exactly one RecordBatch
    return list(pa.ipc.open_stream(s.to_arrow()))[0].to_pandas()
