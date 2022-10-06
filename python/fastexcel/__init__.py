from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Generator

    import pandas as pd


import pyarrow as pa

from .fastexcel import read_excel, read_excel_lazy


def load_excel_file(path: str) -> "Generator[pd.DataFrame, None, None]":
    raw_record_batches = read_excel(path)

    def iter_():
        for raw_record_batch in raw_record_batches:
            for record_batch in pa.ipc.open_stream(raw_record_batch):
                yield record_batch.to_pandas()

    return iter_()


def load_excel_file_lazy(path: str) -> "Generator[pd.DataFrame, None, None]":
    raw_record_batches = read_excel_lazy(path)

    def iter_():
        for raw_record_batch in raw_record_batches:
            for record_batch in pa.ipc.open_stream(raw_record_batch):
                yield record_batch.to_pandas()

    return iter_()
