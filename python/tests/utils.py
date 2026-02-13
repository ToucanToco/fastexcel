from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def path_for_fixture(fixture_file: str) -> str:
    return str(Path(__file__).parent.parent.parent / "tests" / "fixtures" / fixture_file)


def get_expected_pandas_dtype(fastexcel_dtype: str) -> Any:
    """Get the expected pandas dtype for a given fastexcel dtype, accounting for pandas version.

    In pandas < 3.0, string columns use object dtype.
    In pandas >= 3.0, string columns use StringDtype (with na_value=nan when from Arrow).
    """
    pd_version = tuple(int(x) for x in pd.__version__.split(".")[:2])

    dtype_map = {
        "int": np.dtype("int64"),
        "float": np.dtype("float64"),
        "boolean": np.dtype("bool"),
        "datetime": np.dtype("datetime64[ms]"),
        "duration": np.dtype("timedelta64[ms]"),
    }

    if fastexcel_dtype in dtype_map:
        return dtype_map[fastexcel_dtype]

    if fastexcel_dtype == "string":
        if pd_version >= (3, 0):
            # When converting from Arrow, pandas uses nan as na_value
            return pd.StringDtype(na_value=np.nan)
        else:
            return np.dtype("object")

    if fastexcel_dtype == "date":
        # Date columns are always object dtype
        return np.dtype("object")

    raise ValueError(f"Unknown fastexcel dtype: {fastexcel_dtype}")


def assert_pandas_dtypes(df: pd.DataFrame, expected_dtypes: dict[str, str]) -> None:
    """Assert that a pandas DataFrame has the expected dtypes for each column.

    Args:
        df: The pandas DataFrame to check
        expected_dtypes: A dict mapping column names to fastexcel dtype strings
    """
    for col_name, fastexcel_dtype in expected_dtypes.items():
        expected_dtype = get_expected_pandas_dtype(fastexcel_dtype)
        actual_dtype = df[col_name].dtype
        assert actual_dtype == expected_dtype, (
            f"Column '{col_name}': expected dtype {expected_dtype}, got {actual_dtype}"
        )
