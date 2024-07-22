from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest


@pytest.fixture
def expected_data_sheet_null_strings() -> dict[str, list[Any]]:
    return {
        "FIRST_LABEL": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "SECOND_LABEL": ["AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH", "II", "JJ"],
        "DATES_AND_NULLS": [
            None,
            None,
            None,
            datetime(2022, 12, 19, 0, 0),
            datetime(2022, 8, 26, 0, 0),
            datetime(2023, 5, 6, 0, 0),
            datetime(2023, 3, 20, 0, 0),
            datetime(2022, 8, 29, 0, 0),
            None,
            None,
        ],
        "TIMESTAMPS_AND_NULLS": [
            None,
            None,
            datetime(2023, 2, 18, 6, 13, 56, 730000),
            datetime(2022, 9, 20, 20, 0, 7, 50000),
            datetime(2022, 9, 24, 17, 4, 31, 236000),
            None,
            None,
            None,
            datetime(2022, 9, 14, 1, 50, 58, 390000),
            datetime(2022, 10, 21, 17, 20, 12, 223000),
        ],
        "INTS_AND_NULLS": [
            2076.0,
            2285.0,
            39323.0,
            None,
            None,
            None,
            11953.0,
            None,
            30192.0,
            None,
        ],
        "FLOATS_AND_NULLS": [
            141.02023312814603,
            778.0655928608671,
            None,
            497.60307287584106,
            627.446112513911,
            None,
            None,
            None,
            488.3509486743364,
            None,
        ],
    }
