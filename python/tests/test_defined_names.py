import fastexcel
import pytest

from .utils import path_for_fixture


@pytest.mark.parametrize("path", ("sheet-with-defined-names.xlsx",))
def test_defined_names(path: str) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture(path))
    defined_names = excel_reader.defined_names()

    assert len(defined_names) == 3
    assert ("DefinedRange", "sheet1!$A$5:$D$7") in defined_names
    assert ("NamedConstant", "3.4") in defined_names
    assert ("AddingValues", "SUM(sheet1!$K$5:$K$6)") in defined_names
