import fastexcel
import pytest

from .utils import path_for_fixture


@pytest.mark.parametrize("path", ("sheet-with-defined-names.xlsx",))
def test_defined_names(path: str) -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture(path))
    defined_names = excel_reader.defined_names()

    names_dict = {dn.name: dn.formula for dn in defined_names}

    assert names_dict == {
        "AddingValues": "SUM(sheet1!$K$5:$K$6)",
        "DefinedRange": "sheet1!$A$5:$D$7",
        "NamedConstant": "3.4",
    }
