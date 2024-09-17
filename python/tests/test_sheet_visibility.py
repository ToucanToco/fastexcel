import fastexcel

from utils import path_for_fixture


def test_sheet_visibilities() -> None:
    file_path = path_for_fixture("fixture-sheets-different-visibilities.xlsx")

    reader = fastexcel.read_excel(file_path)

    assert reader.load_sheet(0).visible == "visible"
    assert reader.load_sheet(1).visible == "hidden"
    assert reader.load_sheet(2).visible == "veryhidden"
