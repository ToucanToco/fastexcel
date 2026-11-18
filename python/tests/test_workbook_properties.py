import io
import zipfile
from datetime import UTC, datetime

import fastexcel
import pytest

from .utils import path_for_fixture


def test_workbook_properties_xlsx() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("infer-dtypes-fallback.xlsx"))
    properties = excel_reader.workbook_properties()

    assert isinstance(properties, fastexcel.WorkbookProperties)
    assert properties.creator == "Montes, Adrian"
    assert properties.last_modified_by == "Montes, Adrian"
    assert properties.created == datetime(2024, 10, 11, 12, 55, 22, tzinfo=UTC)
    assert properties.modified == datetime(2024, 10, 11, 12, 57, 32, tzinfo=UTC)
    assert properties.application == "Microsoft Excel"
    assert properties.app_version == "16.0300"
    assert properties.company == "Hudson Advisors"
    assert properties.title is None
    assert properties.custom_properties == {}


def test_workbook_properties_custom_properties() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("fixture-type-errors.xlsx"))
    custom_properties = excel_reader.workbook_properties().custom_properties

    assert len(custom_properties) == 8
    label = "MSIP_Label_38f1469a-2c2a-4aee-b92b-090d4c5468ff"
    assert custom_properties[f"{label}_Enabled"] == "true"
    assert custom_properties[f"{label}_SetDate"] == "2025-04-22T20:30:33Z"


def test_workbook_properties_ods() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("dates.ods"))
    properties = excel_reader.workbook_properties()

    # ods files store dates without a UTC offset.
    assert properties.created == datetime(2023, 6, 1, 17, 15, 3, 994817)
    assert properties.modified == datetime(2023, 6, 1, 17, 28, 44, 40617)
    assert properties.revision == "2"
    assert properties.creator is None


def test_workbook_properties_xls_without_properties() -> None:
    excel_reader = fastexcel.read_excel(path_for_fixture("null-bytes-in-columns-names.xls"))
    properties = excel_reader.workbook_properties()

    assert properties.creator is None
    assert properties.created is None
    assert properties.custom_properties == {}
    assert repr(properties) == "WorkbookProperties()"


@pytest.mark.parametrize("created", ["0000-01-01T00:00:00Z", "0000-01-01T00:00:00"])
def test_workbook_properties_unrepresentable_dates(created: str) -> None:
    # Python's datetime only supports years 1 to 9999, so other dates are None.
    source = io.BytesIO()
    with (
        zipfile.ZipFile(path_for_fixture("infer-dtypes-fallback.xlsx")) as original,
        zipfile.ZipFile(source, "w") as modified,
    ):
        for item in original.infolist():
            data = original.read(item)
            if item.filename == "docProps/core.xml":
                data = data.replace(b"2024-10-11T12:55:22Z", created.encode())
            modified.writestr(item, data)

    properties = fastexcel.read_excel(source.getvalue()).workbook_properties()

    assert properties.created is None
    assert properties.modified == datetime(2024, 10, 11, 12, 57, 32, tzinfo=UTC)
