from openpyxl import load_workbook
from xlrd import open_workbook
from fastexcel import read_excel


def pyxl_read(test_file_path: str):
    wb = load_workbook(test_file_path, read_only=True, keep_links=False, data_only=True)
    for ws in wb:
        rows = ws.iter_rows()
        rows = ws.values
        for row in rows:
            for value in row:
                value


def xlrd_read(test_file_path: str):
    wb = open_workbook(test_file_path)
    for ws in wb.sheets():
        for idx in range(ws.nrows):
            for value in ws.row_values(idx):
                value


def fastexcel_read(test_file_path: str):
    reader = read_excel(test_file_path)
    for sheet_name in reader.sheet_names:
        try:
            sheet = reader.load_sheet_by_name(sheet_name)
            sheet.to_arrow()
        except Exception:
            pass
