"""
Compare read performance with fastexcel, xlrd and different openpyxl options
"""

import pytest

from .readers import fastexcel_read, pyxl_read, xlrd_read


@pytest.fixture
def plain_data_xls():
    return "./python/tests/benchmarks/fixtures/plain_data.xls"


@pytest.fixture
def plain_data_xlsx():
    return "./python/tests/benchmarks/fixtures/plain_data.xlsx"


@pytest.fixture
def formula_xlsx():
    return "./python/tests/benchmarks/fixtures/formulas.xlsx"


@pytest.mark.benchmark(group="xlsx")
def test_pyxl(benchmark, plain_data_xlsx):
    benchmark(pyxl_read, plain_data_xlsx)


@pytest.mark.benchmark(group="xls")
def test_xlrd(benchmark, plain_data_xls):
    benchmark(xlrd_read, plain_data_xls)


@pytest.mark.benchmark(group="xls")
def test_fastexcel_xls(benchmark, plain_data_xls):
    benchmark(fastexcel_read, plain_data_xls)


@pytest.mark.benchmark(group="xlsx")
def test_fastexcel_xlsx(benchmark, plain_data_xlsx):
    benchmark(fastexcel_read, plain_data_xlsx)


@pytest.mark.benchmark(group="xlsx")
def test_pyxl_with_formulas(benchmark, formula_xlsx):
    benchmark(pyxl_read, formula_xlsx)


@pytest.mark.benchmark(group="xlsx")
def test_fastexcel_with_formulas(benchmark, formula_xlsx):
    benchmark(fastexcel_read, formula_xlsx)
