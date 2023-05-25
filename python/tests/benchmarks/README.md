# Benchmarks

These benchmarks were generated using `pytest-benchmark`.

> **_NOTE:_**  formulas.xlsx was found [here](https://foss.heptapod.net/openpyxl/openpyxl/-/issues/494) plain_data.xls and plain_data.xlsx can be found [here](https://public.opendatasoft.com/explore/dataset/covid-19-pandemic-worldwide-data/export/?disjunctive.zone&disjunctive.category)

Using the following command:

```bash
make benchmarks
```

The results are from my local machine. This is not 100% accurate.

## Speed
### 'xls': 2 tests
|Name (time in ms)|Min|Max|Mean|StdDev|Median|IQR|Outliers|OPS|Rounds|Iterations|
|-----------------|---|---|----|------|------|---|-------|---|-------|----------|
test_fastexcel_xls|26.6252 (1.0)|28.9866 (1.0)|27.3754 (1.0)|0.5747 (1.0)|27.1692 (1.0)|0.6015 (1.0)|9;3|36.5291 (1.0)|36|1|
test_xlrd|615.1377 (23.10)|643.2589 (22.19)|629.4745 (22.99)|10.5047 (18.28)|627.6482 (23.10)|13.7112 (22.79)|2;0|1.5886 (0.04)|5|1|


### 'xlsx': 4 tests
|Name (time in ms)|Min|Max|Mean|StdDev|Median|IQR|Outliers|OPS|Rounds  Iterations|
|-----------------|---|---|----|------|------|---|--------|---|------------------|
|test_fastexcel_xlsx|448.6932 (1.0)|464.1538 (1.0)|452.2098 (1.0)|6.7106 (1.0)|449.1060 (1.0)|5.0731 (1.0)|1;1|2.2114 (1.0)|5|1|
|test_fastexcel_with_formulas|453.6183 (1.01)|474.6849 (1.02)|462.3037 (1.02)|8.3138 (1.24)|462.9594 (1.03)|11.7921 (2.32)|2;0|2.1631 (0.98)|5|1|
|test_pixel_with_formulas|4,696.1738 (10.47)|4,928.2115 (10.62)|4,817.2041 (10.65)|82.6551 (12.32)|4,820.8642 (10.73)|77.0684 (15.19)|2;0|0.2076 (0.09)|5|1|
|test_pixel|4,713.1411 (10.50)|4,791.7239 (10.32)|4,767.0285 (10.54)|32.2795 (4.81)|4,775.2210 (10.63)|39.9527 (7.88)|1;0|0.2098 (0.09)|5|1|

## Memory usage

| | |
|-|-|
|![fastexcel xls](memory_profiles/test_xls_fastexcel.png "fastexcel xls") |![xlrd xls](memory_profiles/test_xls_xlrd.png "xlrd xls")|
|![fastexcel xlsx](memory_profiles/test_xlsx_fastexcel.png "fastexcel xlsx") |![pyxl xlsx](memory_profiles/test_xlsx_openpyxl.png "pyxl xlsx")|
|![fastexcel formulas xlsx](memory_profiles/test_xlsx_formulas_fastexcel.png "fastexcel formulas xlsx") |![pyxl formulas xlsx](memory_profiles/test_xlsx_formulas_openpyxl.png "pyxl formulas xlsx")|
