from time import time
import pandas as pd
import numpy as np
import pyarrow as pa
from pyarrow import csv
from pyarrow import feather

def create_table(since: str, stop: str, randRange: int) -> pa.Table:
    dates = np.arange(since, stop, dtype='datetime64[D]')
    nbdaySince = pa.array(np.arange(dates.size), type=pa.uint64())
    randoms = []
    names = []
    for i in range(randRange):
        names.append(f"randome{i}")
        randoms.append(pa.array(np.array(np.random.random(dates.size)), type=pa.float32()))

    return pa.table([dates, nbdaySince, *randoms], ['dates', 'day since', *names])


def main():
    print("creating dataset")
    table = create_table('2019', '2022', 100000)

    print("creating files :")
    print("- feather")
    feather.write_feather(table, 'bigfile.feather')
    
    print("- csv")
    csv.write_csv(table, 'bigfile.csv')

    print("start benchmarking")
    start = time()
    pd.read_feather("bigfile.feather")
    delta = time() - start

    print(f"feather took {delta} seconds to load")

    start = time()
    pd.read_csv("bigfile.csv")
    delta = time() - start

    print(f"csv took {delta} seconds to load")

if __name__ == "__main__":
    main()