import argparse

import pandas as pd

import fastexcel

pd.set_option("display.max_columns", 500)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    args = parser.parse_args()
    dfs = fastexcel.load_excel_file(args.file)
    for df in dfs:
        print(df.head(5))


if __name__ == "__main__":
    main()
