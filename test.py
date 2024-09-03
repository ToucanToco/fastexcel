#!/usr/bin/env python3
import argparse

import fastexcel


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("-c", "--column", type=str, nargs="+", help="the columns to use")
    parser.add_argument(
        "--eager", action="store_true", help="wether the sheet should be loaded eagerly"
    )
    parser.add_argument(
        "-i", "--iterations", type=int, help="the number of iterations to do", default=1
    )
    parser.add_argument("-t", "--table", type=str, help="the name of the table to load")
    parser.add_argument(
        "--print-tables", action="store_true", help="whether to print the tables in the file"
    )

    return parser.parse_args()


def main():
    args = get_args()
    excel_file = fastexcel.read_excel(args.file)
    use_columns = args.column or None

    if args.print_tables:
        table_names = excel_file.table_names()
        if len(table_names) > 0:
            print(f"Available tables are {', '.join(table_names)}")
        else:
            print("No tables found")

    for _ in range(args.iterations):
        if args.table:
            tbl = excel_file.load_table(args.table)
            print(f"Found table {args.table}:")
            print(tbl.to_polars())
        else:
            for sheet_name in excel_file.sheet_names:
                if args.eager:
                    excel_file.load_sheet_eager(sheet_name, use_columns=use_columns)
                else:
                    excel_file.load_sheet(sheet_name, use_columns=use_columns).to_arrow()


if __name__ == "__main__":
    main()
