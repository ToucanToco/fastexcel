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

    return parser.parse_args()


def main():
    args = get_args()
    excel_file = fastexcel.read_excel(args.file)
    use_columns = args.column or None

    for _ in range(args.iterations):
        for sheet_name in excel_file.sheet_names:
            if args.eager:
                excel_file.load_sheet_eager(sheet_name, use_columns=use_columns)
            else:
                excel_file.load_sheet(sheet_name, use_columns=use_columns).to_arrow()


if __name__ == "__main__":
    main()
