import argparse

import fastexcel


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    return parser.parse_args()


def main():
    args = get_args()
    excel_file = fastexcel.read_excel(args.file)
    for sheet_name in excel_file.sheet_names:
        excel_file.load_sheet_by_name(sheet_name).to_pandas()


if __name__ == "__main__":
    main()
