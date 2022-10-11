import argparse

import fastexcel


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    return parser.parse_args()


def main():
    args = get_args()
    dfs = list(fastexcel.load_excel_file(args.file))


if __name__ == "__main__":
    main()
