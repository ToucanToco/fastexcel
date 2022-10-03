import argparse

import fastexcel


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("--lazy", action="store_true")
    return parser.parse_args()


def main():
    args = get_args()
    if args.lazy:
        dfs = list(fastexcel.load_excel_file_lazy(args.file))
    else:
        dfs = fastexcel.load_excel_file(args.file)


if __name__ == "__main__":
    main()
