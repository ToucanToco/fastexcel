from enum import Enum
import argparse
from readers import fastexcel_read, pyxl_read, xlrd_read


class Engine(str, Enum):
    FASTEXCEL = "fastexcel"
    XLRD = "xlrd"
    OPENPYXL = "pyxl"


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--engine", default=Engine.FASTEXCEL)
    parser.add_argument("file")
    return parser.parse_args()


def main():
    args = get_args()
    match args.engine:
        case Engine.FASTEXCEL:
            fastexcel_read(args.file)
        case Engine.XLRD:
            xlrd_read(args.file)
        case Engine.OPENPYXL:
            pyxl_read(args.file)


if __name__ == "__main__":
    main()
