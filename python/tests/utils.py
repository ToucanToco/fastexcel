from os.path import dirname
from os.path import join as path_join


def path_for_fixture(fixture_file: str) -> str:
    return path_join(dirname(__file__), "fixtures", fixture_file)
