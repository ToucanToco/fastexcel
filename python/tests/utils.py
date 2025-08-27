from pathlib import Path


def path_for_fixture(fixture_file: str) -> str:
    return str(Path(__file__).parent.parent.parent / "tests" / "fixtures" / fixture_file)
