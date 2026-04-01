#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
"""Manage docs/versions.json and generate the root docs/index.html redirect."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def parse_semver(version: str) -> tuple[int, ...]:
    """Extract numeric parts from a version string like 'v0.19.0'."""
    return tuple(int(x) for x in re.findall(r"\d+", version))


def sort_versions(versions: list[dict]) -> list[dict]:
    """Sort: stable first, then tags descending by semver, 'latest' last."""
    def sort_key(v: dict) -> tuple[int, tuple[int, ...], str]:
        path = v["path"]
        if v.get("stable"):
            return (0, (), "")
        if path == "latest":
            return (2, (), "")
        return (1, tuple(-x for x in parse_semver(path)), path)

    return sorted(versions, key=sort_key)


def update_versions(docs_dir: Path, version: str, *, stable: bool) -> None:
    versions_file = docs_dir / "versions.json"

    if versions_file.exists():
        versions = json.loads(versions_file.read_text())
    else:
        versions = []

    # Build label
    if version == "latest":
        label = "latest (main)"
    elif stable:
        label = f"{version} (stable)"
    else:
        label = version

    # Remove old entry for this version, and clear stable flag from others if
    # this one is now stable
    new_versions = []
    for v in versions:
        if v["path"] == version:
            continue
        if stable and v.get("stable"):
            v = {**v, "stable": False, "label": v["path"]}
        new_versions.append(v)

    new_versions.append({"label": label, "path": version, "stable": stable})
    new_versions = sort_versions(new_versions)

    versions_file.write_text(json.dumps(new_versions, indent=2) + "\n")

    # Generate root redirect
    stable_entry = next((v for v in new_versions if v.get("stable")), None)
    redirect_path = stable_entry["path"] if stable_entry else version
    index_html = docs_dir / "index.html"
    index_html.write_text(
        f"""\
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="0; url=./{redirect_path}/fastexcel.html"/>
</head>
<body>
    <p>Redirecting to <a href="./{redirect_path}/fastexcel.html">{redirect_path} documentation</a>...</p>
</body>
</html>
"""
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Update docs versions.json")
    parser.add_argument("--version", required=True, help="Version name (e.g. v0.19.0 or latest)")
    parser.add_argument("--stable", action="store_true", help="Mark this version as the stable default")
    parser.add_argument("--docs-dir", default="docs", help="Path to the docs directory")
    args = parser.parse_args()

    update_versions(Path(args.docs_dir), args.version, stable=args.stable)


if __name__ == "__main__":
    main()
