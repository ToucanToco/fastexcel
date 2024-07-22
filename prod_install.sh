#!/bin/bash -xe
rm -rf target/wheels/
maturin build --release
uv pip install --force-reinstall "$(echo target/wheels/*.whl)[pandas, polars]"
