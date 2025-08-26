#!/bin/bash -xe
rm -rf target/wheels/
maturin build --release --all-features
uv pip install --force-reinstall "$(echo target/wheels/*.whl)[pandas, polars]"
