#!/bin/bash -xe
rm -rf target/wheels/
maturin build --release
pip install --force-reinstall "$(echo target/wheels/*.whl)[pandas]"
