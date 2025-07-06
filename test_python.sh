#!/bin/bash
set -e

echo "Building Python module..."
rye run maturin build --release --features python

echo "Installing wheel..."
rye run python -m pip3 install --force-reinstall target/wheels/silk_chiffon-*.whl

echo "Running Python tests..."
rye test "$@"
