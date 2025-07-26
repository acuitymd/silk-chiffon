build:
    uv run maturin build --release --features python

# build -> install -> test
test: install
    uv run pytest

# build -> install
install: build
    uv run python -m pip install --force-reinstall target/wheels/silk_chiffon-*.whl

type-check:
    uv run pyright

fmt-check:
    uv run ruff format --check

fmt-fix:
    uv run ruff format

lint-check:
    uv run ruff check

lint-fix:
    uv run ruff check --fix
