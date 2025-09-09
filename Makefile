.DEFAULT_GOAL := all
sources = python/fastexcel python/tests

export CARGO_TERM_COLOR=$(shell (test -t 0 && echo always) || echo auto)

.PHONY: .uv  ## Check that uv is installed
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: install  ## Install the package & dependencies with debug build
install: .uv
	uv sync --frozen --group all
	uv run maturin develop --uv -E pyarrow,pandas,polars

.PHONY: install-prod  ## Install the package & dependencies with release build
install-prod: .uv
	uv sync --frozen --group all
	uv run maturin develop --uv --release -E pyarrow,pandas,polars

.PHONY: setup-dev  ## First-time setup: install + pre-commit hooks
setup-dev: install
	uv run pre-commit install --install-hooks

.PHONY: rebuild-lockfiles  ## Rebuild lockfiles from scratch, updating all dependencies
rebuild-lockfiles: .uv
	uv lock --upgrade
	cargo update

.PHONY: build-dev  ## Build the development version of the package
build-dev:
	uv run maturin build

.PHONY: build-wheel  ## Build production wheel and install it
build-wheel:
	@rm -rf target/wheels/
	uv run maturin build --release
	@wheel=$$(ls target/wheels/*.whl); uv pip install --force-reinstall "$$wheel[pandas,polars]"

.PHONY: lint-python  ## Lint python source files
lint-python:
	uv run ruff check $(sources)
	uv run ruff format --check $(sources)
	uv run mypy $(sources)

.PHONY: lint-rust  ## Lint rust source files
lint-rust:
	cargo fmt --all -- --check
	# Rust
	cargo clippy --tests
	# Python-related code
	cargo clippy --features __maturin --tests
	# Rust+polars
	cargo clippy --features polars --tests

.PHONY: lint  ## Lint rust and python source files
lint: lint-python lint-rust

.PHONY: format-python  ## Auto-format python source files
format-python:
	uv run ruff check --fix $(sources)
	uv run ruff format $(sources)

.PHONY: format-rust  ## Auto-format rust source files
format-rust:
	cargo fmt --all
	cargo clippy --all-features --tests --fix --lib -p fastexcel --allow-dirty --allow-staged

.PHONY: format  ## Auto-format python and rust source files
format: format-rust format-python

.PHONY: test-python  ## Run python tests
test-python: install
	uv run pytest

.PHONY: test-rust-pyo3  ## Run PyO3 rust tests
test-rust-pyo3:
	# --lib to skip integration tests
	cargo test --no-default-features --features __pyo3-tests --lib

.PHONY: test-rust-standalone  ## Run standalone rust tests
test-rust-standalone:
	cargo test --no-default-features --features __rust-tests-standalone

.PHONY: test-rust-polars  ## Run polars rust tests
test-rust-polars:
	cargo test --no-default-features --features __rust-tests-polars

.PHONY: test-rust  ## Run rust tests
test-rust: test-rust-pyo3 test-rust-standalone test-rust-polars

.PHONY: test  ## Run all tests
test: test-rust test-python

.PHONY: doc-serve  ## Serve documentation with live reload
doc-serve: build-dev
	uv run pdoc python/fastexcel

.PHONY: doc  ## Build documentation
doc: build-dev
	uv run pdoc -o docs python/fastexcel
	cargo doc --no-deps --lib -p fastexcel --features polars

.PHONY: all  ## Run the standard set of checks performed in CI
all: format build-dev lint test

.PHONY: benchmarks  ## Run benchmarks
benchmarks: build-wheel
	uv run pytest ./python/tests/benchmarks/speed.py

.PHONY: clean  ## Clear local caches and build artifacts
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
	rm -rf htmlcov
	rm -rf .pytest_cache
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -rf build
	rm -rf perf.data*
	rm -rf python/fastexcel/*.so

.PHONY: help  ## Display this message
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-19s\033[0m %s\n", $$2, $$3}'
