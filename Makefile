.PHONY: lint format dev-setup dev-install prod-install test install-test-requirements benchmarks

# Commands
## Python
ruff	= ruff python/ *.py
format	= ruff format python/ *.py
mypy	= mypy python/ *.py
pytest	= pytest -v
## Rust
clippy		= cargo clippy
fmt		= cargo fmt
cargo-test	= cargo test --no-default-features --features tests
## Docs
pdoc	= pdoc -o docs python/fastexcel

lint-python:
	$(ruff)
	$(format)  --check --diff
	$(mypy)

lint-rust:
	$(clippy)

lint: lint-rust lint-python

format-python:
	$(ruff) --fix
	$(format)

format-rust:
	$(fmt)
	$(clippy) --fix --lib -p fastexcel --allow-dirty --allow-staged

format: format-rust format-python

install-test-requirements:
	pip install -U -r test-requirements.txt -r build-requirements.txt

install-doc-requirements:
	pip install -r doc-requirements.txt

dev-setup: install-test-requirements install-doc-requirements
	pre-commit install

dev-install:
	maturin develop -E pandas,polars

prod-install:
	./prod_install.sh

test-rust:
	$(cargo-test)

test-python:
	$(pytest)

test: test-rust test-python

doc:
	$(pdoc)

test-ci: dev-install test

benchmarks: prod-install
	pytest ./python/tests/benchmarks/speed.py
