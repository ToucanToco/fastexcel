.PHONY: lint format dev-setup dev-install prod-install test install-test-requirements benchmark

# Commands
## Python
ruff	= ruff python/fastexcel *.py
black	= black python/fastexcel *.py
mypy	= mypy python/fastexcel *.py
pytest	= python -m pytest
## Rust
clippy	= cargo clippy
fmt	= cargo fmt
## Docs
pdoc	= pdoc -o docs python/fastexcel

lint:
	$(ruff)
	$(black)  --check --diff
	$(mypy)
	$(clippy)
format:
	$(black)
	$(ruff) --fix
	$(fmt)

install-test-requirements:
	pip install -U maturin -r test-requirements.txt

install-doc-requirements:
	pip install -r doc-requirements.txt

dev-setup: install-test-requirements install-doc-requirements
	pre-commit install

dev-install:
	maturin develop -E pandas,polars

prod-install:
	./prod_install.sh

test:
	$(pytest)

doc:
	$(pdoc)

test-ci: dev-install test

benchmark: prod-install
	pytest ./python/tests/benchmarks/benchmark.py
