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
cargo-test	= cargo test
## Docs
pdoc	= pdoc -o docs python/fastexcel

lint:
	$(ruff)
	$(format)  --check --diff
	$(mypy)
	$(clippy)
format:
	$(ruff) --fix
	$(format)
	$(fmt)
	$(clippy) --fix --lib -p fastexcel --allow-dirty --allow-staged

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

test:
	$(cargo-test)
	$(pytest)

doc:
	$(pdoc)

test-ci: dev-install test

benchmarks: prod-install
	pytest ./python/tests/benchmarks/speed.py
