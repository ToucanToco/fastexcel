.PHONY: lint format dev-setup dev-install prod-install test install-test-requirements

# Commands
## Python
flake8	= flake8 python/fastexcel *.py
isort	= isort python/fastexcel *.py
black	= black python/fastexcel *.py
mypy	= mypy python/fastexcel *.py
pytest	= python -m pytest
## Rust
clippy	= cargo clippy
fmt	= cargo fmt

lint:
	$(flake8)
	$(isort)  --check-only --df
	$(black)  --check --diff
	$(mypy)
	$(clippy)
format:
	$(black)
	$(isort)
	$(fmt)

install-test-requirements:
	pip install -U maturin -r test-requirements.txt

dev-setup: install-test-requirements
	pre-commit install

dev-install:
	maturin develop -E pandas

prod-install:
	./prod_install.sh

test:
	$(pytest)

test-ci: dev-install test
