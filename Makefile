.PHONY: lint format dev-setup dev-install prod-install test install-test-requirements

# Commands
## Python
ruff	= ruff python/fastexcel *.py
black	= black python/fastexcel *.py
mypy	= mypy python/fastexcel *.py
pytest	= python -m pytest
## Rust
clippy	= cargo clippy
fmt	= cargo fmt

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

dev-setup: install-test-requirements
	pre-commit install

dev-install:
	maturin develop -E pandas

prod-install:
	./prod_install.sh

test:
	$(pytest)

test-ci: dev-install test
