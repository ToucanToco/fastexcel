.PHONY: lint format dev-setup dev-install prod-install

# Commands
## Python
flake8	= flake8 python/fastexcel *.py
isort	= isort python/fastexcel *.py
black	= black python/fastexcel *.py
mypy	= mypy python/fastexcel *.py
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

dev-setup:
	pip install -U maturin -r test-requirements.txt
	pre-commit install

dev-install:
	maturin develop -E pandas

prod-install:
	./prod_install.sh
