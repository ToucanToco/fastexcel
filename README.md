# `fastexcel`

A fast excel file reader for Python, written in Rust.

Based on [`calamine`](https://github.com/tafia/calamine) and [Apache Arrow](https://arrow.apache.org/).

Docs available [here](https://fastexcel.toucantoco.dev/).

## Dev setup

### Prerequisites

Python>=3.8 and a recent Rust toolchain must be installed on your machine. `cargo` must be available in your `PATH`.

### First setup

On the very first time you setup the project, you'll need to create a virtualenv and install the necessary tools:

```console
python -m venv .venv
source .venv/bin/activate
(.venv) make dev-setup
```

This will also set up [pre-commit](https://pre-commit.com/).

### Installing the project in dev mode

In order to install the project in dev mode (for local tests for example), use `make dev-install`.
This will compile the wheel (in debug mode) and install it. It will then be available in your venv.

### Installing the project in prod mode

This is required for profiling, as dev mode wheels are much slower. `make prod-install` will compile the project
in release mode and install it in your local venv, overriding previous dev installs.

### Linting and formatting

The Makefile provides the `lint` and `format` extras to ease this.

## Running the tests

`make test`

## Running the benchmarks

### Speed benchmark

`make benchmarks`

### Memory benchmark

`mprof run -T 0.01 python python/tests/benchmarks/memory.py python/tests/benchmarks/fixtures/plain_data.xls`

## Building the docs

`make doc`

## Creating a release

1. Create a PR containing a commit that only updates the version in `Cargo.toml`.
2. Once it is approved, squash and merge it into main.
3. Tag the squashed commit, and push it.
4. The `release` GitHub action will take care of the rest.

## Dev tips

* Use `cargo check` to verify that your rust code compiles, no need to go through `maturin` every time
* `cargo clippy` = 💖
* Careful with arrow constructors, they tend to allocate a lot
* [`mprof`](https://github.com/pythonprofilers/memory_profiler) and `time` go a long way for perf checks,
  no need to go fancy right from the start
