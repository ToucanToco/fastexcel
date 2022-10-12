# `fastexcel`

A fast excel file reader for Python, written in Rust.

Based on [`calamine`](https://github.com/tafia/calamine) and [Apache Arrow](https://arrow.apache.org/).

## Dev setup

### Prerequisites

Python>=3.10 and a recent Rust toolchain must be installed on your machine. `cargo` must be available in your `PATH`.

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

### Installing the project in dev mode

This is required for profiling, as dev mdoe wheels are much slower. `make prod-install` will compile the project
in release mode and install it in your local venv, overriding previous dev installs.

### Linting and formatting

The Makefile provides the `lint` and `format` extras to ease this.

## Dev tips

* Use `cargo check` to verify that your rust code compiles, no need to go through `maturin` every time
* `cargo clippy` = 💖
* Careful with arrow constructors, they tend to allocate a lot
* [`mprof`](https://github.com/pythonprofilers/memory_profiler) and `time` go a long way for perf checks,
  no need to go fancy right from the start
