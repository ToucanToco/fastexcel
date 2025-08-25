# `fastexcel`

A fast excel file reader for Python, written in Rust.

Based on [`calamine`](https://github.com/tafia/calamine) and [Apache Arrow](https://arrow.apache.org/).

Docs available [here](https://fastexcel.toucantoco.dev/).

## Installation

```bash
# Lightweight installation (no pyarrow dependency)
pip install fastexcel

# With Polars support only (no pyarrow needed)
pip install fastexcel[polars]

# With pandas support (includes pyarrow)
pip install fastexcel[pandas]

# With pyarrow support
pip install fastexcel[pyarrow]

# With all integrations
pip install fastexcel[pandas,polars]
```

## Quick Start

### Modern usage (recommended)

FastExcel supports the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) for zero-copy data exchange with libraries like Polars, without requiring pyarrow as a dependency.
Use fastexcel with any Arrow-compatible library without requiring pyarrow.

```python
import fastexcel

# Load an Excel file
reader = fastexcel.read_excel("data.xlsx")
sheet = reader.load_sheet(0)  # Load first sheet

# Use with Polars (zero-copy, no pyarrow needed)
import polars as pl
df = pl.DataFrame(sheet)  # Direct PyCapsule interface
print(df)

# Or use the to_polars() method (also via PyCapsule)
df = sheet.to_polars()
print(df)

# Or access the raw Arrow data via PyCapsule interface
schema = sheet.__arrow_c_schema__()
array_data = sheet.__arrow_c_array__()
```

### Traditional usage (with pandas/pyarrow)

```python
import fastexcel

reader = fastexcel.read_excel("data.xlsx")
sheet = reader.load_sheet(0)

# Convert to pandas (requires `pandas` extra)
df = sheet.to_pandas()

# Or get pyarrow RecordBatch directly
record_batch = sheet.to_arrow()
```

### Working with tables

```python
reader = fastexcel.read_excel("data.xlsx")

# List available tables
tables = reader.table_names()
print(f"Available tables: {tables}")

# Load a specific table
table = reader.load_table("MyTable")
df = pl.DataFrame(table)  # Zero-copy via PyCapsule, no pyarrow needed
```

## Key Features

- **Zero-copy data exchange** via [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
- **Flexible dependencies** - use with Polars (no PyArrow needed) or Pandas (includes PyArrow)
- **Seamless Polars integration** - `pl.DataFrame(sheet)` and `sheet.to_polars()` work without PyArrow via PyCapsule interface
- **High performance** - written in Rust with [calamine](https://github.com/tafia/calamine) and [Apache Arrow](https://arrow.apache.org/)
- **Memory efficient** - lazy loading and optional eager evaluation
- **Type safety** - automatic type inference with manual override options

## Dev setup

### Prerequisites

Python>=3.9 and a recent Rust toolchain must be installed on your machine. `cargo` must be available in your `PATH`.

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
