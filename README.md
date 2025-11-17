# `fastexcel`

A fast excel file reader for Python and Rust.

Docs:
 * [Python](https://fastexcel.toucantoco.dev/).
 * [Rust](https://docs.rs/fastexcel).

## Stability

The Python library is considered production-ready. The API is mostly stable, and we avoid breaking changes as much as
possible. v1.0.0 will be released once the [milestone](https://github.com/ToucanToco/fastexcel/milestone/2) is reached.

> ⚠️ The free-threaded build is still considered experimental

The Rust crate is still experimental, and breaking changes are to be expected.

## Installation

```bash
# Lightweight installation (no PyArrow dependency)
pip install fastexcel

# With Polars support only (no PyArrow needed)
pip install fastexcel[polars]

# With Pandas support (includes PyArrow)
pip install fastexcel[pandas]

# With PyArrow support
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

## Contributing & Development

### Prerequisites

You'll need:
1. **[Rust](https://rustup.rs/)** - Rust stable or nightly
2. **[uv](https://docs.astral.sh/uv/getting-started/installation/)** - Fast Python package manager (will install Python 3.10+ automatically)
3. **[git](https://git-scm.com/)** - For version control
4. **[make](https://www.gnu.org/software/make/)** - For running development commands

**Python Version Management:**
uv handles Python installation automatically. To use a specific Python version:
```bash
uv python install 3.13  # Install Python 3.13
uv python pin 3.13      # Pin project to Python 3.13
```

### Quick Start

```bash
# Clone the repository (or from your fork)
git clone https://github.com/ToucanToco/fastexcel.git
cd fastexcel

# First-time setup: install dependencies, build debug version, and setup pre-commit hooks
make setup-dev
```

Verify your installation by running:

```bash
make
```

This runs a full development cycle: formatting, building, linting, and testing

### Development Commands

Run `make help` to see all available commands, or use these common ones:

```bash
make all          # full dev cycle: format, build, lint, test
make install      # install with debug build (daily development)
make install-prod # install with release build (benchmarking)
make test         # to run the tests
make lint         # to run the linter
make format       # to format python and rust code
make doc-serve    # to serve the documentation locally
```

### Useful Resources

* [`python/fastexcel/_fastexcel.pyi`](./python/fastexcel/_fastexcel.pyi) - Python API types
* [`python/tests/`](./python/tests) - Comprehensive usage examples

## Benchmarking

For benchmarking, use `make benchmarks` which automatically builds an optimised wheel.
This is required for profiling, as dev mode builds are much slower.

### Speed benchmarks
```bash
make benchmarks
```

### Memory profiling
```bash
mprof run -T 0.01 python python/tests/benchmarks/memory.py python/tests/benchmarks/fixtures/plain_data.xls
```

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
