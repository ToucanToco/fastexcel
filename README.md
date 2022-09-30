# How to launch

First, install [`maturin`](https://github.com/PyO3/maturin).

For debug mode, you can just use `maturin develop` and run `python` (or `ipython`) to start a python shell.

For prod mode (required for profiling):

```shell
maturin build --release
pip install --force-reinstall target/wheels/fastexcel-*.whl
```

# Dev tips

* Use `cargo check` to verify that your rust code compiles, no need to go through `maturin` every time
* `cargo clippy` = 💖
* Careful with arrow constructors, they tend to allocate a lot
* [`mprof`](https://github.com/pythonprofilers/memory_profiler) and `time` go a long way for perf checks,
  no need to go fancy right from the start
