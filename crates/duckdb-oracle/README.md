# DuckDB test oracle

This developer-only helper loads an installed libduckdb (minimum 1.5.0) through
ADBC and returns Arrow RecordBatches without Rust row decoding. It does not build
or bundle DuckDB. Missing or unsupported libraries fail tests.

Install a DuckDB shared-library release and create `duckdb.toml` in a directory
listed in `ADBC_DRIVER_PATH`:

```toml
[Driver]
shared = "/absolute/path/to/libduckdb.dylib" # libduckdb.so on Linux
entrypoint = "duckdb_adbc_init"
```

Run the Cayenne oracle suites with:

```sh
ADBC_DRIVER_PATH=/path/to/manifests cargo test -p cayenne --features result-correctness-duckdb --test integration result_correctness
```

SQL settings, ATTACH, transactions, INSTALL/LOAD, and TPC extension generation use
ordinary SQL over ADBC. Extensions must match the installed library version.
