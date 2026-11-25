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

On this release branch, the `file_append` source in `crates/test-framework`
is its only user: testoperator's append tests generate their TPC-H data with
the installed DuckDB, whose `dbgen` writes the rows `validation/tpch` answers
describe. `scripts/libduckdb.py` installs a checksum-verified release and
writes the manifest; CI runs it through `.github/actions/setup-libduckdb`.

SQL settings, ATTACH, transactions, INSTALL/LOAD, and TPC extension generation use
ordinary SQL over ADBC. Extensions must match the installed library version.
