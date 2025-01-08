# testoperator

## Overview

`testoperator` is a command-line tool for running and exporting Spicepod environments for testing purposes.

## Commands

### Run

Run a throughput test using the specified Spicepod.

```sh
testoperator run [OPTIONS]
```

#### Options

- `-p, --spicepod-path <SPICEPOD_PATH>`: Path to the `spicepod.yaml` file.
- `-s, --spiced-path <SPICED_PATH>`: Path to the `spiced` binary.
- `-d, --data-dir <DATA_DIR>`: An optional data directory to symlink into the `spiced` instance.
- `--scale-factor <SCALE_FACTOR>`: The expected scale factor for the test, used in metrics calculation.
- `--duration <DURATION>`: The duration of the test in seconds.
- `--query-set <QUERY_SET>`: The query set to use for the test. Possible values: `tpch`, `tpcds`, `clickbench`.
- `--query-overrides <QUERY_OVERRIDES>`: Optional query overrides. Possible values: `sqlite`, `postgresql`, `mysql`, `dremio`, `spark`, `odbcathena`, `duckdb`.
- `--concurrency <CONCURRENCY>`: The concurrency level for the test.

### Export

Export the Spicepod environment that would run for a test.

```sh
testoperator export [OPTIONS]
```

#### Options

- `-p, --spicepod-path <SPICEPOD_PATH>`: Path to the `spicepod.yaml` file.
- `-s, --spiced-path <SPICED_PATH>`: Path to the `spiced` binary.
- `-d, --data-dir <DATA_DIR>`: An optional data directory to symlink into the `spiced` instance.
- `--scale-factor <SCALE_FACTOR>`: The expected scale factor for the test, used in metrics calculation.
- `--duration <DURATION>`: The duration of the test in seconds.
- `--query-set <QUERY_SET>`: The query set to use for the test. Possible values: `tpch`, `tpcds`, `clickbench`.
- `--query-overrides <QUERY_OVERRIDES>`: Optional query overrides. Possible values: `sqlite`, `postgresql`, `mysql`, `dremio`, `spark`, `odbcathena`, `duckdb`.
- `--concurrency <CONCURRENCY>`: The concurrency level for the test.

## Examples

### Running a TPCH Throughput Test on the File Connector

```sh
testoperator run throughput -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data --query-set tpch
```

### Exporting the Spicepod Environment for the File Connector TPCH test

```sh
testoperator export throughput -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data --query-set tpch
```

### Using a non-system wide spiced binary path

```sh
testoperator run throughput -p spicepod.yaml -s ./target/debug/spiced --query-set tpch
```
