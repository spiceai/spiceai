# testoperator

## Overview

`testoperator` is a command-line tool for running and exporting Spicepod environments for testing purposes.

## Commands

### Run

Run a test using the specified Spicepod.

```sh
testoperator run [COMMAND] [OPTIONS]
```

#### Commands

- `throughput`: Run a throughput test.
- `load`: Run a load test.
- `bench`: Run a benchmark test.
- `data-consistency`: Run a data consistency test.

### Export

Export the Spicepod environment that would run for a test.

```sh
testoperator export [COMMAND] [OPTIONS]
```

#### Commands

- `throughput`: Export the environment for a throughput test.
- `load`: Export the environment for a load test.
- `bench`: Export the environment for a benchmark test.
- `data-consistency`: Export the environment for a data consistency test.

### Common Options

- `-p, --spicepod-path <SPICEPOD_PATH>`: Path to the `spicepod.yaml` file.
- `-s, --spiced-path <SPICED_PATH>`: Path to the `spiced` binary.
- `-d, --data-dir <DATA_DIR>`: An optional data directory to symlink into the `spiced` instance.
- `--query-set <QUERY_SET>`: The query set to use for the test. Possible values: `tpch`, `tpcds`, `clickbench`.
- `--query-overrides <QUERY_OVERRIDES>`: Optional query overrides. Possible values: `sqlite`, `postgresql`, `mysql`, `dremio`, `spark`, `odbcathena`, `duckdb`.
- `--concurrency <CONCURRENCY>`: The concurrency level for the test.
- `--ready-wait <WAIT TIME>`: How long to wait before spiced is ready.
- `--disable-progress-bars`: Disable progress bars during the test.

### Specific Options

#### Throughput and Load Tests

- `--scale-factor <SCALE_FACTOR>`: The expected scale factor for the test, used in metrics calculation.
- `--duration <DURATION>`: The duration of the test in seconds (only for load tests).

#### Data Consistency Test

- `--compare-spicepod <SPICEPOD_PATH>`: Path to a `spicepod.yaml` file to compare in a data consistency test.

### Examples

#### Running a TPCH Throughput Test on the File Connector

```sh
testoperator run throughput -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data --query-set tpch
```

#### Exporting the Spicepod Environment for the File Connector TPCH Test

```sh
testoperator export throughput -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data --query-set tpch
```

#### Using a Non-System Wide Spiced Binary Path

```sh
testoperator run throughput -p spicepod.yaml -s ./target/debug/spiced --query-set tpch
```
