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

### Export

Export the Spicepod environment that would run for a test.

```sh
testoperator export [OPTIONS]
```

#### Options

- `-p, --spicepod-path <SPICEPOD_PATH>`: Path to the `spicepod.yaml` file.
- `-s, --spiced-path <SPICED_PATH>`: Path to the `spiced` binary.
- `-d, --data-dir <DATA_DIR>`: An optional data directory to symlink into the `spiced` instance.

## Examples

### Running a TPCH Throughput Test on the File Connector

```sh
testoperator run -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data
```

### Exporting the Spicepod Environment for the File Connector TPCH test

```sh
testoperator export -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data
```

### Using a non-system wide spiced binary path

```sh
testoperator run -p spicepod.yaml -s ./target/debug/spiced
```
