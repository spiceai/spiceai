# testoperator

## Overview

`testoperator` is a command-line tool for running and exporting Spicepod environments for testing purposes.

While a test is executing, `testoperator` continuously probes the `/health` and `/v1/ready` endpoints on the running `spiced` instance. Responses that fail or take longer than 5 ms are recorded and surfaced after the test run; any such issues will cause the test to fail with a summary that includes the number of failures and the worst latency observed.

## Common Options

- `-p, --spicepod-path <SPICEPOD_PATH>`: Path to the `spicepod.yaml` file.
- `-s, --spiced-path <SPICED_PATH>`: Path to the `spiced` binary.
- `-d, --data-dir <DATA_DIR>`: An optional data directory to symlink into the `spiced` instance.
- `--ready-wait <WAIT TIME>`: How long to wait before spiced is ready.
- `--disable-progress-bars`: Disable progress bars during the test.

## Use cases

### Running Benchmarks

Run standard benchmarks using the `testoperator run bench [OPTIONS]` command. In addition to the common options, this command supports the following options:

- `--query-set <QUERY_SET>`: The query set to use for the test. Possible values: `tpch`, `tpcds`, `clickbench`, `tpch[parameterized]`, `integration[http]`, `scenario`.
- `--scenario-query-file <FILE_PATH>`: Path to a YAML file containing custom scenario queries. Required when `--query-set scenario` is specified.
- `--query-overrides <QUERY_OVERRIDES>`: Optional query overrides. Possible values: `sqlite`, `postgresql`, `mysql`, `dremio`, `spark`, `odbcathena`, `duckdb`.
- `--scale-factor <SCALE_FACTOR>`: The expected scale factor for the test, used in metrics calculation.
- `--validate`: A boolean flag to specify whether results should be validated against their expected results. Supported for `tpch`, `tpch[parameterized]` (scale factor 1 only), and `scenario` query sets (when expected results are defined in the scenario file).
- `--metrics`: Whether to upload metrics to the Spice OSS benchmarks dashboards. By default, submits to the Production metrics endpoint using the API key specified in the `SPICEAI_BENCHMARK_METRICS_KEY` environment variable. If specified, the metrics delivery endpoint can be overridden with the `SPICEAI_TELEMETRY_ENDPOINT` environment variable.
- `--disable-caching`: Whether to disable results cache by supplying a `Cache-Control: no-cache` header over the Flight request. Allows disabling results cache separately from spicepod configuration.

Running a benchmark test will always generate snapshots for the query explain plan and results for `tpch` and `tpcds` queries. Only explain plans will be generated for `clickbench` queries.

Snapshots can be automatically re-generated using the [`INSTA_UPDATE`](https://docs.rs/insta/latest/insta/#updating-snapshots) environment variable.

`testoperator run bench [OPTIONS]`

#### Benchmark Test Examples

##### Run the federated DuckDB spicepod with validation

This assumes that the provided `-d` data directory contains the required data file specified in the spicepod. In this case, `./.data/tpch.db`.

```sh
testoperator run bench -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --validate
```

or:

```sh
cargo run -p testoperator -- run bench -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --validate
```

##### Run the Postgres spicepod with an override

Because PostgreSQL uses a server and requires no local files, the `-d` data directory value can be omitted.

```sh
testoperator run bench -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced --query-set tpch --query-overrides postgresql --validate
```

or:

```sh
cargo run -p testoperator -- run bench -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced --query-set tpch --query-overrides postgresql --validate
```

##### Run a custom scenario query set with validation

Scenario query sets allow you to define custom queries in a YAML file. This is useful for ad-hoc testing or when you need custom validation that doesn't fit the standard integration test pattern.

```sh
testoperator run bench \
  -p test/spicepods/http/post_requests.yaml \
  -s spiced \
  --query-set scenario \
  --scenario-query-file test/scenario/http/post_requests.yaml \
  --validate
```

The scenario query file format:

```yaml
name: my_custom_queries # Optional name for the query set

queries:
  # Query without validation
  - name: basic_select
    sql: SELECT * FROM my_table

  # Query with row count validation
  - name: count_check
    sql: SELECT COUNT(*) FROM my_table
    expected_results:
      row_count: 100

  # Query with inline expected results
  - name: specific_values
    sql: SELECT id, name FROM users ORDER BY id LIMIT 2
    expected_results:
      columns: 'id, name'
      rows:
        - '1, Alice'
        - '2, Bob'

  # Query with external CSV file validation
  # - name: full_dataset
  #   sql: SELECT * FROM my_table ORDER BY id
  #   expected_results: ./expected/full_dataset.csv
```

For more examples, see `test/spicepods/http/queries.yaml`.

### Running Throughput Tests

A throughput test replicates a benchmark test, but runs with multiple concurrent query executors. A throughput test uses the same command options as a benchmark test, with the additional options:

- `--concurrency <CONCURRENCY>`: The number of concurrent query workers to execute the test.

A throughput test can specify both `--validate` and `--metrics`, but the effects of these options are ignored. Throughput tests disable snapshotting functionality, and do not support data validation.

`testoperator run throughput [OPTIONS]`

#### Throughput Test Examples

##### Run the federated DuckDB spicepod with 25 concurrent workers

This assumes that the provided `-d` data directory contains the required data file specified in the spicepod. In this case, `./.data/tpch.db`.

```sh
testoperator run throughput -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --concurrency 25
```

#### Running a TPCH Throughput Test on the File Connector

```sh
testoperator run throughput -p ./benchmarks/file_tpch.yaml -s spiced -d ./.data --query-set tpch
```

### Running Load Tests

A load test replicates a throughput test, but instead of running for a set number of query executions (2 by default for throughput tests) load tests run for a specified duration. A load test uses the same command options as a throughput test, with the additional options:

- `--duration <SECONDS>`: The duration of the load test to run in seconds.

A load test will match the specified duration as a best-effort. A load test will never be shorter than the specified duration, but can be longer than the specified duration if there are running queries when the end duration is passed. For example, a `--duration 10` is specified but a query that takes 60 seconds runs. The load test will end after the query finishes, taking 60 seconds instead of 10.

Similar to throughput tests, load tests do not support validation, snapshotting, or metrics.

`testoperator run load [OPTIONS]`

#### Load Test Examples

##### Run the federated DuckDB spicepod with 8 concurrent workers for 10 minutes

This assumes that the provided `-d` data directory contains the required data file specified in the spicepod. In this case, `./.data/tpch.db`.

```sh
testoperator run load -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --concurrency 8 --duration 600
```

### Running Data Consistency tests

Data consistency tests support specifying two spicepods, and validating that the outputs of queries between the two match. This has been partially superseded by the functionality of `--validate`, but is still useful for testing between query sets that do not yet support the `--validate` option (like `tpcds` and `clickbench`).

A data consistency test supports the same options as a benchmark test, with the additional options:

- `--compare-spicepod <SPICEPOD_PATH>`: Path to a `spicepod.yaml` file to compare in a data consistency test.

Data consistency tests **do** support nested `--validate` options, as well as `--metrics` and snapshotting. Internally, the data consistency test runs two benchmark tests. This means that results from a data consistency test will emit metrics if the `--metrics` option is supplied with a valid API key in the environment variable.

`testoperator run data-consistency [OPTIONS]`

### Running HTTP Consistency tests

Runs a test to compare the latency performance of a HTTP enabled component as the component is persistently queried. In addition to the common options, supports specifying:

- `--embedding <MODEL NAME>`: The embedding model (named in spicepod) to test against. Cannot be used in conjunction with `model`.
- `--model <MODEL NAME>`: The language model (named in spicepod) to test against. Cannot be used in conjunction with `embedding`.
- `--payload_file <FILE NAME>`: The path to a file containing payloads to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload`.
- `--payload <PAYLOAD STRING>`: The payload to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload_file`.
- `--buckets <COUNT>`: The number of buckets to divide the test duration into. Defaults to `10`.
- `--warmup <SECONDS>`: How long to wait before collecting results, as a warmup period. Defaults to `0`.
- `--increase-threshold <RATE>`: The threshold for the increase in percentile latency between the first and last bucket of the test. Defaults to `1.1`.

`testoperator run http-consistency [OPTIONS]`

#### HTTP Consistency Test Examples

##### Run a HTTP consistency test against an embedding model

```sh
testoperator run http-consistency \
    --duration 300 \
    --buckets 5 \
    --embedding openai-ada \
    --payload "A nice string to embed" \
    --payload "{
        \"input\": \"The food was delicious and the waiter...\",
        \"model\": \"text-embedding-ada-002\",
        \"encoding_format\": \"float\"
      }"
```

Note: The `.model` field in the payload will be overridden.

##### Run a HTTP consistency test against an LLM model

```sh
testoperator run http-consistency \
    --duration 300 \
    --buckets 5 \
    --model openai-gpt5 \
    --payload-file payloads.txt  #Use JSONL-like format for JSON payloads
```

### Running HTTP Overhead tests

Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the duration of the test when N clients are sending queries concurrently. In addition to the common options, supports specifying:

- `--embedding <MODEL NAME>`: The embedding model (named in spicepod) to test against. Cannot be used in conjunction with `model`.
- `--model <MODEL NAME>`: The language model (named in spicepod) to test against. Cannot be used in conjunction with `embedding`.
- `--payload_file <FILE NAME>`: The path to a file containing payloads to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload`.
- `--payload <PAYLOAD STRING>`: The payload to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload_file`.
- `--increase-threshold <RATE>`: The threshold for the increase in percentile latency between the first and last bucket of the test. Defaults to `1.1`.
- `--base-url <URL>`: The base URL of the underlying HTTP service to test against.
- `--base-header <HEADERS>`: Headers to use when making requests to the base URL.
- `--base-component <COMPONENT NAME>`: If the component has a different name between the spicepod and the HTTP service, specify the name of the component in the HTTP service.
- `--base-payload-file <PAYLOAD FILE>`: The path to a file containing request body to use in testing the baseline component. Expects a request body compatible payloads. Cannot not be used in conjunction with `base_payload`.
- `--base-payload <PAYLOADS>`: The request body(s) to use in testing. Expects a request body compatible payloads.Cannot not be used in conjunction with `base_payload_file`.

`testoperator run http-overhead [OPTIONS]`

#### HTTP Overhead Test Examples

##### Run a HTTP overhead test against an embedding model

```sh
testoperator run http-overhead \
  --duration 10 \
  --embedding oai \
  --base-url "https://api.openai.com/v1" \
  --base-component "text-embedding-3-small" \
  --base-header "Content-Type: application/json" \
  --base-header "Authorization: Bearer $MY_OPENAI_API_KEY" \
  --payload "A nice string to embed" \
  --payload "{
      \"input\": \"The food was delicious and the waiter...\",
      \"model\": \"text-embedding-ada-002\",
      \"encoding_format\": \"float\"
    }"
```

##### Run a HTTP overhead test against an LLM model with incompatible API (e.g. Anthropic)

```sh
cargo run run http-overhead \
  --duration 10 \
  # These fields are for the spice component
  --model claude-tool \
  --payload "A nice string to embed" \
  --payload "{
      \"input\": \"The food was delicious and the waiter...\",
      \"model\": \"text-embedding-ada-002\",
      \"encoding_format\": \"float\"
    }" \

  # These fields are for the base/underlying component
  --base-url "https://api.anthropic.com/v1/messages" \
  --base-header "Content-Type: application/json" \
  --base-header "anthropic-version: 2023-06-01" \
  --base-header ""x-api-key: $ANTHROPIC_API_KEY" \
  --base-payload-file bodies.jsonl
```

Where `bodies.jsonl` might look like

```jsonl
{"model": "claude-3-5-sonnet-20241022","max_tokens": 1024,"messages": [{"role": "user", "content": "Hello, world"}]}
{"model": "claude-3-5-sonnet-20241022","max_tokens": 512,"messages": [{"role": "system", "content": "You are god"}, {"role": "user", "content": "Is god real?"}]}
```

### Running Evaluation tests

Run model evaluations (evals) test. In addition to the common options, supports specifying:

- `--model <MODEL NAME>`: The language model (as named in Spicepod) to test against. If not specified, the first model from the Spicepod definition will be used.
- `--eval <EVAL NAME>`: The eval name (as named in Spicepod) to test against. If not specified, the first eval from the Spicepod definition will be used.

`testoperator run evals [OPTIONS]`

### Running Vector Search Tests

Running search tests with the testoperator is still experimental, and uses statically defined tests within the command file. Vector search tests support the common options.

`testoperator run search [OPTIONS]`

### Running Append Tests

Append tests with the testoperator are similar to load tests, but operate with a changing data source over time to test the behavior of append-mode acceleration. Append tests support the same options as load tests. Append test sources are only supported for the file connector, but are supported for any `tpch`, `tpcds` or `clickbench` source.

Results validation, snapshotting and metrics are not supported with append tests.

Append tests are not built by default, as the File connector source generation relies on the `duckdb` crate to generate the source data. Because of this, the append test can significantly increase the testoperator build time. To build with append support, use the `append` feature flag: `cargo build -p testoperator --release --features append`.

### Other Examples

#### Using a Non-System Wide Spiced Binary Path

```sh
testoperator run throughput -p spicepod.yaml -s ./target/debug/spiced --query-set tpch
```
