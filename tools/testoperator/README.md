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
- `http-consistency`: Runs a test to compare the latency performance of a HTTP enabled component as the component is persistently queried.
- `http-overhead`: Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the duration of the test when N clients are sending queries concurrently.
- `evals`: Run model evaluations (evals) test.

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

#### Run a HTTP consistency test against an embedding model
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
Note: The `.model` field in the payload will be overriden.

#### Run a HTTP consistency test against an LLM model
```sh
testoperator run http-consistency \
    --duration 300 \
    --buckets 5 \
    --model openai-gpt5 \
    --payload-file payloads.txt  #Use JSONL-like format for JSON payloads
```


#### Run a HTTP overhead test against an embedding model
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

#### Run a HTTP overhead test against an LLM model with incompatible API (e.g. Anthropic)
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
