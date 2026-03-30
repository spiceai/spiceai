# Spiced Metrics Scraping in TestOperator

TestOperator now supports scraping Prometheus metrics from the spiced runtime during test execution. This allows you to capture and analyze runtime performance metrics alongside your test results.

## Overview

When enabled, TestOperator will:

1. Start spiced with the `--metrics` flag (exposing Prometheus metrics on port 9090)
2. Periodically scrape metrics from `http://localhost:9090/metrics` during test execution
3. Process and aggregate the metrics
4. Display key metrics after test completion
5. Emit metrics via OpenTelemetry alongside test metrics (when `--metrics` is also enabled)

## Usage

Add the `--scrape-spiced-metrics` flag to any testoperator command:

```bash
# Benchmark test with spiced metrics scraping
cargo run -p testoperator -- run bench \
  -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml \
  -s spiced \
  --query-set tpch \
  --scrape-spiced-metrics

# Load test with spiced metrics scraping
cargo run -p testoperator -- run load \
  -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml \
  -s spiced \
  --query-set tpch \
  --concurrency 10 \
  --duration 60 \
  --scrape-spiced-metrics

# With both test metrics and spiced metrics
cargo run -p testoperator -- run bench \
  -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml \
  -s spiced \
  --query-set tpch \
  --metrics \
  --scrape-spiced-metrics
```

## Metrics Captured

The scraper captures all Prometheus metrics exposed by spiced, including:

- **Query Metrics**: Total query count, query durations, query errors
- **Cache Metrics**: Cache hits, cache misses, cache hit rate
- **Connection Metrics**: Active connections, connection pool stats
- **Memory Metrics**: Memory usage, allocations
- **Custom Metrics**: Any custom metrics exposed by spiced extensions

### Key Metrics Displayed

After test completion, TestOperator displays a summary of key metrics:

```
==============================
Spiced Runtime Metrics:
==============================
Total Queries Executed: 250
Cache Hit Rate: 85.20%
Peak Active Connections: 12
==============================
```

### Metrics Emitted to OpenTelemetry

When both `--metrics` and `--scrape-spiced-metrics` are enabled, the following metrics are emitted:

- `spiced_query_count`: Total number of queries executed by spiced (from `query_executions_total`)
- `spiced_cache_hit_rate`: Cache hit rate from spiced metrics (calculated from `results_cache_hits_total` / `results_cache_requests_total`)
- `spiced_active_connections`: Peak active query count during test (from `query_active_count`)

These metrics are tagged with appropriate dimensions (e.g., test name, query set) for filtering in your observability platform.

## Architecture

### Scraping Process

1. **Initialization**: MetricsScraper spawns a background task when spiced starts
2. **Periodic Collection**: Every 1 second, the scraper fetches `/metrics` endpoint
3. **Parsing**: Prometheus text format is parsed into structured samples
4. **Aggregation**: Samples are collected and aggregated during test execution
5. **Reporting**: At test completion, metrics are processed and displayed

### Prometheus Format Support

The scraper supports the Prometheus text exposition format:

- Counter metrics
- Gauge metrics
- Histogram metrics (basic support)
- Summary metrics (basic support)
- Labels and dimensions
- Metric types (via `# TYPE` comments)

## Example Output

```bash
$ cargo run -p testoperator -- run bench \
    -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml \
    -s spiced --query-set tpch --scrape-spiced-metrics

Running benchmark test
[========================================] 22/22 queries completed

┌─────────────┬──────────────┬─────────────┬─────────────┬──────────────┐
│ query_name  │ median_ms    │ min_ms      │ max_ms      │ p99_ms       │
├─────────────┼──────────────┼─────────────┼─────────────┼──────────────┤
│ tpch_q1     │ 125.4        │ 118.2       │ 142.8       │ 141.2        │
│ tpch_q2     │ 89.3         │ 82.1        │ 98.7        │ 97.4         │
│ ...         │ ...          │ ...         │ ...         │ ...          │
└─────────────┴──────────────┴─────────────┴─────────────┴──────────────┘

==============================
Spiced Runtime Metrics:
==============================
Total Queries Executed: 110
Cache Hit Rate: 0.00%
Peak Active Connections: 1
==============================

Benchmark test completed
```

## Implementation Details

### MetricsScraper

The `MetricsScraper` struct manages the background scraping task:

```rust
pub struct MetricsScraper {
    cancel_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<SpicedMetrics>>,
}
```

Key methods:

- `spawn()`: Start the background scraper
- `stop()`: Stop scraping and return collected metrics

### SpicedMetrics

The `SpicedMetrics` struct holds aggregated metrics:

```rust
pub struct SpicedMetrics {
    pub samples: HashMap<String, Vec<MetricSample>>,
}
```

Helper methods:

- `get_counter_value(name)`: Get final value of a counter
- `get_gauge_max(name)`: Get maximum value of a gauge
- `get_gauge_avg(name)`: Get average value of a gauge

## Configuration

### Metrics Endpoint

By default, spiced is started with `--metrics 0.0.0.0:9090`. This endpoint is scraped at:

```
http://localhost:9090/metrics
```

### Scrape Interval

Metrics are scraped every 1 second. This is configured in:

```rust
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
```

## Troubleshooting

### Metrics Endpoint Unavailable

If you see warnings about failed scrapes:

```
Warning: Failed to collect spiced metrics: Metrics endpoint returned status: 404
```

This usually means:

- Spiced wasn't started with the `--metrics` flag (check that `--scrape-spiced-metrics` is enabled)
- The metrics port is already in use
- Spiced crashed or stopped prematurely

### No Metrics Displayed

If metrics are scraped but not displayed:

- Check that spiced is exposing metrics (visit `http://localhost:9090/metrics` manually)
- Verify the metric names match what spiced exports
- Check for parsing errors in the logs

### Metric Names

Spiced metric names as exposed via Prometheus:

- **Counters** (have `_total` suffix in Prometheus format):
  - `query_executions_total` - Total queries executed
  - `results_cache_hits_total` - Cache hits
  - `results_cache_requests_total` - Cache requests
  - `query_processed_bytes_total` - Bytes processed
  - `query_returned_bytes_total` - Bytes returned

- **Gauges**:
  - `query_active_count` - Active query count (up-down counter)
  - `dataset_active_count` - Active datasets
  - `model_active_count` - Active models

- **Histograms**:
  - `query_duration_ms` - Query duration in milliseconds
  - `query_execution_duration_ms` - Query execution duration
  - `query_returned_rows` - Rows returned per query
