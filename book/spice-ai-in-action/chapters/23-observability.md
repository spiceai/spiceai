# 23. Observability and performance engineering

The purpose of observability is to explain a user's experience in terms of the system that produced it. For Northstar, “the dashboard is slow” can mean a source query, a refresh backlog, a result-cache miss, a local sort, a shared resource limit, or a browser rendering problem. A useful investigation connects the request to a plan and measured work.

## 23.1 Start with service objectives

Define objectives for specific request classes. A sales card, an order lookup, a policy search, and a report export need different latency, availability, and freshness targets. Record success as an answer that is authorized, semantically correct, and within its freshness contract—not merely an HTTP response without an error.

Separate latency from freshness. A cached stale answer can be fast. A current query can be slow. Plot or record both so a tuning change cannot quietly exchange one for the other.

## 23.2 Use all three observation surfaces

Runtime logs describe lifecycle and failures. Metrics describe distributions and evolving state. Query plans and task artifacts explain the shape and work of an individual execution. Use them together.

The local runtime was started with `--metrics 127.0.0.1:19090`. Its scrape was captured with:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:19090/metrics > metrics.prom
```

The artifact contains, among other fields, `dataset_acceleration_last_refresh_unix_time_ms` and `dataset_acceleration_refresh_duration_ms` for the runtime's own datasets. Metric names and labels are versioned; inspect the current scrape and its `HELP` text before writing dashboards.

Do not infer a configured listener from a conventional port number. The metrics endpoint was explicitly enabled in the lab. A production service should expose it only to the intended monitoring network.

## 23.3 Discover system tables before querying them

The local run used:

```sql
SELECT table_catalog, table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'runtime'
ORDER BY table_name;
```

Observed tables were `spice.runtime.metrics` and `spice.runtime.task_history`. The installed stable binary did not list a separate `runtime.query_history` table in that result. Newer versions can provide additional query-history or task facilities. Discover the schema rather than copying an assumed column list from another release.

Next, query `information_schema.columns` for the relevant table. Select only fields needed for the investigation and apply time or row bounds. Task history can contain SQL, prompts, or other sensitive content, so treat it as controlled operational data.

## 23.4 Preserve the plan and returned rows

For a slow query, capture `EXPLAIN` and, when appropriate for its cost and side effects, `EXPLAIN ANALYZE`. Record source rows, filtered rows, aggregate output, join shape, repartitioning, and spills. Compare the observed row counts with your expected data distribution.

Chapter 5's fixture plan shows eight source rows, six paid rows, and two groups. That small example is the pattern to scale: connect each important operator to a reason it has that amount of work. A final ten-row result can conceal millions of intermediate rows.

When comparing versions, validate returned rows before interpreting a faster plan. Preserve the full plan, not only the operator you expected to change. A secondary difference can explain the measurement.

## 23.5 Design a benchmark that answers a question

A benchmark specification includes the data snapshot, schema, source version, Spice binary, configuration, hardware or resource entitlement, storage, network, query set, concurrency, warmup, cache policy, run duration, and result validation. Without these, a number is difficult to reproduce or interpret.

Use the repository's `testoperator` or another appropriate workload harness. A representative pattern is:

```bash
cargo run -p testoperator -- run bench \
  -p test/spicepods/tpch/sf1/federated/duckdb.yaml \
  -s spiced -d ./.data --query-set tpch --validate
```

This is a repository harness example, not a command executed for the book's performance claims. It requires the referenced test data and environment. Follow the repository's scoped build instructions and run one heavy workload at a time.

Save per-query timings, validation results, plans where collected, and the run directory. Report the distribution and failures, not only an average across successful queries. Excluding timed-out queries can make a failing system appear faster.

## 23.6 Measure memory as a process behavior

Collect RSS or a heap profile while running the production-shaped workload under the intended query-memory settings. Include ingestion and maintenance. Record peak and sustained usage, not just an idle value after load.

A query memory limit usually governs a particular accounting domain. Native libraries, model memory, caches, file mappings, and other runtime state may have separate lifecycles. Compare accounting metrics with the process and container observations rather than assuming they are identical.

For a suspected leak, show growth across repeated comparable workload cycles and retain the profile or allocation evidence. A one-time high-water mark after a cache warms is not, by itself, evidence of a leak.

## 23.7 Investigate tail latency and fairness

An isolated query can be fast while concurrent queries suffer. Measure request classes under a realistic mixture, including refresh and model activity. Track queueing, execution, and transfer where the system exposes them.

A small number of long-running scans can consume capacity needed by point lookups. Use admission limits, request deadlines, workload separation, or a different deployment boundary based on observed interference. Increasing all concurrency settings at once can make contention harder to locate.

For a distributed workload, retain per-task measurements. For a source-backed workload, retain source load. The bottleneck may be outside the Spice process.

## 23.8 Turn an incident into an experiment

State the hypothesis narrowly: “This query transfers the unfiltered relation,” or “Refresh work overlaps with the latency spike.” Identify the observation that could disprove it. Capture the baseline, change one factor, and rerun the same workload.

If the run contradicts the hypothesis, withdraw it. A convincing source-code story does not outweigh observed behavior. Conversely, a symptom without a reading of the relevant path can lead to a superficial fix. Use both.

**Exercise.** Create a one-page performance report template with fields for rig, data, SQL, plan, validation, cache state, concurrency, latency distribution, memory, source load, and artifacts. Fill only the fields you actually measured; mark the rest unmeasured.

**Further reading.** See the [monitoring documentation](https://spiceai.org/docs/monitoring), `docs/dev/metrics.md`, `crates/runtime-metrics`, and the repository's benchmark harness. The book's small runs demonstrate observation methods and do not establish production performance.
