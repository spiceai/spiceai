# 5. Federation and the query plan

Northstar eventually replaces its order CSV with a database table while keeping merchandising data in files. A common SQL interface makes the join possible. It does not make every placement of work equally sensible. This chapter teaches you to inspect the boundary between source execution and local execution before attempting to tune it.

## 5.1 Pushdown is a negotiated capability

A connector may push a projection, predicate, aggregation, join, or limit into its source. Each operation depends on what the connector and remote system can express while preserving semantics. An expression that works in Spice's SQL dialect may not translate to the source dialect. In that case, the runtime may perform additional work locally.

Projection pushdown asks the source for fewer columns. Predicate pushdown asks it for fewer rows. Aggregation pushdown can reduce a large relation to a small summary before transfer. A pushed limit can reduce transfer only when it preserves the query's ordering and other semantics. These mechanisms should be inspected separately.

An exact pushed predicate means the local layer can trust the source to enforce the condition as required. An inexact or partial predicate can require a residual filter. Seeing the predicate mentioned near a source scan is not enough to conclude that all filtering happened remotely.

## 5.2 Read a small plan first

**Local lab.**

```sql
EXPLAIN
SELECT order_id
FROM orders
WHERE status = 'paid' AND total_cents > 5000;
```

The captured logical plan includes:

```text
Projection: orders.order_id
  Filter: orders.status = Utf8("paid")
          AND orders.total_cents > Int64(5000)
    TableScan: orders
      projection=[order_id, status, total_cents]
      partial_filters=[...]
```

The excerpt preserves the relevant operators; the full text is in the evidence file. The scan needs the projected output key plus the two columns used by the predicate. Its physical plan contains a `FilterExec` above a CSV `DataSourceExec`. That is evidence of a remaining filter in the local execution plan. It is not evidence that a PostgreSQL connector would behave identically.

The physical plan also shows repartitioning on the author's machine. Partition counts depend on the runtime's configuration and available CPU entitlement. Do not copy the count into production merely because it appears in a printed plan.

## 5.3 Add actual operator observations

`EXPLAIN ANALYZE` executes the query and adds metrics. On the fixture's grouped paid-order query, the captured plan reports eight rows at the source, six after the paid-status filter, and two output groups. These counts connect the plan to the business arithmetic.

```sql
EXPLAIN ANALYZE
SELECT tenant_id, SUM(total_cents)
FROM paid_orders
GROUP BY tenant_id;
```

The full artifact contains per-operator timings, output rows, and memory or spill fields where provided. Those timings describe one tiny run. They are useful for understanding the plan, not for asserting production latency or an engine ranking.

Read plans from the leaves upward to understand where data originates and how its shape changes. Then read top-down to ask what each parent requires. A sort may require all candidate rows. An aggregate may shrink the relation. A repartition redistributes it. A join may introduce a large intermediate result even when the final result has ten rows.

![Figure 5.1. Query evidence connects source rows, filtering, and final groups.](figures/plan.png)

## 5.4 Cross-source joins have a transfer budget

Suppose the source has 100 million orders, but the page asks for one tenant's last seven days. The central question is whether the source scans and returns a narrow interval or whether Spice receives a much larger relation before applying the restriction. The answer affects source load, network traffic, and local memory.

Estimate the candidate transfer volume as rows transferred times average transferred row width. This is a planning estimate, not a measured network counter. Compare it with actual bytes and rows from the runtime and source. A wide description column can dominate transfer even when row counts look reasonable.

When two tables are in the same remote system, a connector may be able to delegate more of the query together. When they are in unrelated sources, some combination usually must happen at the coordinating engine. Verify the actual behavior with the configured connector and query shape. Moving the smaller relation locally or accelerating a repeatedly scanned working set can change the economics.

## 5.5 Statistics are promises

Optimizers use row counts, distinct-value estimates, and column statistics to choose plans. In a federated system, these facts may be unavailable, approximate, or stale. A table with a known exact row count is different from one with an estimate collected before a large ingest.

For users, the practical response is to compare estimated shape with observed operator rows and investigate a large mismatch. For connector authors, exactness is a correctness obligation: statistics marked exact can enable result substitutions, so “approximately right” is not sufficient. Chapter 24 returns to the extension boundary.

A plan with an unexpected join order is a hypothesis about performance until you run it with relevant data. Preserve its actual operator metrics before changing settings. Otherwise, it is easy to optimize a diagram rather than a workload.

## 5.6 A disciplined tuning experiment

Choose a representative query and fixed data snapshot. Capture the original SQL, Spicepod, binary version, source configuration, plan, result rows or digest, and operator metrics. Change one causal factor: a projection, a filter expression, acceleration, or a source index. Run the same workload and compare results before comparing speed.

Measure source impact as well as application latency. A rewrite that reduces local CPU by making the operational database work harder may violate the reason you introduced Spice. Likewise, a smaller local memory footprint may have shifted bytes into remote transfer.

**Exercise.** Compare `SELECT *` with a narrow projection over the same filtered orders. Capture both plans and identify the scan columns. Next, place a disposable copy of the fixture in PostgreSQL and repeat. Which observation is a property of SQL, and which is a property of the connector?

**Further reading.** See the [federated query documentation](https://spiceai.org/docs/features/query-federation), cookbook `postgres/connector/`, and `crates/runtime/src/datafusion`. For this chapter's observed plans, use the `plan` and `analyze` entries in the local evidence.
