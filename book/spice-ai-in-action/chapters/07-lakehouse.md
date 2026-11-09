# 7. Working with a lakehouse

Northstar's operational database is the authority for current orders. Its historical exports support seasonality, audits, and longer analytical windows. Object storage is a natural home for that history, but “files in a bucket” and “a table with snapshots” are different abstractions. This chapter explains the distinction and shows how it changes a Spice integration.

## 7.1 Parquet is a file format, not a table transaction

Parquet organizes columnar data with metadata that readers can use for projection and pruning. It is useful for analytical scans because a query can often avoid reading unneeded columns and some irrelevant row groups. The actual skipped work depends on the predicate, file statistics, and execution path; inspect the plan and read metrics.

A collection of Parquet files does not by itself define which files constitute a committed table version. If a producer replaces ten files one at a time, a directory scan can encounter a mixture unless publication is coordinated. A table format such as Iceberg or Delta Lake adds metadata and commit semantics around a set of data files.

Do not treat direct file discovery as a substitute for a table-format connector when the writer relies on transaction metadata. Deleted or superseded data files can remain in storage for snapshots or garbage collection. Reading every file under the prefix can assign a meaning different from the committed table.

## 7.2 Partitioning should follow useful predicates

A path such as `orders/year=2026/month=08/` expresses a partition layout. A query filtering the corresponding partition values can potentially skip unrelated files when the connector is configured to interpret the layout. A query that wraps a field in an unsupported expression may not expose the same pruning opportunity.

Start from the workload: does the application usually ask for one tenant, a date interval, or a category? Estimate cardinality before partitioning. A partition for every order creates a management problem; a single partition for years of data may provide little selectivity. The right granularity depends on data volume and access patterns.

File size and partition size are separate choices. One partition can contain many files. A few very large files can limit parallel work for some scans, while many tiny files add discovery and scheduling overhead. These are workload hypotheses until measured with the actual reader.

## 7.3 Connect through the right catalog

Iceberg integrates table metadata with a catalog that resolves table names and commits. Different catalogs expose different authentication and endpoint requirements. Spice's connector configuration must match the catalog implementation and the selected release. The cookbook's Iceberg and Glue examples are starting points, not interchangeable connection strings.

An integration rehearsal should establish four facts: the intended namespace resolves, the intended table snapshot is readable, row values and types match the source, and the runtime uses credentials that remain valid during long queries. Where the source exposes snapshot identifiers, retain the identifier with the query evidence.

For Delta Lake, verify that the configured path and connector interpret the transaction log rather than merely discovering Parquet files. Check timestamp and decimal mappings and the table features supported by the exact reader version. A lakehouse table may enable writer features newer than a given client supports.

## 7.4 Historical and operational windows can overlap

Suppose Northstar exports orders nightly and also replicates recent orders into a local accelerator. Joining these windows with `UNION ALL` can duplicate orders during the overlap. Avoid resolving the problem with `UNION` over all columns: an updated order can have different values and survive de-duplication as two rows.

Define an explicit boundary. One approach is to query immutable historical data before a published cutoff and recent data at or after it. Another is to merge by a stable key and a version or commit ordering. The latter requires a trustworthy ordering; a wall-clock modification timestamp is not automatically unique or monotonic.

A boundary table can hold the published historical cutoff and export version. Update it as part of the export publication protocol. The application should not independently guess the cutoff from the current date. Late-arriving corrections require a policy for revising old partitions or representing adjustment events.

## 7.5 Writes need a precise contract

Spice can expose supported write paths for selected sources and table formats. Do not generalize an `INSERT INTO` example into support for arbitrary updates, multi-table transactions, or cross-source atomicity. The available operation, catalog permissions, and commit semantics belong in the integration contract.

For a write rehearsal, use a disposable namespace. Insert a uniquely identifiable batch, query it through the table-format path, and inspect the source's committed state. Retry the client request only according to a documented idempotency strategy. A network timeout after submission does not prove that a commit failed.

Keep ingestion identity separate from row identity. If the batch carries a unique load ID, you can investigate whether it was committed without blindly repeating the insertion. Test concurrent writers where the deployment will have them, and preserve conflicts rather than silently converting them to successes.

## 7.6 Accelerating a lakehouse working set

Northstar need not accelerate all history. It can materialize a recent interval or a derived summary while leaving the long tail federated. The selected representation should match the application's allowed questions. A daily summary cannot answer arbitrary order-level drilldowns, and a recent-only table must not be labeled as all-time history.

A refresh query that narrows data changes the accelerator's completeness. Pair it with a documented coverage interval. Fallback behavior, covered in Chapter 9, must not be mistaken for a universal way to reconstruct missing historical rows for any aggregate.

![Figure 7.1. Historical snapshots and the operational working set meet at an explicit coverage boundary.](figures/lakehouse.png)

**Exercise.** Design a seven-day accelerated window backed by a nightly historical export. State how an order corrected after ten days appears in a monthly report. Identify the publication artifact that lets a query distinguish a complete export from an in-progress one.

**Further reading.** See the [Iceberg connector](https://spiceai.org/docs/components/data-connectors/iceberg), [Delta Lake connector](https://spiceai.org/docs/components/data-connectors/delta-lake), and cookbook `glue/` and `delta-lake/`. The Apache Iceberg and Delta Lake project specifications define their table semantics; use the versions supported by your runtime.
