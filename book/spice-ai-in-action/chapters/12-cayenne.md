# 12. Cayenne: storage, visibility, and maintenance

Cayenne is Spice's native accelerator. Its design brings together a columnar representation, managed table metadata, mutable ingestion, and maintenance. Understanding those responsibilities helps you evaluate it without treating an engine name as a performance guarantee.

This chapter explains the inspected source architecture and a small local configuration. It does not present an unrun throughput comparison. The local acceptance record includes a NULL-sensitive query discrepancy described in Appendix A; the chapter does not claim that every query in the suite passed on Cayenne.

## 12.1 Separate data from metadata

Columnar data files hold encoded values. Metadata describes which files and versions belong to a table, its schema, and other state needed to interpret mutations and snapshots. A table is therefore more than a directory of Vortex files.

Cayenne's source reference documents a metadata layer alongside Vortex-backed storage. When planning persistence or backup, account for both. Copying files while ignoring the metadata that selects their visibility is not a demonstrated restore procedure. Copying metadata while omitting referenced data is equally incomplete.

```yaml
# Acceleration fragment for the local fixture
acceleration:
  enabled: true
  engine: cayenne
  mode: file
  refresh_mode: full
  params:
    cayenne_file_path: .spice/cayenne/data/orders
    cayenne_metadata_dir: .spice/cayenne/metadata
```

The companion `spicepod.cayenne.yaml` declares a complete configuration. Use its isolated paths and retain the verification output. Select an engine based on a passed application contract before evaluating performance.

## 12.2 Why columnar representation matters

Analytical queries often read a subset of columns across many rows. A columnar format can encode similar values together and expose metadata useful for avoiding irrelevant work. Encodings and compression also affect random access, decoding cost, and memory movement.

Arrow is a common execution interchange representation; Vortex is a storage and array-format foundation used by Cayenne. “Columnar” does not mean every operation is zero-copy. Decoding, casting, filtering, joining, and serialization can allocate or transform data. The useful question is where those operations occur in the observed query path.

Inspect plans and operator metrics for the configured storage mode. A point lookup, a wide scan, a selective aggregate, and a join can stress different parts of the system. One benchmark cannot stand in for all four.

## 12.3 Updates complicate immutable files

An insert can append new values. An update must make an old logical version stop contributing while making a new version visible. A delete must remove a logical row from query results even if its encoded bytes remain in an older file until maintenance reclaims them.

Cayenne's source describes sequence-based visibility and deletion state used to reconcile mutable data with stored files. For an operator, the consequence is that raw file counts and raw file contents do not alone define the current SQL table. The query path must apply the table's visibility rules.

For a connector author, the implication is stronger: bypassing a wrapper or using an exact statistic without considering mutable state can invalidate results. Do not infer query semantics by reading only the file format layer. The accelerated table, storage provider, and overlay behavior form one path.

## 12.4 Query visibility is a snapshot decision

A scan needs a coherent view of the files and mutable state it reads. Concurrent ingestion and compaction cannot be allowed to make a query arbitrarily lose or double-count a row. The inspected Cayenne reference explains snapshot and sequence coordination, with behavior that evolves across releases and access modes.

Keep application expectations precise. Read-after-write behavior through an explicitly supported write path, eventual visibility through CDC, and consistency across independently replicated tables are different guarantees. A persistent file mode alone establishes none of them.

Use a visibility experiment with identifiable rows: repeatedly update one key while querying a sum and key count, then retain the returned states and the source event sequence. To make a correctness claim, the artifact must show rows that violate a stated guarantee, not merely a suspicious internal timing argument.

![Figure 12.1. Cayenne queries interpret stored data through metadata and mutable visibility state.](figures/cayenne.png)

## 12.5 Compaction is background production work

Compaction reorganizes stored data and mutation state into a representation that is more efficient to read or manage. It consumes CPU, I/O, and temporary space while queries and ingestion may be active. A deployment must budget for maintenance rather than assuming all machine resources are available to foreground queries.

Small-file counts, delete density, sort order, and protected snapshots can influence the work required. Tuning a trigger without observing its effect can move cost from one moment to another. Capture maintenance activity, file counts, query metrics, and ingest lag together.

Do not manually remove files because they appear old or unreferenced to a directory listing. Snapshot retention and garbage collection need to follow the storage engine's documented lifecycle. A file may still be needed by an active query or recovery state.

## 12.6 Memory has several owners

An accelerated dataset can consume memory for mutable batches, decoded data, caches, metadata, query operators, and background maintenance. Some memory is shared or represented through views; some is separately reserved. A single configuration limit is not necessarily a cap on total process RSS.

When evaluating memory, record process RSS alongside the runtime's query-memory settings and available engine metrics. Test concurrent ingestion and queries, not only an idle loaded table. A working set that fits when no query runs may fail under a realistic join or compaction burst.

The current source has CPU and memory budgeting mechanisms that coordinate work across components. Treat their defaults as versioned implementation choices. Start with documented defaults and alter a setting only when a measured bottleneck and an acceptance workload justify it.

## 12.7 Local and remote storage

Local persistent storage and object-backed data storage have different latency, durability, availability, and credential models. A configuration that places data in an object store may still depend on local metadata. Understand the whole state placement before calling a node disposable.

For remote storage, verify which object-storage service and URL forms the chosen release supports. Test permission expiry, restart, and interrupted writes in a disposable environment. A successful initial load does not exercise the recovery path of a distributed storage configuration.

## 12.8 Evaluate Cayenne fairly

Use a representative schema, realistic data distribution, and the production refresh path. Preserve before-and-after plans, result validation, per-query timings, memory traces, and maintenance observations on the same rig. Run one heavy benchmark at a time. Report the workload mix and whether caches and storage were warm.

If an acceptance query differs from its reference result, stop treating that variant as validated. Preserve the discrepancy and investigate it separately from performance tuning. Correctness precedes an attractive throughput number.

**Exercise.** Design a four-part evaluation: initial load, steady CDC, concurrent analytics, and restart recovery. Name the artifacts for each. Explain which state must be restored together and which can be regenerated from the source.

**Further reading.** The detailed pinned reference is `docs/cayenne/cayenne.md`; implementation is under `crates/cayenne`. Consult the [Cayenne configuration page](https://spiceai.org/docs/components/data-accelerators/cayenne) for release-specific parameters and cookbook `cayenne/` for a runnable starting pattern.
